import sys
import requests
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from kiteconnect import KiteConnect
from scipy.optimize import newton
import sqlalchemy
import time
from tqdm import tqdm
import random,os,re
from pprint import pprint
from openai import OpenAI
from dotenv import load_dotenv
from globals import common_variables
from common_util import get_cashed_isin_meta_data, get_isin_meta_data,get_record_due_dates,get_zero_coupon_cashflows,type_of_interest_rate,get_missing_partial_redemption
load_dotenv()

key = os.getenv("OPENAI_API_KEY")
if not key:
    print(key)
    raise ValueError("❌ OPENAI_API_KEY is not set. Please export it or load it explicitly.")
client = OpenAI(api_key=key)

# === Headers to mimic real browser ===
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.indiabondinfo.nsdl.com/",
    "Origin": "https://www.indiabondinfo.nsdl.com",
    "Connection": "keep-alive"
}

today = datetime.today()
common_variables.engine = sqlalchemy.create_engine(
    "postgresql+psycopg2://localhost/production"
)

def fetch_raw_redemption(isin):
    url = f"https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/redemptions?isin={isin}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        return resp.json()
    except Exception as e:
        pass

def generate_cashflows_from_schedule(
    sec_id,
    issue_price,
    coupon_rate,
    interest_payment_frequency,
    redemption_schedule,
    maturity_date,
    issue_date,
    today_str
):
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    # Convert inputs
    coupon_rate_decimal = float(coupon_rate) / 100
    payments_per_year = int(interest_payment_frequency)
    interval_months = int(12 / payments_per_year)
    # Assume redemption_schedule is pre-sorted and contains date objects

    # Initialize
    cashflows = []
    current_face_value = float(issue_price)
    start_date = issue_date
    end_date = maturity_date
    payment_date = start_date
    accrued_interest = 0.0
    redemption_index = 0

    while payment_date < end_date:
        next_payment_date = payment_date + relativedelta(months=interval_months)
        if next_payment_date > end_date:
            next_payment_date = end_date

        current_date = payment_date
        while current_date < next_payment_date:
            # Check for redemption on this day (before interest accrual)
            while redemption_index < len(redemption_schedule) and current_date == redemption_schedule[redemption_index]["date"]:
                redemption_amount = redemption_schedule[redemption_index]["amount"]
                cashflows.append({
                    "sec_id": sec_id,
                    "date": current_date.strftime("%Y-%m-%d"),
                    "amount": redemption_amount,
                    "type": "redemption",
                    "created_at": today_str
                })
                current_face_value -= redemption_amount
                redemption_index += 1
                continue

            # Accrue interest
            year_days = 366 if current_date.year % 4 == 0 else 365
            daily_interest = current_face_value * coupon_rate_decimal / year_days
            accrued_interest += daily_interest

            current_date = (datetime.combine(current_date, datetime.min.time()) + timedelta(days=1)).date()

        # Record interest payment
        cashflows.append({
            "sec_id": sec_id,
            "date": next_payment_date,
            "amount": round(accrued_interest, 2),
            "type": "interest",
            "created_at": today_str
        })
        accrued_interest = 0.0
        payment_date = next_payment_date

    # Handle any remaining redemptions after final interest
    for i in range(redemption_index, len(redemption_schedule)):
        cashflows.append({
            "sec_id": sec_id,
            "date": redemption_schedule[i]["date"],
            "amount": redemption_schedule[i]["amount"],
            "type": "redemption",
            "created_at": today_str
        })

    return cashflows

def calculate_interest_amounts(df,issue_date):
    print("Calculating interest amounts for each period...")

    # Ensure types
    df['due_date'] = pd.to_datetime(df['due_date'])
    df['record_date'] = pd.to_datetime(df['record_date'])
    df['coupon'] = pd.to_numeric(df['coupon'], errors='coerce')
    df['face_value'] = pd.to_numeric(df['face_value'], errors='coerce')

    # Sort only by due_date, not record_date
    df = df.sort_values('due_date').reset_index(drop=True)
    print(df)
    interest_idx = df[df['event_type'] == 'interest_payment'].index

    prev_due_date = None
    for i, idx in enumerate(interest_idx):
        row = df.loc[idx]

        # First payment: if no previous due_date, skip or set days=0
        if prev_due_date is None:
            days = (row['due_date'] - issue_date).days
        else:
            days = (row['due_date'] - prev_due_date).days

        days = max(days, 0)

        coupon_rate = row['coupon'] / 100 if pd.notnull(row['coupon']) else 0
        face_value = row['face_value'] if pd.notnull(row['face_value']) else 0
        interest = face_value * coupon_rate * days / 365

        df.at[idx, 'days'] = days
        df.at[idx, 'amount'] = round(interest, 2)

        # update prev_due_date
        prev_due_date = row['due_date']

    return df
# Usage:
# schedule = calculate_interest_amounts(schedule)

def parse_redemption_data_with_local_llm(id, interest_payment_frequency,coupon_rate):
    import json
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    today_str = datetime.today().strftime("%Y-%m-%d")
    # Analyze interest_payment_frequency string and return integer
    freq_str = (interest_payment_frequency or "").lower()
    if any(x in freq_str for x in ["monthly", "every month","month","twelve"]):
        return 12
    if any(x in freq_str for x in ["quarterly", "every 3 months",'four']):
        return 4
    if any(x in freq_str for x in ["semi", "half-yearly", "every 6 months", "twice a"]):
        return 2
    if ("annual" in freq_str or "yearly" in freq_str) and "semi" in freq_str:
        return 2
    if any(x in freq_str for x in ["annual", "yearly", 'once a year']) and 'semi' not in freq_str:
        return 1
    if coupon_rate == 0 or coupon_rate is None or coupon_rate == '0' or coupon_rate == 'NA' or 'on maturity' in freq_str:
        return 0
    
    prompt = f"""
You are given information about a bond:

sec_id = {id}
Raw frequency description = "{interest_payment_frequency}"

Your task is to **extract how many times per year interest is paid**. Return an integer only. 

Guidelines:
- Keywords:
    - "annually", "yearly", "annual" → return 1
    - "semi-annually", "half-yearly", "every 6 months" → return 2
    - "quarterly", "every 3 months" → return 4
    - "monthly", "every month" → return 12
- Also check for clues like “starting XX-XXX-YYYY till XX-XXX-YYYY”, especially if the word “annual” appears — assume yearly unless other keywords override.
- If no clear frequency is present but a fixed future date appears multiple times (e.g., every Feb 23), infer annual and return 1.
- If the input is empty, NULL, "NA", "0", or similar, return 0 (indicating a zero coupon bond).
- Do not return text or comments — return only the integer.

This prompt is inside Python code that expects a plain integer. So output must be:
→ Only an integer like 0, 1, 2, 4, or 12 (and nothing else).
"""
    print(prompt)
    response = requests.post(
        "http://192.168.0.163:11434/api/generate",
        json={"model": "llama3.1:8b", "prompt": prompt, "stream": False},
        timeout=120
    )

    output = response.json().get("response", "").strip()
    # Extract only integer from output string
    match = re.search(r"\d+", output)
    if match:
        payments_per_year = int(match.group())
        print(payments_per_year)
        return payments_per_year
    else:
        print(f"❌ Could not extract integer from output: {output}")
        return 0

def parse_redemption_data_with_llm(id, interest_payment_frequency, coupon_rate):
    import re
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    today_str = datetime.today().strftime("%Y-%m-%d")
    # Analyze interest_payment_frequency string and return integer
    freq_str = (interest_payment_frequency or "").lower()
    if any(x in freq_str for x in ["monthly", "every month","month","twelve"]):
        return 12
    if any(x in freq_str for x in ["quarterly", "every 3 months",'four']):
        return 4
    if any(x in freq_str for x in ["semi", "half-yearly", "every 6 months", "twice a"]):
        return 2
    if ("annual" in freq_str or "yearly" in freq_str) and "semi" in freq_str:
        return 2
    if any(x in freq_str for x in ["annual", "yearly", 'once a year']) and 'semi' not in freq_str:
        return 1
    if any(x in freq_str for x in ["thrice"]):
        return 3
    if coupon_rate == 0 or coupon_rate is None or coupon_rate == '0' or coupon_rate == 'NA' or 'on maturity' in freq_str:
        return 0
    
    prompt = f"""
You are given information about a bond:

sec_id = {id}
Raw frequency description = "{interest_payment_frequency}"

Your task is to **extract how many times per year interest is paid**. Return an integer only. 

Guidelines:
- Keywords:
    - "annually", "yearly", "annual" → return 1
    - "semi-annually", "half-yearly", "every 6 months" → return 2
    - "quarterly", "every 3 months" → return 4
    - "monthly", "every month" → return 12
- Also check for clues like “starting XX-XXX-YYYY till XX-XXX-YYYY”, especially if the word “annual” appears — assume yearly unless other keywords override.
- If no clear frequency is present but a fixed future date appears multiple times (e.g., every Feb 23), infer annual and return 1.
- If the input is empty, NULL, "NA", "0", or similar, return 0 (indicating a zero coupon bond).
- Do not return text or comments — return only the integer.

This prompt is inside Python code that expects a plain integer. So output must be:
→ Only an integer like 0, 1, 2, 4, or 12 (and nothing else).
"""
    print(prompt)
    # Log the prompt to a log file before calling the LLM
    try:
        log_entry = (
            "\n=== New Prompt ===\n"
            f"id: {id}\n"
            f"Datetime: {datetime.now().isoformat()}\n"
            f"Prompt:\n{prompt}\n"
        )
        with open("llm_prompts.log", "a", encoding="utf-8") as log_f:
            log_f.write(log_entry)
    except Exception as log_exc:
        print(f"⚠️ Failed to log prompt for id {id}: {log_exc}")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
    )
    output = response.choices[0].message.content.strip()
    # Extract only integer from output string
    match = re.search(r"\d+", output)
    if match:
        payments_per_year = int(match.group())
        print(payments_per_year)
        return payments_per_year
    else:
        print(f"❌ Could not extract integer from output: {output}")
        return 0

isins = pd.read_sql_query("""SELECT distinct sec_id ,isin
FROM security_ids
WHERE captured_at::date = (SELECT MAX(captured_at::date) from security_ids)
""", common_variables.engine)
# isins = pd.read_sql_query("""SELECT sec_id,isin from security_ids where sec_id = 5978
# --                          (select distinct sec_id from bond_cashflows)""", common_variables.engine)
isin_dict = dict(zip(isins['sec_id'], isins['isin']))

interest_payment_frequency_dict = pd.read_sql_query("""
SELECT sec_id, frequency FROM bond_metadata_prod
""", common_variables.engine).set_index('sec_id')['frequency'].to_dict()

# pprint(interest_payment_frequency_dict)
# print(isin_dict)
# isin_dict = {
#     '6744': "INE01CY079E7"
#     # Add more ISINs as needed
# }
# isin_dict = {7456:'INE657N07431'}
for sec_id, isin in tqdm(isin_dict.items(), desc="Fetching redemption data"):
    time.sleep(random.randint(1, 300))
    print(f"Processing ISIN: {isin}")
    raw_redemption = fetch_raw_redemption(isin = isin)
    if (raw_redemption.get('message') == 'No Record Found') or (raw_redemption == None):
        with common_variables.engine.begin() as conn:
            conn.execute(
                sqlalchemy.text("delete from security_ids where isin = :isin"),
                {"isin": isin}
            )
        continue
    # print(raw_redemption)
    maturity_date, coupon, frequency, face_value, issue_date = get_cashed_isin_meta_data(sec_id=sec_id,engine=common_variables.engine)
    # Build redemption payment schedule DataFrame similar to interest_payment_schedule
    if raw_redemption.get('redemptionType') == 'Partial Redemption By Face Value':
        redemption_schedule = [
            {
                'record_date': (pd.to_datetime(entry['partialRedemptionDates'], format="%d-%m-%Y", errors='coerce') - timedelta(days=15)).date(),
                'due_date': pd.to_datetime(entry['partialRedemptionDates'], format="%d-%m-%Y", errors='coerce').date(),
                'event_type': 'partial_redemption',
                'amount': entry.get("quantityRedeemed") or entry.get("valueRedeemed"),
                'coupon': None,
                'frequency': None
            }
            for entry in raw_redemption.get('redemption', [])
            if entry.get("quantityRedeemed") or entry.get("valueRedeemed")
            ]
        if redemption_schedule == []:
            redemption_schedule = get_missing_partial_redemption(isin=isin)
    if (raw_redemption.get('redemptionType') == 'Full Redemption'):
        redemption_schedule = [
            {
                'record_date': maturity_date - timedelta(days=15),
                'due_date': maturity_date,
                'event_type': 'full_redemption',
                'amount': face_value,
                'coupon': None,
                'frequency': None
            }
        ]
    print('isin:', isin)
    pprint(raw_redemption.get('redemption'))
    redemption_schedule = pd.DataFrame(redemption_schedule)
    print(redemption_schedule)
    # sys.exit()
    try:
        redemption_schedule['due_date'] = pd.to_datetime(redemption_schedule['due_date'], errors='coerce')
        redemption_schedule['record_date'] = pd.to_datetime(redemption_schedule['record_date'], errors='coerce')
        redemption_schedule['event_type'] = 'redemption'
        redemption_schedule['sec_id'] = sec_id
    except:
        continue
    original_face_value = redemption_schedule['amount'].sum()

    redemption_type = raw_redemption.get("redemptionType")
    interest_payment_frequency = interest_payment_frequency_dict.get(sec_id)
    frequency_integer = None
    while (not isinstance(frequency_integer, int)) or (frequency_integer not in [0,1, 2,3, 4,12]):
        try:
            frequency_integer = parse_redemption_data_with_llm(
                id=sec_id,
                interest_payment_frequency=interest_payment_frequency,
                coupon_rate=coupon,
            )
            frequency_integer = int(frequency_integer)
        except Exception as e:
            print(f"❌ Error parsing frequency for sec_id {sec_id}: {e}")
            frequency_integer = None
    if type_of_interest_rate(isin) == 'Variable-Others':
        continue
    try:
        record_dates, due_dates = get_record_due_dates(isin,maturity_date=maturity_date,issue_date=issue_date,frequency_integer=frequency_integer)
    except Exception as e:
        print(f"❌ Error generating record dates for ISIN {isin}: {e}")
        continue
    interest_payment_schedule = pd.DataFrame()
    interest_payment_schedule['record_date'] = record_dates
    interest_payment_schedule['due_date'] = due_dates
    interest_payment_schedule['due_date'] = pd.to_datetime(interest_payment_schedule['due_date'], errors='coerce')
    interest_payment_schedule['record_date'] = pd.to_datetime(interest_payment_schedule['record_date'], errors='coerce')
    interest_payment_schedule['event_type'] = 'interest_payment'
    interest_payment_schedule['amount'] = None
    interest_payment_schedule['coupon'] = coupon
    interest_payment_schedule['frequency'] = frequency_integer
    interest_payment_schedule['sec_id'] = sec_id
    # print(interest_payment_schedule)
    # print(pd.DataFrame(redemption_schedule))
    schedule = pd.concat([interest_payment_schedule, redemption_schedule], ignore_index=True)
    schedule = schedule.sort_values(by='due_date').reset_index(drop=True)
    schedule['face_value'] = face_value
    schedule['original_face_value'] = original_face_value

    schedule = schedule.sort_values(by='due_date').reset_index(drop=True)

    # Calculate current face value at each row
    current_face = schedule['original_face_value'].iloc[0]
    face_values = []

    for idx, row in schedule.iterrows():
        face_values.append(current_face)
        # Subtract redemption amount from current_face if this row is a redemption
        if row['event_type'] == 'redemption' and pd.notnull(row['amount']):
            current_face -= row['amount']

    schedule['face_value'] = face_values
    print(schedule)
    if coupon > 0:
        issue_date_df = pd.read_sql_query(f"""select issue_date from bond_metadata_prod b where b.sec_id = {sec_id}""", common_variables.engine)
        issue_date = pd.to_datetime(issue_date_df['issue_date'].iloc[0])
        schedule = calculate_interest_amounts(schedule,issue_date)
        print(schedule)
        # sys.exit()

    if coupon == 0:
        print(f'isin:{isin}')
        try:
            total_payment = get_zero_coupon_cashflows(isin,face_value)
            issue_date_df = pd.read_sql_query(f"""select issue_date from bond_metadata_prod b where b.sec_id = {sec_id}""", common_variables.engine)
            maturity_date_df = pd.read_sql_query(f"""select maturity from bond_metadata_prod b where b.sec_id = {sec_id}""", common_variables.engine)
            issue_date = pd.to_datetime(issue_date_df['issue_date'].iloc[0])
            maturity_date = pd.to_datetime(maturity_date_df['maturity'].iloc[0])
            issue_price = float(face_value)

            # Calculate IRR for zero coupon bond
            days = (maturity_date - issue_date).days
            print(f'issue_priceissue_price:{issue_price},total_payment:{total_payment},days:{days}')
            if issue_price > 0 and total_payment > 0 and days > 0:
                irr = (total_payment / issue_price) ** (365 / days) - 1
                print(f"Calculated IRR for ISIN {isin}: {irr:.6f}")
            else:
                irr = None
                print(f"❌ Could not calculate IRR for ISIN {isin}")

            if irr*100 > 20:
                total_payment = total_payment - float(face_value)
            schedule = pd.DataFrame()
            schedule = pd.DataFrame([
                {
                    'record_date': maturity_date - pd.Timedelta(days=15),
                    'due_date': maturity_date,
                    'event_type': 'interest_payment',
                    'amount': total_payment,
                    'coupon': 0,
                    'frequency': 0,
                    'sec_id': sec_id,
                    'face_value': face_value,
                    'original_face_value': face_value
                },
                {
                    'record_date': maturity_date - pd.Timedelta(days=15),
                    'due_date': maturity_date,
                    'event_type': 'redemption',
                    'amount': face_value,
                    'coupon': 0,
                    'frequency': 0,
                    'sec_id': sec_id,
                    'face_value': face_value,
                    'original_face_value': face_value
                }
            ])
        except Exception as e:
            import traceback, sys
            tb_str = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            print(f"Error occurred while creating schedule for ISIN {isin}, sec_id {sec_id}:\n{tb_str}")
            continue

    print(schedule)
# Save to database
    with common_variables.engine.begin() as conn:
        for _, row in schedule.iterrows():
            conn.execute(
                sqlalchemy.text("""
                    INSERT INTO bond_cashflows (
                        record_date,
                        due_date,
                        event_type,
                        amount,
                        coupon,
                        frequency,
                        sec_id,
                        face_value,
                        original_face_value,
                        created_at
                    )
                    VALUES (
                        :record_date,
                        :due_date,
                        :event_type,
                        :amount,
                        :coupon,
                        :frequency,
                        :sec_id,
                        :face_value,
                        :original_face_value,
                        :created_at
                    )
                    ON CONFLICT DO NOTHING
                """),
                {
                    "record_date": row['record_date'].strftime("%Y-%m-%d") if pd.notnull(row['record_date']) else None,
                    "due_date": row['due_date'].strftime("%Y-%m-%d") if pd.notnull(row['due_date']) else None,
                    "event_type": row['event_type'],
                    "amount": float(row['amount']) if pd.notnull(row['amount']) else 0.0,
                    "coupon": float(row['coupon']) if pd.notnull(row['coupon']) else None,
                    "frequency": int(row['frequency']) if pd.notnull(row['frequency']) else None,
                    "sec_id": int(row['sec_id']),
                    "face_value": float(row['face_value']),
                    "original_face_value": float(row['original_face_value']),
                    "created_at": today.strftime("%Y-%m-%d")
                }
            )
        print(f"✅ Redemption schedule for ISIN {isin} written to database.")
        