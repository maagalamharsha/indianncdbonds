from numpy import isin
import requests,io,os,picologging,sys
import pandas as pd
import sqlalchemy
from globals import common_variables
from pprint import pprint
from datetime import datetime
from scipy.optimize import newton
from scipy.optimize import brentq

def get_logger(process_name):
    os.makedirs(f"logs/{process_name}_logs", exist_ok=True)
    logger = picologging.getLogger(process_name)
    logger.setLevel(picologging.INFO)

    if not logger.handlers:
        log_path = os.path.join("logs", f"{process_name}_logs", f"{process_name}.log")
        file_handler = picologging.FileHandler(log_path, mode='a', encoding='utf-8')
        formatter = picologging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        stream_handler = picologging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger

def get_kite_instruments():
    url = "https://api.kite.trade/instruments"
    response = requests.get(url)
    response.raise_for_status()
    # Read CSV into DataFrame
    df = pd.read_csv(io.StringIO(response.text))
    return df


import requests
import requests

def fetch_bse_bond_metadata(scrip_code):
    url = f"https://api.bseindia.com/BseIndiaAPI/api/DebSecurityInfo/w?scripcode={scrip_code}"

    response = requests.get(url, headers=common_variables.headers_bse, timeout=10)
    try:
        return response.json()
    except Exception as e:
        print("❌ Failed to parse JSON. Raw response was:")
        print(response.text)
        raise

def get_isin_meta_data(isin):
    instrument_url = f"https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/instruments?isin={isin}"
    isin_url = f"https://www.indiabondinfo.nsdl.com/bds-service/v1/public/isins?isin={isin}"
    coupon_url = f'https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin={isin}'

    
    # common_variables.logger.info(f"🔍 Fetching metadata for ISIN: {isin}")
    isin_response = requests.get(isin_url, headers=common_variables.headers_nsdl, timeout=10)
    instrument_response = requests.get(instrument_url, headers=common_variables.headers_nsdl, timeout=10)
    coupon_response = requests.get(coupon_url, headers=common_variables.headers_nsdl, timeout=10)
    instrument = instrument_response.json().get("instrumentsVo", {}).get("instruments", {})

    return isin_response.json(), instrument, coupon_response.json()

def parse_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
    except Exception:
        return None
    
from sqlalchemy import text

def get_cashed_isin_meta_data(engine, sec_id):
    query = text("""
        SELECT maturity, coupon, frequency, face_value, issue_date
        FROM bond_metadata_prod
        WHERE sec_id = :sec_id
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {"sec_id": sec_id}).fetchone()

    if result is None:
        return None, None, None, None, None
    print(result[0], result[1], result[2], result[3], result[4])
    return result[0], result[1], result[2], result[3], result[4]  # or result["maturity"]... if using RowMapping

def genereate_record_due_dates(isin, maturity_date=None, issue_date=None, frequency_integer=None):
    """
    Manually generate record dates and due dates based on frequency.
    frequency_integer: number of payments per year (e.g., 1=annual, 2=semi-annual, 4=quarterly)
    """
    from datetime import timedelta

    if not (maturity_date and issue_date and frequency_integer):
        raise ValueError("maturity_date, issue_date, and frequency_integer are required.")

    # Convert to datetime.date if needed
    if isinstance(maturity_date, str):
        maturity_date = datetime.strptime(maturity_date, "%Y-%m-%d").date()
    if isinstance(issue_date, str):
        issue_date = datetime.strptime(issue_date, "%Y-%m-%d").date()

    # Calculate interval in days
    interval_days = int(365 / frequency_integer)
    dates = []
    current_due_date = issue_date

    while current_due_date < maturity_date:
        record_date = current_due_date - timedelta(days=15)
        dates.append({
            "record_date": record_date,
            "due_date": current_due_date
        })
        current_due_date += timedelta(days=interval_days)

    # Ensure last payment is at maturity
    if dates and dates[-1]["due_date"] < maturity_date:
        record_date = maturity_date - timedelta(days=15)
        dates.append({
            "record_date": record_date,
            "due_date": maturity_date
        })

    df = pd.DataFrame(dates)
    record_date = df['record_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    due_date = df['due_date'].apply(lambda x: x.strftime("%Y-%m-%d"))

    return record_date, due_date

# Example usage:
def get_missing_partial_redemption(isin):
    coupon_url = f'https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin={isin}'
    response = requests.get(coupon_url, headers=common_variables.headers_nsdl, timeout=10)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch coupon details for ISIN {isin}. Status code: {response.status_code}")
    data = response.json()
    partial_redemptions = [
    {
        "record_date": item["recordDate"],
        "due_date": item["paymentDate"],
        "event_type": 'partial_redemption',
        "amount": item["amountPayable"],
        'coupon': None,
        'frequency': None
    }
    for item in data["coupensVo"]["cashFlowScheduleDetails"]["cashFlowSchedule"]
    if "Redemption" in str(item.get("cashFlowsEvent", ""))
]
    pprint(partial_redemptions)
    return partial_redemptions

def get_record_due_dates(isin, maturity_date=None, issue_date=None,frequency_integer=None):
    coupon_url = f'https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin={isin}'
    response = requests.get(coupon_url, headers=common_variables.headers_nsdl, timeout=10)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch coupon details for ISIN {isin}. Status code: {response.status_code}")
    # Assume your dictionary is stored in variable `data`
    data = response.json()
    cashflows = data['coupensVo']['cashFlowScheduleDetails']['cashFlowSchedule']

    record_dates = []
    due_dates = []
    try:
        if len(cashflows) > 0:
            for entry in cashflows:
                record_date = entry.get('recordDate')
                due_date = entry.get('dueDate')
                if record_date:
                    record_dates.append(record_date)
                if due_date:
                    due_dates.append(due_date)

            # Remove duplicates and sort
            record_dates = [datetime.strptime(d, "%d-%m-%Y").date() for d in sorted(set(record_dates)) if d]
            due_dates =  [datetime.strptime(d, "%d-%m-%Y").date() for d in sorted(set(due_dates)) if d]
            record_dates = sorted(set(record_dates))
            due_dates = sorted(set(due_dates))
            if any(rd > dd for rd, dd in zip(record_dates, due_dates)):
                print("Inconsistent record and due dates found:")
                print("Record Dates:", record_dates)
                print("Due Dates:", due_dates)
                record_dates, due_dates = genereate_record_due_dates(isin, maturity_date=maturity_date, issue_date=issue_date, frequency_integer=frequency_integer)
    except:
        record_dates, due_dates = genereate_record_due_dates(isin, maturity_date=maturity_date, issue_date=issue_date, frequency_integer=frequency_integer)
        # Remove duplicates and sort
        record_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in sorted(set(record_dates)) if d]
        due_dates =  [datetime.strptime(d, "%Y-%m-%d").date() for d in sorted(set(due_dates)) if d]
        record_dates = sorted(set(record_dates))
        due_dates = sorted(set(due_dates))

    # If record_dates is empty, generate them by subtracting 15 days from each due_date
    # Ensure record_dates and due_dates have the same length
    if len(record_dates) != len(due_dates):
        # If record_dates is empty or length mismatch, generate by subtracting 15 days from each due_date
        print(due_dates)
        record_dates = [
            (datetime.strptime(d, "%d-%m-%Y").date() if isinstance(d, str) else d) - pd.Timedelta(days=15)
            for d in due_dates
        ]
    
    return record_dates, due_dates

def get_zero_coupon_cashflows(isin,face_value):
    coupon_url = f'https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin={isin}'
    response = requests.get(coupon_url, headers=common_variables.headers_nsdl, timeout=10)
    data = response.json()
    cashflows = data['coupensVo']['cashFlowScheduleDetails']['cashFlowSchedule']
    interest_payments = [item["amountPayable"] for item in cashflows if item["cashFlowsEvent"] == "Interest"]
    if interest_payments == []:
        interest_payments = [
        item.get("amountPayable")
        for item in cashflows
        if "redemption" in str(item.get("cashFlowsEvent", "")).lower()
        and item.get("amountPayable") is not None
    ]
        return interest_payments[0] - float(face_value)
    return interest_payments[0]

def type_of_interest_rate(isin):
    coupon_url = f'https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin={isin}'
    response = requests.get(coupon_url, headers=common_variables.headers_nsdl, timeout=10)
    data = response.json()
    cashflows = data['coupensVo']['couponDetails']['couponBasis']
    return cashflows

def get_bond_cashflows(query):
    bond_cashflows = pd.read_sql(query,con=common_variables.engine)

    return bond_cashflows.sort_values("due_date").reset_index(drop=True)

def xnpv(rate, values, dates):
    return sum(v / (1 + rate) ** ((d - dates[0]).days / 365.0) for v, d in zip(values, dates))

def xirr(values, dates):
    # search in a reasonable positive range, e.g. [-0.9999, 10]
    return brentq(lambda r: xnpv(r, values, dates), -0.9999, 10)

# query_str = f"""
# SELECT record_date, due_date, amount
# FROM bond_cashflows
# WHERE sec_id = 9090
# and record_date > CURRENT_DATE + INTERVAL '2 days'
# ORDER BY due_date
# """
# common_variables.engine = sqlalchemy.create_engine("postgresql+psycopg2://localhost/production")

# df = get_bond_cashflows(query=query_str)
# values = [-1097.69, 105.0, 1000.0]
# dates  = [
#     pd.Timestamp("2025-08-19"),
#     pd.Timestamp("2025-09-06"),
#     pd.Timestamp("2025-09-06"),
# ]
# xirr_val = xirr(values, dates)
# print(df)
# print(values)
# print(dates)
# print(xirr_val)

# df = get_zero_coupon_cashflows(isin = 'INE549K07BQ8')
# print(df)
# A,B,C = get_isin_meta_data(isin="INE540P07244")
# pprint(C)
# pprint(B.get('faceValue'))

# A,B,C,D,E = get_cashed_isin_meta_data(sec_id="6744", engine=sqlalchemy.create_engine("postgresql+psycopg2://localhost/production"))
# print(A, B, C, D, E)

# get_record_due_dates("INE540P07244")
# record_dates, due_dates = get_record_due_dates("INE01CY079E7")
# print(record_dates, due_dates)
# isin = "INE539K08179"
# coupon_url = f'https://www.indiabondinfo.nsdl.com/bds-service/v1/public/bdsinfo/coupondetail?isin={isin}'
# response = requests.get(coupon_url, headers=common_variables.headers_nsdl, timeout=10)
# data = response.json()
# cashflows = data['coupensVo']['cashFlowScheduleDetails']['cashFlowSchedule']
# pprint(cashflows)

# type_of_interest_rate(isin = 'INE539K08179')
common_variables.engine = sqlalchemy.create_engine(
    "postgresql+psycopg2://localhost/production"
)

# df = pd.read_sql_query("select * from security_ids;",common_variables.engine)
# common_variables.isin_mapping = df.set_index('sec_id')['isin'].to_dict()

# query = sqlalchemy.text("""
#         SELECT record_date, due_date, amount
#         FROM bond_cashflows
#         WHERE sec_id = :sec_id
#         ORDER BY due_date
#                 """)
# common_variables.sec_id = "7449"
# temp_df = get_bond_cashflows(query = query)
# print(temp_df)
# sys.exit(0)
