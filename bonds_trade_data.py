from globals import common_variables
import pandas as pd
run_time = pd.Timestamp.now()

def get_bond_trade_data(run_time):
    # from cash_flow_engine import get_bond_cashflows
    import numpy as np
    import pandas as pd
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    from kiteconnect import KiteConnect
    from scipy.optimize import newton
    import sqlalchemy
    # from zero_coupon_redemption_meta_data import redemption_dict
    import time
    from tqdm import tqdm
    import os,sys
    from pprint import pprint
    from common_util import get_bond_cashflows,xnpv,xirr
    import traceback
    access_token_path = os.path.join(os.path.dirname(__file__), "..", "access_token.txt")
    with open(access_token_path, "r") as f:
        ACCESS_TOKEN = f.read().strip()

    API_KEY = "tu4kpuy8ikx7jge3"

    today = datetime.today()
    common_variables.engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://localhost/production"
    )

    # === Redemption dict (imported beforehand) ===
    # Ensure `redemption_dict` is available in scope

    # === Load bonds ===
    df = pd.read_sql_query("select * from security_ids;",common_variables.engine)
    common_variables.secid_to_isin_mapping = df.set_index('sec_id')['isin'].to_dict()
    common_variables.symbol_to_secid_mapping = df.set_index('tradingsymbol')['sec_id'].to_dict()

    # === Kite Connect setup ===
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    # === Get best ask prices ===
    exchange_tokens = df["tradingsymbol"].tolist()
    symbols = [f"BSE:{token}" for token in exchange_tokens]


    depth_data = {}
    batch_size = 200
    sec_id = []
    xirr_list = []
    for i in tqdm(range(0, len(symbols), batch_size), desc="Fetching quote data"):
        batch = symbols[i:i + batch_size]
        try:
            # Use quote() for full depth (ask/qty); use ltp() if only price is needed
            batch_quotes = kite.quote(batch)  # or kite.ltp(batch)
            depth_data.update(batch_quotes)
        except Exception as e:
            print(f"⚠️ Failed to get batch {i}-{i+batch_size}: {e}")
        time.sleep(3)  # Avoid hitting API rate limits

    print("✅ Quote data fetched successfully.")
    # === Helper: get next coupon date ===
    def next_coupon(issue_date, freq, from_date):
        if freq == 'zero':
            return from_date
        current = issue_date
        step = {"monthly": 1, "quarterly": 3, "annually": 12}.get(freq.lower(), 1)
        while current <= from_date:
            current += relativedelta(months=step)
        return current


    # === Main Calculation ===
   
    prices = []
    quantities = []
    for symbol in tqdm(depth_data.keys()):
        clean_symbol = symbol.replace("BSE:", "")
        try:
            price = depth_data[symbol]["depth"]["sell"][0]["price"]
            qty = depth_data[symbol]["depth"]["sell"][0]["quantity"]
        except (KeyError, IndexError, TypeError):
            price = None
            qty = None
        common_variables.sec_id = common_variables.symbol_to_secid_mapping.get(clean_symbol, None)
        query_str = f"""
        SELECT record_date, due_date, amount
        FROM bond_cashflows
        WHERE sec_id = {common_variables.sec_id}
        and record_date > CURRENT_DATE + INTERVAL '2 days'
        ORDER BY due_date
        """
        cashflows_df = get_bond_cashflows(query = query_str)
        sec_id.append(common_variables.sec_id)
        prices.append(price)
        quantities.append(qty)
        # Assume the first cashflow is a negative investment (purchase price)
        # If not, prepend a negative value as needed
        cashflow_amounts = [-price] + cashflows_df["amount"].tolist()
        cashflow_dates = [pd.to_datetime(today)] + pd.to_datetime(cashflows_df["due_date"]).tolist()
        try:
            xirr_val = xirr(cashflow_amounts, cashflow_dates)
            xirr_list.append(round(xirr_val * 100,2))
        except Exception as e:
            xirr_list.append(0)
    # Final output
    final_df = pd.DataFrame()
    final_df['sec_id'] = sec_id
    final_df["price"] = prices
    final_df["qty"] = quantities
    final_df["xirr"] = xirr_list
    final_df['time_stamp'] = run_time

    print(final_df.sort_values(by="xirr", ascending=False))

    # Now append the new data
    final_df = final_df[(final_df['price'] > 0) & (final_df['xirr'] > 0)]
    final_df.to_sql("bond_trade_data_prod", common_variables.engine, if_exists="append", index=False)
    print("✅ Data written to bond_trade_data_prod table.")

    # # df.to_sql("bond_trade_data", engine, if_exists="append", index=False)
    # print("✅ Data written to bond_trade_data table.")
