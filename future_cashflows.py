import pandas as pd
from kiteconnect import KiteConnect
from datetime import datetime, timedelta
import sqlalchemy,sys,os
from pprint import pprint
from globals import common_variables
from common_util import get_bond_cashflows
from flask import Flask, request, render_template_string
from sqlalchemy import create_engine, text
import colorsys, hashlib  # added for coloring

access_token_path = os.path.join(os.path.dirname(__file__), "..", "access_token.txt")
with open(access_token_path, "r") as f:
    ACCESS_TOKEN = f.read().strip()

REDEMPTION_VALUE = 1000
API_KEY = "tu4kpuy8ikx7jge3"

# === Kite Connect setup ===
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)
holdings = kite.holdings()
margins = kite.margins(segment="equity")
cash_balance = margins['available']['cash']
print("Current Cash Balance:", cash_balance)

print("✅ Holdings and funds fetched successfully.")
holdings = pd.DataFrame(holdings)
holdings['quantity'] = holdings['quantity'].fillna(0) + holdings['t1_quantity'].fillna(0)
print(holdings)
sys.exit()
holdings = holdings[['tradingsymbol', 'quantity', 'average_price']]
yesterday = (datetime.now() - timedelta(days=1)).date()
holdings['asofdate'] = pd.to_datetime(yesterday)

common_variables.engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://localhost/production"    )

print(holdings)
with common_variables.engine.connect() as conn:
    holdings.to_sql("current_holdings", conn, if_exists="replace", index=False)
print("✅ Data written to current_holdings table.")

delete_query = """
DELETE FROM current_holdings
WHERE tradingsymbol IN (
    SELECT DISTINCT s.tradingsymbol
    FROM bond_metadata_prod meta
    JOIN security_ids s
      ON meta.sec_id = s.sec_id
    WHERE meta.maturity < CURRENT_DATE
);
"""

with conn.cursor() as cur:
    cur.execute(delete_query)

conn.commit()
# get FI Instruments
temp_df = pd.DataFrame()
sec_id_df = pd.read_sql_query("select sec_id, tradingsymbol from security_ids", common_variables.engine)
sec_id_tradingsymbol_dict = sec_id_df.set_index('sec_id')['tradingsymbol'].to_dict()
tradingsymbol_sec_id_dict = {v: k for k, v in sec_id_tradingsymbol_dict.items()}

print(holdings)
for tradingsymbol in set(holdings['tradingsymbol'].tolist()):
    if tradingsymbol in tradingsymbol_sec_id_dict.keys():
        print(f"Fetching cashflows for ISIN: {tradingsymbol}")
        print(f'its sec_id:{tradingsymbol_sec_id_dict.get(tradingsymbol)}')
        query_str = f"""
        SELECT due_date,event_type, amount
        FROM bond_cashflows
        WHERE sec_id = {tradingsymbol_sec_id_dict.get(tradingsymbol)}
        and due_date > CURRENT_DATE
        ORDER BY due_date
        """
        print(query_str)
        try:
            isin_cashflows = get_bond_cashflows(query = query_str)
            isin_qty = holdings['quantity'][holdings['tradingsymbol'] == tradingsymbol].sum()
            isin_cashflows['total_cashflow'] = isin_cashflows['amount'] * isin_qty
            isin_cashflows['sec_id'] = tradingsymbol_sec_id_dict.get(tradingsymbol)
            print(isin_cashflows)
            temp_df = pd.concat([temp_df, isin_cashflows], ignore_index=True)
        except Exception as e:
            print(f"⚠️ Error fetching cashflows for {tradingsymbol}: {e}")

print(temp_df)

# with common_variables.engine.connect() as conn:
#     conn.execute(text("DELETE FROM future_bond_cashflows_prod;"))
#     conn.commit()
#     temp_df.to_sql("future_bond_cashflows_prod", conn, if_exists="append", index=False)
# print("✅ Data written to future_total_cashflows table.")

sql = """SELECT 
    sec_id.sec_id,
    sec_id.isin,
    sec_id.tradingsymbol,
    meta.issuer,
    meta.maturity,
    cash.due_date,
    cash.event_type,
    cash.amount AS amount_per_bond,
    cash.total_cashflow / cash.amount AS quantity,
    cash.total_cashflow
FROM future_bond_cashflows_prod AS cash
JOIN security_ids AS sec_id
    ON cash.sec_id = sec_id.sec_id
JOIN bond_metadata_prod AS meta
    ON meta.sec_id = cash.sec_id
ORDER BY cash.due_date;"""

# === Flask App ===
app = Flask(__name__)
engine = create_engine("postgresql+psycopg2://localhost/production")

@app.route("/bonds")
def show_bonds():
    df = pd.read_sql(sql, engine)
    df['due_date'] = pd.to_datetime(df['due_date'])
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)
    if year:
        df = df[df['due_date'].dt.year == year]
    if month:
        df = df[df['due_date'].dt.month == month]

    # Apply color styling by event type
    def highlight_event(row):
        event = row["event_type"].lower()

        if event == "interest_payment":
            return ['background-color: #e5e5e5; color: black;'] * len(row)

        if event == "redemption":
            return ['background-color: white; color: black; font-weight: bold;'] * len(row)

        return ['background-color: white; color: black;'] * len(row)

    styled_df = (
        df.style
        .apply(highlight_event, axis=1)
        .set_table_styles(
            [
                {"selector": "th", "props": [("background-color", "#1f2937"), ("color", "white"), ("font-size", "14px")]},
                {"selector": "td", "props": [("font-size", "13px"), ("padding", "6px")]},
                {"selector": "table", "props": [("border", "1px solid #ccc"), ("border-collapse", "collapse"), ("width", "100%")]}
            ]
        )
    )

    html_template = """
    <html>
    <head>
        <title>Bond Cashflows</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="font-family:sans-serif; margin:20px;">
        <h2>Bond Cashflows</h2>
        <form method="get">
            Year: 
            <select name="year">
                <option value="">All</option>
                {% for y in years %}
                    <option value="{{y}}" {% if y==selected_year %}selected{% endif %}>{{y}}</option>
                {% endfor %}
            </select>
            Month:
            <select name="month">
                <option value="">All</option>
                {% for m in months %}
                    <option value="{{m}}" {% if m==selected_month %}selected{% endif %}>{{m}}</option>
                {% endfor %}
            </select>
            <input type="submit" value="Filter">
        </form>
        <br>
        {{table|safe}}
    </body>
    </html>
    """

    years = sorted(df['due_date'].dt.year.unique())
    months = list(range(1, 13))
    return render_template_string(
        html_template,
        table=styled_df.to_html(),
        years=years,
        months=months,
        selected_year=year,
        selected_month=month
    )

@app.route("/")
def index():
    print("Open this in your browser: http://127.0.0.1:1969/bonds")
    return '<h3>✅ Flask is running. Open <a href="/bonds">/bonds</a></h3>'

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1969)