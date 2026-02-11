import os
import random
import sys
import time
import sqlalchemy
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, desc, text
from common_util import get_logger,get_kite_instruments,fetch_bse_bond_metadata,get_isin_meta_data,parse_date
import requests
from open_ai_api_calls import fetch_new_isins_from_nsdl, extract_bond_metadata
from globals import common_variables
from tqdm import tqdm
from pprint import pprint
common_variables.logger = get_logger("meta_data")

engine = sqlalchemy.create_engine(
    "postgresql+psycopg2://localhost/production"
)

kite_instruments = get_kite_instruments()
kite_instruments = kite_instruments[kite_instruments['exchange'].str.lower() == 'bse']
common_variables.logger.info(f"✅ Fetched {len(kite_instruments)} instruments from Kite API.")
with engine.begin() as conn:
    result = conn.execute(text("SELECT COALESCE(MAX(sec_id), 0) FROM security_ids"))
    current_max_sec_id = result.scalar_one()
    bad_isin_df = pd.read_sql_query('select isin,tradingsymbol from bad_isins_symbols', engine)
    existing_symbols = pd.read_sql_query('select s.tradingsymbol  from security_ids s join bond_metadata_prod b on s.sec_id = b.sec_id', engine)
    existing_symbols = existing_symbols['tradingsymbol'].tolist()
    bad_trading_symbol = bad_isin_df['tradingsymbol'].tolist()

    for symbol, exchange_token in tqdm(zip(kite_instruments['tradingsymbol'].tolist(), kite_instruments['exchange_token'].tolist()), desc="Processing symbols"):
        if symbol in bad_trading_symbol or symbol in existing_symbols:
            continue
        data = fetch_bse_bond_metadata(exchange_token)
        if not data or not data.get('Table') or not data.get('Table')[0].get('ISSebiIsin'):
            try:
                with engine.begin() as conn_bad_1:
                    conn_bad_1.execute(
                        text("INSERT INTO bad_isins_symbols (tradingsymbol, isin) VALUES (:tradingsymbol, :isin) ON CONFLICT (tradingsymbol) DO NOTHING;"),
                        { "tradingsymbol": symbol, "isin": None }
                    )
                print(f"INSERT INTO bad_isins_symbols (tradingsymbol, isin) VALUES ('{symbol}', NULL) ON CONFLICT (tradingsymbol) DO NOTHING;")
            except Exception as e:
                common_variables.logger.error(f"Error inserting bad ISIN for {symbol}: {e}")
            # common_variables.logger.warning(f"❌ No data found for {symbol}, skipping.")
            continue
        isin = data.get('Table')[0].get('ISSebiIsin')
        isin_details, instrument, coupon = get_isin_meta_data(isin)
        if (isin_details.get('isin') is None) | (instrument == {}) | (instrument.get('message') == 'No Record Found'):
            try:
                with engine.begin() as conn_bad_2:
                    conn_bad_2.execute(
                        text("INSERT INTO bad_isins_symbols (tradingsymbol, isin) VALUES (:tradingsymbol, :isin) ON CONFLICT (tradingsymbol) DO NOTHING;"),
                        {"tradingsymbol": symbol, "isin": isin}
                    )
                continue
            except Exception as e:
                common_variables.logger.error(f"Error inserting bad ISIN for {symbol}: {e}")
        coupon_rate_str = str(coupon.get('coupensVo', {}).get('couponDetails', {}).get('couponRate', '0'))
        coupon_rate_clean = coupon_rate_str.split('%')[0].strip()
        try:
            coupon_rate = float(coupon_rate_clean) if coupon_rate_clean else 0.0
        except ValueError:
            coupon_rate = 0.0
        frequency = coupon.get('coupensVo', {}).get('couponDetails', {}).get('interestPaymentFrequency', 'NA')
        common_variables.logger.info(f"✅ Fetched metadata for ISIN: {isin}")
        current_max_sec_id += 1
        conn.execute(
            text("""
                INSERT INTO bond_metadata_prod (sec_id, maturity, coupon, frequency, issuer, issue_date, sector, industry, initial_discovery_date, face_value)
                VALUES (:sec_id, :maturity, :coupon, :frequency, :issuer, :issue_date, :sector, :industry, :initial_discovery_date, :face_value)
                ON CONFLICT (sec_id) DO UPDATE SET
                    maturity = EXCLUDED.maturity,
                    coupon = EXCLUDED.coupon,
                    frequency = EXCLUDED.frequency,
                    issuer = EXCLUDED.issuer,
                    issue_date = EXCLUDED.issue_date,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    initial_discovery_date = EXCLUDED.initial_discovery_date,
                    face_value = EXCLUDED.face_value;
            """),
            {
                "sec_id": current_max_sec_id,
                "maturity": parse_date(instrument.get('redemptionDate')),
                "coupon": coupon_rate,
                "frequency": frequency,
                "issuer": isin_details['issuerName'],
                "issue_date": parse_date(instrument.get('allotmentDate')),
                "sector": isin_details['sector'],
                "industry": isin_details['industry'],
                "initial_discovery_date": datetime.now().date(),
                "face_value": instrument.get('faceValue')
            }
            )
        conn.execute(
            text(
                "INSERT INTO security_ids (sec_id, isin, tradingsymbol, captured_at)\n"
                "VALUES (:sec_id, :isin, :tradingsymbol, :captured_at);"
            ),
            {
                "sec_id": current_max_sec_id,
                "isin": isin,
                "tradingsymbol": symbol,
                "captured_at": datetime.now()
            }
        )
        print(f"✅ {isin} ({symbol}) metadata written to database.")
        time.sleep(random.randint(1, 5))

