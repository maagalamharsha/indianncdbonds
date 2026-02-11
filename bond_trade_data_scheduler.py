import time
from datetime import datetime, timedelta
from bonds_trade_data import get_bond_trade_data

MARKET_START = {"hour": 9, "minute": 15}
MARKET_END   = {"hour": 15, "minute": 30}
INTERVAL_MIN = 15

def next_run_time_from(base_dt: datetime, interval_min: int = INTERVAL_MIN) -> datetime:
    """
    Return the next wall-clock time after base_dt that is a multiple of interval_min minutes.
    Robust for hour/day rollovers.
    """
    interval_sec = interval_min * 60
    seconds_since_midnight = base_dt.hour * 3600 + base_dt.minute * 60 + base_dt.second
    next_bucket = ((seconds_since_midnight // interval_sec) + 1) * interval_sec
    delta_sec = next_bucket - seconds_since_midnight
    return base_dt + timedelta(seconds=delta_sec)

def market_time_today(hour: int, minute: int) -> datetime:
    now = datetime.now()
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

def run_scheduler():
    start_time = market_time_today(MARKET_START["hour"], MARKET_START["minute"])
    end_time = market_time_today(MARKET_END["hour"], MARKET_END["minute"])

    while True:
        now = datetime.now()

        # If before market start, wait until market start
        if now < start_time:
            wait = (start_time - now).total_seconds()
            print(f"⏳ Market not open yet. Sleeping {int(wait)}s until {start_time.strftime('%H:%M:%S')}")
            time.sleep(wait)
            continue

        # If past market end, break
        if now > end_time:
            print(f"🔔 Market closed for today at {end_time.strftime('%H:%M:%S')}. Exiting.")
            break

        # Capture run_time as close as possible to the actual fetch
        run_time = datetime.now()
        print(f"▶️  Starting run at {run_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  (passed to get_bond_trade_data)")

        # Call your main data function with the captured timestamp
        try:
            get_bond_trade_data(run_time=run_time)
        except Exception as e:
            print(f"❗ Error in get_bond_trade_data: {e}")

        finished = datetime.now()

        # Compute next run time from 'finished' (so we align to actual completion time)
        next_time = next_run_time_from(finished, INTERVAL_MIN)
        sleep_seconds = (next_time - datetime.now()).total_seconds()

        # Defensive: if negative (some drift), compute from now
        if sleep_seconds < 0:
            next_time = next_run_time_from(datetime.now(), INTERVAL_MIN)
            sleep_seconds = (next_time - datetime.now()).total_seconds()

        print(f"Sleeping for {int(sleep_seconds)}s until next run at {next_time.strftime('%H:%M:%S')}")
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    run_scheduler()