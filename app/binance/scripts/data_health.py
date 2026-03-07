import time
import json
import argparse
import requests

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import func

from app.db import SessionLocal
from app.config import TIMEFRAMES
from app.binance.scripts.insert import MODEL_MAP


# ==========================================================
# CONFIG
# ==========================================================

BINANCE_TIME_URL = "https://fapi.binance.com/fapi/v1/time"

IST = ZoneInfo("Asia/Kolkata")

DEFAULT_INTERVAL = 3600  # 1 hour


# ==========================================================
# LOGGER
# ==========================================================


def log(level, event, **data):

    now_ist = datetime.now(timezone.utc).astimezone(IST)

    record = {
        "ts": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "event": event,
        **data
    }

    line = json.dumps(record)

    print(line)

    log_fp.write(line + "\n")


# ==========================================================
# BINANCE TIME
# ==========================================================

def get_exchange_time_ms():

    r = requests.get(BINANCE_TIME_URL, timeout=5)

    r.raise_for_status()

    return r.json()["serverTime"]


# ==========================================================
# SYMBOL SOURCE
# ==========================================================

def get_symbols():

    session = SessionLocal()

    try:

        rows = session.execute("SELECT name FROM symbols").fetchall()

        return [r[0] for r in rows]

    finally:
        session.close()


# ==========================================================
# GAP CHECK
# ==========================================================

def check_symbol_tf(symbol, tf):

    Model = MODEL_MAP.get(tf)

    if not Model:
        return []

    tf_ms = int(TIMEFRAMES[tf]["tf_ms"])

    session = SessionLocal()

    try:

        rows = (
            session.query(Model.open_time)
            .filter(Model.symbol == symbol)
            .order_by(Model.open_time)
            .all()
        )

        timestamps = [r[0] for r in rows]

        gaps = []

        for i in range(1, len(timestamps)):

            prev_ts = timestamps[i - 1]
            curr_ts = timestamps[i]

            if curr_ts - prev_ts != tf_ms:

                expected = prev_ts + tf_ms

                gaps.append({
                    "symbol": symbol,
                    "tf": tf,
                    "missing_open_time": expected
                })

        return gaps

    finally:
        session.close()


# ==========================================================
# RUN TF CHECK
# ==========================================================

def run_tf_check(tf, symbols):

    log("INFO", "TF_HEALTH_CHECK_START", tf=tf)

    total_gaps = 0

    for symbol in symbols:

        gaps = check_symbol_tf(symbol, tf)

        if gaps:

            total_gaps += len(gaps)

            for g in gaps:

                log(
                    "WARN",
                    "MISSING_CANDLE",
                    symbol=g["symbol"],
                    tf=g["tf"],
                    open_time=g["missing_open_time"]
                )

    log(
        "INFO",
        "TF_HEALTH_CHECK_COMPLETE",
        tf=tf,
        gaps_found=total_gaps
    )


# ==========================================================
# HEALTH RUN
# ==========================================================

def run_health_check(target_tf=None):

    symbols = get_symbols()

    tfs = [target_tf] if target_tf else list(TIMEFRAMES.keys())

    for tf in tfs:

        if tf not in MODEL_MAP:
            continue

        run_tf_check(tf, symbols)


# ==========================================================
# SCHEDULER
# ==========================================================

def scheduler(interval, tf):

    log("INFO", "DATA_HEALTH_CHECKER_STARTED")

    # FIRST RUN IMMEDIATE
    run_health_check(tf)

    while True:

        exchange_time = get_exchange_time_ms()

        next_run = ((exchange_time // (interval * 1000)) + 1) * (interval * 1000)

        sleep_seconds = (next_run - exchange_time) / 1000

        log("INFO", "NEXT_RUN_WAIT", seconds=int(sleep_seconds))

        time.sleep(sleep_seconds)

        run_health_check(tf)


# ==========================================================
# CLI
# ==========================================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tf",
        help="Run for specific timeframe (optional)",
        default=None
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help="Run interval in seconds (default 3600)"
    )

    args = parser.parse_args()

    scheduler(args.interval, args.tf)


# ==========================================================
# ENTRY
# ==========================================================

if __name__ == "__main__":

    main()