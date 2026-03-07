import time
import json
import requests

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text

from app.db import SessionLocal


# ======================================================
# CONFIG
# ======================================================

OI_URL = "https://fapi.binance.com/futures/data/openInterestHist"

IST = ZoneInfo("Asia/Kolkata")

CHECK_INTERVAL = 600  # seconds


# Supported TF
OI_TFS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
}


# ======================================================
# LOGGER
# ======================================================

def log(level, event, **data):

    ts = datetime.now(timezone.utc).astimezone(IST)

    record = {
        "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "event": event,
        **data
    }

    print(json.dumps(record))


# ======================================================
# SYMBOL SOURCE
# ======================================================

def get_symbols():

    session = SessionLocal()

    try:

        rows = session.execute(
            text("SELECT name FROM symbols")
        ).fetchall()

        return [r[0] for r in rows]

    finally:
        session.close()


# ======================================================
# LAST OI
# ======================================================

def get_last_oi(symbol, tf):

    session = SessionLocal()

    try:

        row = session.execute(
            text(
                """
                SELECT ts
                FROM open_interest
                WHERE symbol=:symbol
                AND tf=:tf
                ORDER BY ts DESC
                LIMIT 1
                """
            ),
            {"symbol": symbol, "tf": tf}
        ).fetchone()

        if row:
            return row[0]

        return None

    finally:
        session.close()


# ======================================================
# FETCH OI
# ======================================================

def fetch_oi(symbol, tf, start, end):

    params = {
        "symbol": symbol,
        "period": tf,
        "startTime": start,
        "endTime": end,
        "limit": 500
    }

    r = requests.get(OI_URL, params=params, timeout=10)

    r.raise_for_status()

    return r.json()


# ======================================================
# INSERT
# ======================================================

def insert_oi_batch(rows):

    if not rows:
        return

    session = SessionLocal()

    try:

        insert_sql = text(
            """
            INSERT INTO open_interest
            (symbol, tf, ts, open_interest)

            VALUES
            (:symbol, :tf, :ts, :open_interest)

            ON CONFLICT DO NOTHING
            """
        )

        session.execute(insert_sql, rows)

        session.commit()

    finally:
        session.close()


# ======================================================
# BACKFILL
# ======================================================

def backfill_oi(symbol, tf, start, end):

    log("INFO", "OI_BACKFILL_START", symbol=symbol, tf=tf)

    data = fetch_oi(symbol, tf, start, end)

    rows = []

    for r in data:

        rows.append({
            "symbol": symbol,
            "tf": tf,
            "ts": int(r["timestamp"]),
            "open_interest": float(r["sumOpenInterest"])
        })

    insert_oi_batch(rows)

    log(
        "INFO",
        "OI_BACKFILL_COMPLETE",
        symbol=symbol,
        tf=tf,
        rows=len(rows)
    )


# ======================================================
# CHECK SYMBOL
# ======================================================

def check_symbol_tf(symbol, tf, tf_ms):

    now = int(datetime.now(timezone.utc).timestamp() * 1000)

    expected = now - (now % tf_ms)

    last_ts = get_last_oi(symbol, tf)

    if last_ts is None:

        start = expected - tf_ms * 50
        end = expected

        backfill_oi(symbol, tf, start, end)

        return

    if last_ts < expected:

        start = last_ts + tf_ms
        end = expected

        backfill_oi(symbol, tf, start, end)


# ======================================================
# RUN HEALTH
# ======================================================

def run_health():

    symbols = get_symbols()

    log("INFO", "OI_HEALTH_START")

    for tf, tf_ms in OI_TFS.items():

        for symbol in symbols:

            try:

                check_symbol_tf(symbol, tf, tf_ms)

            except Exception as e:

                log(
                    "ERROR",
                    "OI_HEALTH_FAIL",
                    symbol=symbol,
                    tf=tf,
                    error=str(e)
                )

    log("INFO", "OI_HEALTH_COMPLETE")


# ======================================================
# SCHEDULER
# ======================================================

def scheduler():

    log("INFO", "OI_HEALTH_WORKER_STARTED")

    while True:

        start = time.time()

        run_health()

        elapsed = time.time() - start

        sleep_time = max(0, CHECK_INTERVAL - elapsed)

        log("INFO", "NEXT_OI_CHECK", seconds=int(sleep_time))

        time.sleep(sleep_time)


# ======================================================
# ENTRY
# ======================================================

if __name__ == "__main__":

    scheduler()