import requests
import json
from datetime import datetime, timezone

from sqlalchemy import text
from app.db import SessionLocal
from app.redis_client import redis_client


# =============================
# CONFIG
# =============================

OI_URL = "https://fapi.binance.com/futures/data/openInterestHist"

INTERVAL = "1h"
INTERVAL_MS = 60 * 60 * 1000
BACKFILL_DAYS = 30
REDIS_KEY = "liquid_coins"
LIMIT = 1000


# =============================
# TIME HELPERS
# =============================

def get_latest_closed_hour_ms():
    now_utc = datetime.now(timezone.utc)
    closed_hour = now_utc.replace(minute=0, second=0, microsecond=0)
    return int(closed_hour.timestamp() * 1000)


# =============================
# DB HELPERS
# =============================

def get_latest_oi_time(symbol):
    db = SessionLocal()
    try:
        q = text("""
            SELECT MAX(open_time)
            FROM open_interest_1h
            WHERE symbol = :symbol
        """)
        row = db.execute(q, {"symbol": symbol}).fetchone()
        return int(row[0]) if row and row[0] else None
    finally:
        db.close()


def insert_oi_batch(rows):
    if not rows:
        return

    db = SessionLocal()
    try:
        q = text("""
            INSERT INTO open_interest_1h
            (symbol, open_time, open_time_utc,
             open_interest, oi_notional)
            VALUES (:symbol, :open_time, :open_time_utc,
                    :open_interest, :oi_notional)
            ON CONFLICT (symbol, open_time)
            DO UPDATE SET
                open_interest = EXCLUDED.open_interest,
                oi_notional = EXCLUDED.oi_notional,
                open_time_utc = EXCLUDED.open_time_utc
        """)
        db.execute(q, rows)
        db.commit()
    finally:
        db.close()


# =============================
# API CALL
# =============================

def fetch_oi(symbol, start_time, end_time):
    params = {
        "symbol": symbol,
        "period": INTERVAL,
        "startTime": start_time,
        "endTime": end_time,
        "limit": LIMIT
    }

    resp = requests.get(OI_URL, params=params, timeout=10)

    if resp.status_code != 200:
        print(f"Failed OI fetch for {symbol}")
        return []

    return resp.json()


# =============================
# CORE SYNC
# =============================

def sync_symbol_oi(symbol):

    print(f"\nProcessing OI for {symbol}")

    latest_closed = get_latest_closed_hour_ms()
    latest_db = get_latest_oi_time(symbol)

    # Determine backfill start
    if not latest_db:
        print("First sync → 30-day OI backfill")
        start_ts = latest_closed - (BACKFILL_DAYS * 24 * INTERVAL_MS)
    else:
        if latest_db >= latest_closed:
            print("OI already up to date.")
            return
        start_ts = latest_db + INTERVAL_MS

    end_ts = latest_closed

    oi_data = fetch_oi(symbol, start_ts, end_ts)

    if not oi_data:
        print("No OI data returned.")
        return

    rows = []

    for item in oi_data:

        # Raw timestamp from Binance (UTC ms)
        ts = int(item["timestamp"])

        # Normalize to exact 1H boundary
        ts = ts - (ts % INTERVAL_MS)

        # Convert to UTC datetime
        open_time_utc = datetime.fromtimestamp(
            ts / 1000,
            tz=timezone.utc
        )

        rows.append({
            "symbol": item["symbol"],  # safer than outer symbol
            "open_time": ts,
            "open_time_utc": open_time_utc,
            "open_interest": float(item["sumOpenInterest"]),
            "oi_notional": float(item["sumOpenInterestValue"]),
            "cmc_circulating_supply": (
                float(item["CMCCirculatingSupply"])
                if item.get("CMCCirculatingSupply")
                else None
            )
        })

    insert_oi_batch(rows)

    print(f"Inserted/Updated {len(rows)} OI rows")


# =============================
# ENTRY
# =============================

if __name__ == "__main__":

    symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")

    print("Starting standalone OI sync...\n")

    for symbol in symbols:
        sync_symbol_oi(symbol)

    print("\nAll OI symbols processed.")