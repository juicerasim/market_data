import requests
import json
from datetime import datetime, timezone

from sqlalchemy import text
from app.db import SessionLocal
from app.redis_client import redis_client


# =============================
# CONFIG
# =============================

FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

REDIS_KEY = "liquid_coins"
BACKFILL_DAYS = 30
LIMIT = 1000  # Binance max per request


# =============================
# TIME HELPERS
# =============================

def now_utc_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# =============================
# DB HELPERS
# =============================

def get_latest_funding_time(symbol):
    db = SessionLocal()
    try:
        q = text("""
            SELECT MAX(funding_time)
            FROM funding_rate_8h
            WHERE symbol = :symbol
        """)
        row = db.execute(q, {"symbol": symbol}).fetchone()
        return int(row[0]) if row and row[0] else None
    finally:
        db.close()


def insert_funding_batch(rows):
    if not rows:
        return

    db = SessionLocal()
    try:
        q = text("""
            INSERT INTO funding_rate_8h
            (symbol, funding_time, funding_time_utc,
             funding_rate, mark_price)
            VALUES (:symbol, :funding_time, :funding_time_utc,
                    :funding_rate, :mark_price)
            ON CONFLICT (symbol, funding_time)
            DO UPDATE SET
                funding_rate = EXCLUDED.funding_rate,
                mark_price = EXCLUDED.mark_price,
                funding_time_utc = EXCLUDED.funding_time_utc
        """)
        db.execute(q, rows)
        db.commit()
    finally:
        db.close()


# =============================
# API CALL
# =============================

def fetch_funding(symbol, start_time=None, end_time=None):

    params = {
        "symbol": symbol,
        "limit": LIMIT
    }

    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    resp = requests.get(FUNDING_URL, params=params, timeout=10)

    if resp.status_code != 200:
        print(f"Funding fetch failed for {symbol}")
        return []

    return resp.json()


# =============================
# CORE SYNC
# =============================

def sync_symbol_funding(symbol):

    print(f"\nProcessing funding for {symbol}")

    current_ms = now_utc_ms()
    latest_db = get_latest_funding_time(symbol)

    # Determine start point
    if not latest_db:
        print("First sync → 30-day funding backfill")
        start_ts = current_ms - (BACKFILL_DAYS * 24 * 60 * 60 * 1000)
    else:
        print("Incremental sync from last stored funding time")
        start_ts = latest_db + 1

    end_ts = current_ms

    while True:

        funding_data = fetch_funding(symbol, start_ts, end_ts)

        if not funding_data:
            print("No more funding data.")
            break

        rows = []

        for item in funding_data:

            ts = int(item["fundingTime"])

            funding_time_utc = datetime.fromtimestamp(
                ts / 1000,
                tz=timezone.utc
            )

            rows.append({
                "symbol": item["symbol"],
                "funding_time": ts,
                "funding_time_utc": funding_time_utc,
                "funding_rate": float(item["fundingRate"]),
                "mark_price": float(item["markPrice"])
            })

        insert_funding_batch(rows)

        print(f"Inserted/Updated {len(rows)} funding rows")

        # If returned less than LIMIT, no more pages
        if len(funding_data) < LIMIT:
            break

        # Otherwise continue from last timestamp
        start_ts = int(funding_data[-1]["fundingTime"]) + 1


# =============================
# ENTRY
# =============================

if __name__ == "__main__":

    symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")

    print("Starting funding sync...\n")

    for symbol in symbols:
        sync_symbol_funding(symbol)

    print("\nAll funding symbols processed.")