import requests
import json
from datetime import datetime, timezone
from sqlalchemy import text
from app.db import SessionLocal
from app.redis_client import redis_client


# =====================================================
# CONFIG
# =====================================================

OI_URL = "https://fapi.binance.com/futures/data/openInterestHist"

INTERVAL = "1h"
INTERVAL_MS = 60 * 60 * 1000
BACKFILL_DAYS = 30
REDIS_KEY = "liquid_coins"
LIMIT = 1000


# =====================================================
# TIME HELPERS
# =====================================================

def get_latest_closed_hour_ms():
    now_utc = datetime.now(timezone.utc)
    closed_hour = now_utc.replace(minute=0, second=0, microsecond=0)
    return int(closed_hour.timestamp() * 1000)


# =====================================================
# DATABASE HELPERS
# =====================================================

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


def get_symbols_from_db():
    db = SessionLocal()
    try:
        q = text("SELECT DISTINCT symbol FROM open_interest_1h")
        rows = db.execute(q).fetchall()
        return [row[0] for row in rows]
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


# =====================================================
# API FETCH
# =====================================================

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


# =====================================================
# CORE SYNC PER SYMBOL (PAGINATED)
# =====================================================

def sync_symbol_oi(symbol):

    print(f"\nProcessing OI for {symbol}")

    latest_closed = get_latest_closed_hour_ms()
    latest_db = get_latest_oi_time(symbol)

    # Determine start time
    if not latest_db:
        print("First sync → 30-day backfill")
        start_ts = latest_closed - (BACKFILL_DAYS * 24 * INTERVAL_MS)
    else:
        if latest_db >= latest_closed:
            print("OI already up to date.")
            return
        start_ts = latest_db + INTERVAL_MS

    end_ts = latest_closed

    total_inserted = 0

    while start_ts <= end_ts:

        oi_data = fetch_oi(symbol, start_ts, end_ts)

        if not oi_data:
            break

        rows = []

        for item in oi_data:

            ts = int(item["timestamp"])
            ts = ts - (ts % INTERVAL_MS)  # normalize to 1h boundary

            open_time_utc = datetime.fromtimestamp(
                ts / 1000,
                tz=timezone.utc
            )

            rows.append({
                "symbol": item["symbol"],
                "open_time": ts,
                "open_time_utc": open_time_utc,
                "open_interest": float(item["sumOpenInterest"]),
                "oi_notional": float(item["sumOpenInterestValue"]),
            })

        insert_oi_batch(rows)
        total_inserted += len(rows)

        last_ts = int(oi_data[-1]["timestamp"])
        last_ts = last_ts - (last_ts % INTERVAL_MS)

        # Forward-only safety
        if last_ts < start_ts:
            print("Cursor stalled — stopping to prevent infinite loop.")
            break

        start_ts = last_ts + INTERVAL_MS

        # If less than LIMIT → no more pages
        if len(oi_data) < LIMIT:
            break

    print(f"Inserted/Updated {total_inserted} OI rows")


# =====================================================
# ENTRY
# =====================================================

if __name__ == "__main__":

    print("Starting standalone OI sync...\n")

    # Redis symbols
    redis_symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")

    # DB symbols
    db_symbols = get_symbols_from_db()

    # UNION(redis + db)
    symbols = sorted(set(redis_symbols) | set(db_symbols))

    if not symbols:
        print("No symbols found.")
    else:
        for symbol in symbols:
            sync_symbol_oi(symbol)

    print("\nAll OI symbols processed.")