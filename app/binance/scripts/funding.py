import requests
import json
from datetime import datetime, timezone
from sqlalchemy import text
from app.db import SessionLocal
from app.redis_client import redis_client


# =====================================================
# CONFIG
# =====================================================

FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000
BACKFILL_DAYS = 30
REDIS_KEY = "liquid_coins"
LIMIT = 1000


# =====================================================
# TIME HELPERS
# =====================================================

def get_latest_closed_funding_ms():
    """
    Returns last closed 8H funding timestamp in UTC ms.
    """
    now_utc = datetime.now(timezone.utc)
    hour_block = (now_utc.hour // 8) * 8
    closed_time = now_utc.replace(
        hour=hour_block,
        minute=0,
        second=0,
        microsecond=0
    )
    return int(closed_time.timestamp() * 1000)


# =====================================================
# DATABASE HELPERS
# =====================================================

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


def get_symbols_from_db():
    db = SessionLocal()
    try:
        q = text("SELECT DISTINCT symbol FROM funding_rate_8h")
        rows = db.execute(q).fetchall()
        return [row[0] for row in rows]
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


# =====================================================
# API FETCH
# =====================================================

def fetch_funding(symbol, start_time, end_time):

    params = {
        "symbol": symbol,
        "startTime": start_time,
        "endTime": end_time,
        "limit": LIMIT
    }

    resp = requests.get(FUNDING_URL, params=params, timeout=10)

    if resp.status_code != 200:
        print(f"Funding fetch failed for {symbol}")
        return []

    return resp.json()


# =====================================================
# CORE SYNC PER SYMBOL (PAGINATED)
# =====================================================

def sync_symbol_funding(symbol):

    print(f"\nProcessing funding for {symbol}")

    latest_closed = get_latest_closed_funding_ms()
    latest_db = get_latest_funding_time(symbol)

    if not latest_db:
        print("First sync → 30-day funding backfill")
        start_ts = latest_closed - (
            BACKFILL_DAYS * 24 * 60 * 60 * 1000
        )
    else:
        if latest_db >= latest_closed:
            print("Funding already up to date.")
            return
        start_ts = latest_db + FUNDING_INTERVAL_MS

    end_ts = latest_closed
    total_inserted = 0

    while start_ts <= end_ts:

        data = fetch_funding(symbol, start_ts, end_ts)

        if not data:
            break

        rows = []

        for item in data:

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
                "mark_price": float(item["markPrice"]),
            })

        insert_funding_batch(rows)
        total_inserted += len(rows)

        last_ts = int(data[-1]["fundingTime"])

        # Forward-only safety
        if last_ts < start_ts:
            print("Cursor stalled — stopping.")
            break

        start_ts = last_ts + FUNDING_INTERVAL_MS

        if len(data) < LIMIT:
            break

    print(f"Inserted/Updated {total_inserted} funding rows")


# =====================================================
# ENTRY
# =====================================================

if __name__ == "__main__":

    print("Starting funding sync...\n")

    redis_symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")
    db_symbols = get_symbols_from_db()

    symbols = sorted(set(redis_symbols) | set(db_symbols))

    if not symbols:
        print("No symbols found.")
    else:
        for symbol in symbols:
            sync_symbol_funding(symbol)

    print("\nAll funding symbols processed.")