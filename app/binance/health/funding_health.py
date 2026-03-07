import time
import json
import requests

from datetime import datetime, timezone
from sqlalchemy import text

from app.db import SessionLocal
from app.redis_client import redis_client


# =====================================================
# CONFIG
# =====================================================

FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000
LIMIT = 1000
REDIS_KEY = "liquid_coins"


# =====================================================
# DATABASE HELPERS
# =====================================================

def get_symbol_range(symbol):

    db = SessionLocal()

    try:

        row = db.execute(text("""
            SELECT
                MIN(funding_time),
                MAX(funding_time),
                COUNT(*)
            FROM funding_rate_8h
            WHERE symbol=:symbol
        """), {"symbol": symbol}).fetchone()

        return row

    finally:
        db.close()


def get_all_times(symbol):

    db = SessionLocal()

    try:

        rows = db.execute(text("""
            SELECT funding_time
            FROM funding_rate_8h
            WHERE symbol=:symbol
            ORDER BY funding_time
        """), {"symbol": symbol}).fetchall()

        return [int(r[0]) for r in rows]

    finally:
        db.close()


def insert_funding_batch(rows):

    if not rows:
        return

    db = SessionLocal()

    try:

        db.execute(text("""
            INSERT INTO funding_rate_8h
            (symbol, funding_time, funding_time_utc,
             funding_rate, mark_price)
            VALUES (:symbol, :funding_time, :funding_time_utc,
                    :funding_rate, :mark_price)
            ON CONFLICT (symbol, funding_time)
            DO UPDATE SET
                funding_rate = EXCLUDED.funding_rate,
                mark_price = EXCLUDED.mark_price
        """), rows)

        db.commit()

    finally:
        db.close()


# =====================================================
# API
# =====================================================

def fetch_funding(symbol, start_ts, end_ts):

    params = {
        "symbol": symbol,
        "startTime": start_ts,
        "endTime": end_ts,
        "limit": LIMIT
    }

    r = requests.get(FUNDING_URL, params=params, timeout=10)

    if r.status_code != 200:
        print("API error", symbol)
        return []

    return r.json()


# =====================================================
# GAP DETECTION
# =====================================================

def find_missing_times(times):

    missing = []

    for i in range(len(times) - 1):

        cur = times[i]
        nxt = times[i + 1]

        diff = nxt - cur

        if diff > FUNDING_INTERVAL_MS:

            t = cur + FUNDING_INTERVAL_MS

            while t < nxt:

                missing.append(t)
                t += FUNDING_INTERVAL_MS

    return missing


# =====================================================
# GROUP MISSING INTO RANGES
# =====================================================

def group_ranges(missing):

    if not missing:
        return []

    ranges = []
    start = missing[0]
    prev = missing[0]

    for ts in missing[1:]:

        if ts - prev == FUNDING_INTERVAL_MS:
            prev = ts
        else:
            ranges.append((start, prev))
            start = ts
            prev = ts

    ranges.append((start, prev))

    return ranges


# =====================================================
# BACKFILL RANGE
# =====================================================

def backfill_range(symbol, start_ts, end_ts):

    print(f"Backfilling {symbol} {start_ts} → {end_ts}")

    while start_ts <= end_ts:

        data = fetch_funding(symbol, start_ts, end_ts)

        if not data:
            break

        rows = []

        for r in data:

            ts = int(r["fundingTime"])

            rows.append({
                "symbol": r["symbol"],
                "funding_time": ts,
                "funding_time_utc": datetime.fromtimestamp(
                    ts/1000, tz=timezone.utc
                ),
                "funding_rate": float(r["fundingRate"]),
                "mark_price": float(r["markPrice"])
            })

        insert_funding_batch(rows)

        last_ts = int(data[-1]["fundingTime"])

        start_ts = last_ts + FUNDING_INTERVAL_MS

        if len(data) < LIMIT:
            break


# =====================================================
# SYMBOL HEALTH CHECK
# =====================================================

def check_symbol(symbol):

    min_ts, max_ts, count = get_symbol_range(symbol)

    if not min_ts:
        print("No data for", symbol)
        return

    expected = ((max_ts - min_ts) // FUNDING_INTERVAL_MS) + 1

    if expected == count:

        print(f"{symbol} OK ({count})")
        return

    print(f"{symbol} GAP detected expected={expected} actual={count}")

    times = get_all_times(symbol)

    missing = find_missing_times(times)

    if not missing:
        return

    ranges = group_ranges(missing)

    for start_ts, end_ts in ranges:
        backfill_range(symbol, start_ts, end_ts)


# =====================================================
# SYMBOL SOURCE
# =====================================================

def get_symbols():

    redis_symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")

    db = SessionLocal()

    try:

        rows = db.execute(text(
            "SELECT DISTINCT symbol FROM funding_rate_8h"
        )).fetchall()

        db_symbols = [r[0] for r in rows]

    finally:
        db.close()

    return sorted(set(redis_symbols) | set(db_symbols))


# =====================================================
# ENTRY
# =====================================================

if __name__ == "__main__":

    while True:

        print("\nFunding health cycle start\n")

        symbols = get_symbols()

        for symbol in symbols:

            try:
                check_symbol(symbol)
            except Exception as e:
                print("Error:", symbol, e)

        print("\nCycle complete. Sleeping 1 hour\n")

        time.sleep(3600)