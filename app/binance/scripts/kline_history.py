import requests
import time
import json
from datetime import datetime, timezone, timedelta

from sqlalchemy import text
from app.db import SessionLocal
from app.redis_client import redis_client
from app.binance.scripts.insert import insert_candles_batch
from app.binance.payload_builder import build_payloads


# =============================
# CONFIG
# =============================

KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
OI_URL = "https://fapi.binance.com/futures/data/openInterestHist"
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

LIMIT = 500
REDIS_KEY = "liquid_coins"

BACKFILL_DAYS = 30   # Sync with Binance OI retention


# =============================
# HELPERS
# =============================

def datetime_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def get_previous_oi(symbol, interval, current_open_time):
    db = SessionLocal()
    try:
        q = text("""
            SELECT open_interest
            FROM candles_1h
            WHERE symbol = :symbol
              AND interval = :interval
              AND open_time < :open_time
              AND open_interest IS NOT NULL
            ORDER BY open_time DESC
            LIMIT 1
        """)
        row = db.execute(q, {
            "symbol": symbol,
            "interval": interval,
            "open_time": current_open_time
        }).fetchone()

        return float(row[0]) if row else None
    finally:
        db.close()


# =============================
# API CALLS
# =============================

def fetch_klines(symbol, tf, end_time=None):
    params = {
        "symbol": symbol,
        "interval": tf,
        "limit": LIMIT,
    }

    if end_time:
        params["endTime"] = end_time

    resp = requests.get(KLINE_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_recent_oi(symbol, tf, limit=500):
    params = {
        "symbol": symbol,
        "period": tf,
        "limit": limit
    }

    resp = requests.get(OI_URL, params=params, timeout=10)

    if resp.status_code != 200:
        print("OI ERROR:", resp.text)
        return []

    return resp.json()


def fetch_recent_funding(symbol, limit=1000):
    params = {
        "symbol": symbol,
        "limit": limit
    }

    resp = requests.get(FUNDING_URL, params=params, timeout=10)

    if resp.status_code != 200:
        print("FUNDING ERROR:", resp.text)
        return []

    return resp.json()


# =============================
# ENRICHMENT
# =============================

def enrich_with_futures_data(
    klines,
    oi_map,
    funding_sorted,
    initial_prev_oi=None
):

    funding_index = 0
    prev_oi = initial_prev_oi
    enriched_map = {}

    # ensure chronological order
    for k in sorted(klines, key=lambda x: x[0]):

        open_time = k[0]
        close_time = k[6]

        current_oi = oi_map.get(open_time)

        # funding regime mapping
        while (
            funding_index + 1 < len(funding_sorted)
            and funding_sorted[funding_index + 1]["fundingTime"] <= close_time
        ):
            funding_index += 1

        funding_rate = (
            float(funding_sorted[funding_index]["fundingRate"])
            if funding_sorted else None
        )

        oi_delta = None
        if current_oi is not None and prev_oi not in (None, 0):
            oi_delta = (current_oi - prev_oi) / prev_oi

        prev_oi = current_oi

        enriched_map[open_time] = {
            "open_interest": current_oi,
            "oi_delta_percent": oi_delta,
            "funding_rate": funding_rate
        }

    return enriched_map


# =============================
# BACKFILL
# =============================

def backfill_symbol(symbol, tf, start_ts, end_ts):

    print("BACKFILL START:", symbol)

    # Fetch recent OI & Funding once per symbol
    oi_data = fetch_recent_oi(symbol, tf, limit=500)
    funding_data = fetch_recent_funding(symbol, limit=1000)

    oi_map = {
        int(item["timestamp"]): float(item["sumOpenInterest"])
        for item in oi_data
    }

    funding_sorted = sorted(
        funding_data,
        key=lambda x: x["fundingTime"]
    )

    cursor_end = end_ts

    while True:

        klines = fetch_klines(symbol, tf, end_time=cursor_end)

        if not klines:
            break

        filtered = [k for k in klines if k[0] >= start_ts]

        if not filtered:
            break

        oldest_open_time = klines[0][0]

        first_open_time = min(k[0] for k in filtered)

        previous_oi = get_previous_oi(
            symbol,
            tf,
            first_open_time
        )

        enriched_map = enrich_with_futures_data(
            filtered,
            oi_map,
            funding_sorted,
            initial_prev_oi=previous_oi
        )

        payloads = build_payloads(
            symbol,
            tf,
            filtered,
            futures_data_map=enriched_map
        )

        insert_candles_batch(tf, payloads)

        cursor_end = oldest_open_time - 1

        if oldest_open_time <= start_ts:
            break

        time.sleep(0.25)

    print("BACKFILL DONE:", symbol)


# =============================
# ENTRY
# =============================

if __name__ == "__main__":

    now_utc = datetime.now(timezone.utc)
    start_date = now_utc - timedelta(days=BACKFILL_DAYS)

    start_ts = datetime_to_ms(start_date)
    end_ts = datetime_to_ms(now_utc)

    symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")

    print(f"Backfilling last {BACKFILL_DAYS} days (OI-synced window)")

    for symbol in symbols:
        backfill_symbol(symbol, "1h", start_ts, end_ts)