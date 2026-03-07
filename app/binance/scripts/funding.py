import os
import json
import time
import requests

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
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
API_DELAY = 0.15

LOG_DIR = "logs/health"
os.makedirs(LOG_DIR, exist_ok=True)

IST = ZoneInfo("Asia/Kolkata")


# =====================================================
# LOGGING
# =====================================================

log_file = None


def start_cycle_log():

    global log_file

    ts = datetime.now(timezone.utc).astimezone(IST)

    filename = ts.strftime("funding_health %Y-%m-%d %H-%M-%S.jsonl")

    path = os.path.join(LOG_DIR, filename)

    log_file = open(path, "a")

    print(f"Logging → {path}")


def close_cycle_log():

    global log_file

    if log_file:
        log_file.close()
        log_file = None


def log(event, symbol=None, payload=None, response=None, **extra):

    ts = datetime.now(timezone.utc).astimezone(IST)

    record = {
        "component": "FUNDING",
        "event": event,
        "time_local": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "payload": payload,
        "response": response,
        **extra
    }

    print(record)

    if log_file:
        log_file.write(json.dumps(record) + "\n")
        log_file.flush()


# =====================================================
# TIME HELPERS
# =====================================================

def get_latest_closed_funding_ms():
    """
    Funding happens every 8 hours:
    00:00 / 08:00 / 16:00 UTC
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


def normalize_funding_ts(ts):
    """
    Binance sometimes returns funding timestamps with
    small millisecond offsets.
    Normalize to exact 8h boundary.
    """

    return (ts // FUNDING_INTERVAL_MS) * FUNDING_INTERVAL_MS


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

        if not row or not row[0]:
            return None

        raw_ts = int(row[0])

        # normalize DB timestamp
        return (raw_ts // FUNDING_INTERVAL_MS) * FUNDING_INTERVAL_MS

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


# =====================================================
# INSERT
# =====================================================

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

        log("db_insert", response={"rows": len(rows)})

    finally:
        db.close()


# =====================================================
# API
# =====================================================

def fetch_funding(symbol, start_time, end_time):

    params = {
        "symbol": symbol,
        "startTime": start_time,
        "endTime": end_time,
        "limit": LIMIT
    }

    log("api_request", symbol=symbol, payload=params)

    resp = requests.get(FUNDING_URL, params=params, timeout=10)

    if resp.status_code != 200:

        log(
            "api_failed",
            symbol=symbol,
            response={"status": resp.status_code}
        )

        return []

    data = resp.json()

    log("api_response", symbol=symbol, response={"rows": len(data)})

    return data


# =====================================================
# SYNC
# =====================================================

def sync_symbol_funding(symbol):

    latest_closed = get_latest_closed_funding_ms()

    latest_db = get_latest_funding_time(symbol)

    if not latest_db:

        log("bootstrap_backfill", symbol=symbol)

        start_ts = latest_closed - (
            BACKFILL_DAYS * 24 * 60 * 60 * 1000
        )

    else:

        if latest_db >= latest_closed:

            log("symbol_up_to_date", symbol=symbol)

            return

        # overlap-safe start
        start_ts = latest_db + FUNDING_INTERVAL_MS

    end_ts = latest_closed

    total_inserted = 0

    while start_ts <= end_ts:

        data = fetch_funding(symbol, start_ts, end_ts)

        if not data:

            log("api_empty", symbol=symbol)

            break

        rows = []

        for item in data:

            raw_ts = int(item["fundingTime"])

            ts = normalize_funding_ts(raw_ts)

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

        last_ts = normalize_funding_ts(int(data[-1]["fundingTime"]))

        if last_ts < start_ts:

            log("cursor_stalled", symbol=symbol)

            break

        start_ts = last_ts + FUNDING_INTERVAL_MS

        if len(data) < LIMIT:
            break

        time.sleep(API_DELAY)

    log(
        "symbol_complete",
        symbol=symbol,
        response={"rows_inserted": total_inserted}
    )


# =====================================================
# ENTRY
# =====================================================

if __name__ == "__main__":

    try:

        while True:

            start_cycle_log()

            log("cycle_start")

            redis_symbols = json.loads(
                redis_client.get(REDIS_KEY) or "[]"
            )

            db_symbols = get_symbols_from_db()

            symbols = sorted(set(redis_symbols) | set(db_symbols))

            log("symbols_loaded", response={"count": len(symbols)})

            if not symbols:

                log("no_symbols")

            else:

                for symbol in symbols:

                    try:
                        sync_symbol_funding(symbol)
                    except Exception as e:
                        log(
                            "symbol_error",
                            symbol=symbol,
                            response={"error": str(e)}
                        )

            log("cycle_complete")

            close_cycle_log()

            log("sleeping", response={"seconds": 3600})

            time.sleep(3600)

    except Exception as e:

        log("worker_crashed", response={"error": str(e)})

        close_cycle_log()

        raise