import requests
import time
import json
import os
from datetime import datetime, timezone, timedelta

from sqlalchemy import text
from app.db import SessionLocal

from app.redis_client import redis_client
from app.binance.scripts.insert import insert_candles_batch
from app.binance.payload_builder import build_payloads


URL = "https://fapi.binance.com/fapi/v1/klines"
LIMIT = 500
REDIS_KEY = "liquid_coins"


# --------------------------------------------------
# Logger (Console + JSONL File Per Run)
# --------------------------------------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"backfill_{RUN_ID}.jsonl")

print(f"[LOGGER] Writing logs → {LOG_FILE}")


def log(msg, **extra):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")

    record = {
        "time": now,
        "msg": msg,
        **extra,
    }

    print(f"[{now}] {msg}")

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
        f.flush()


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def datetime_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# --------------------------------------------------
# DB: Read symbols from 1m candles
# --------------------------------------------------
def get_symbols_from_db():
    db = SessionLocal()
    try:
        q = text("""
            SELECT DISTINCT symbol
            FROM candles_1m
        """)
        rows = db.execute(q).fetchall()
        symbols = [r[0] for r in rows]

        log("[DB] Loaded symbols from DB", count=len(symbols))
        return symbols
    finally:
        db.close()


# --------------------------------------------------
# Redis + DB merged symbols
# --------------------------------------------------
def get_symbols():

    redis_symbols = []
    redis_data = redis_client.get(REDIS_KEY)

    if redis_data:
        redis_symbols = json.loads(redis_data)
        log("[REDIS] Loaded symbols", count=len(redis_symbols))

    db_symbols = get_symbols_from_db()

    merged = list(set(redis_symbols) | set(db_symbols))

    log("[SYMBOLS] Final merged symbols", count=len(merged))

    return merged


# --------------------------------------------------
# Fetch klines
# --------------------------------------------------
def fetch_klines(symbol, tf, start_time=None, end_time=None):

    params = {
        "symbol": symbol,
        "interval": tf,
        "limit": LIMIT,
    }

    if start_time is not None:
        params["startTime"] = start_time

    if end_time is not None:
        params["endTime"] = end_time

    log(
        "[API] Request",
        symbol=symbol,
        tf=tf,
        start=start_time,
        end=end_time,
    )

    resp = requests.get(URL, params=params, timeout=10)
    resp.raise_for_status()

    data = resp.json()

    log("[API] Response", symbol=symbol, candles=len(data))

    if data:
        oldest = data[0][0]
        newest = data[-1][0]
        log(
            "[API] Time Range",
            symbol=symbol,
            oldest=ms_to_utc(oldest),
            newest=ms_to_utc(newest),
        )

    return data


# --------------------------------------------------
# ROUND ROBIN BACKFILL / SYNC ENGINE
# --------------------------------------------------
def backfill_all_symbols(tf, start_date=None, end_date=None):

    now_utc = datetime.now(timezone.utc)

    # Resolve Date Range
    if start_date is None and end_date is None:
        end_date = now_utc
        start_date = now_utc - timedelta(days=365)

    elif start_date is not None and end_date is None:
        end_date = now_utc

    elif start_date is None and end_date is not None:
        start_date = end_date - timedelta(days=365)

    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    start_ts = datetime_to_ms(start_date)
    end_ts = datetime_to_ms(end_date)

    symbols = get_symbols()
    if not symbols:
        log("[EXIT] No symbols found")
        return

    log("[START]", tf=tf, limit=LIMIT)
    log("[START] Date Range", start=str(start_date), end=str(end_date))

    # --------------------------------------------------
    # STATE with FETCH COUNTER
    # --------------------------------------------------
    state = {
        symbol: {
            "cursor_end": end_ts,
            "done": False,
            "fetched": 0,   # 👈 total candles fetched from API
        }
        for symbol in symbols
    }

    loop = 0

    while True:

        loop += 1
        log("LOOP START", loop=loop)

        active = sum(1 for v in state.values() if not v["done"])
        log(
            "[LOOP STATUS]",
            active_symbols=active,
            completed=len(symbols) - active,
        )

        for symbol in symbols:

            info = state[symbol]

            if info["done"]:
                log("[SKIP] completed", symbol=symbol)
                continue

            klines = fetch_klines(
                symbol,
                tf,
                start_time=start_ts,
                end_time=info["cursor_end"],
            )

            if not klines:
                log("[END] No more history", symbol=symbol)
                info["done"] = True
                continue

            # 👇 COUNT FETCHED RECORDS
            info["fetched"] += len(klines)

            payloads = build_payloads(symbol, tf, klines)

            log(
                "[DB] UPSERT START",
                symbol=symbol,
                rows=len(payloads),
                tf=tf,
            )

            insert_candles_batch(tf, payloads)

            oldest_open_time = klines[0][0]
            newest_open_time = klines[-1][0]

            log(
                "[DB] UPSERT DONE",
                symbol=symbol,
                oldest=ms_to_utc(oldest_open_time),
                newest=ms_to_utc(newest_open_time),
            )

            # move cursor backward
            info["cursor_end"] = oldest_open_time - 1

            # stop condition
            if oldest_open_time <= start_ts:
                log("[STOP] reached start boundary", symbol=symbol)
                info["done"] = True

            time.sleep(0.12)

        # --------------------------------------------------
        # FINAL SUMMARY
        # --------------------------------------------------
        if all(v["done"] for v in state.values()):
            log("[FINISH] All symbols completed")

            log("======== FINAL FETCH SUMMARY ========")
            for sym, info in state.items():
                log(
                    "[SUMMARY]",
                    symbol=sym,
                    total_fetched=info["fetched"],
                )

            break


# --------------------------------------------------
# Entry
# --------------------------------------------------
if __name__ == "__main__":
    backfill_all_symbols("1h")