import requests
import time
import json
from datetime import datetime, timezone, timedelta

from app.redis_client import redis_client
from app.binance.scripts.insert import insert_candles_batch
from app.binance.payload_builder import build_payloads

URL = "https://fapi.binance.com/fapi/v1/klines"
LIMIT = 500
REDIS_KEY = "liquid_coins"


# --------------------------------------------------
# Logger
# --------------------------------------------------
def log(msg):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


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
# Fetch klines
# --------------------------------------------------
def fetch_klines(symbol, tf, start_time=None, end_time=None):

    params = {
        "symbol": symbol,
        "interval": tf,
        "limit": LIMIT,
    }

    if start_time:
        params["startTime"] = start_time

    if end_time:
        params["endTime"] = end_time

    log(f"[API] Request → {symbol} tf={tf} start={start_time} end={end_time}")

    resp = requests.get(URL, params=params, timeout=10)
    resp.raise_for_status()

    data = resp.json()

    log(f"[API] Response ← {symbol} candles={len(data)}")

    if data:
        oldest = data[0][0]
        newest = data[-1][0]
        log(f"[API] Time Range UTC → {ms_to_utc(oldest)} → {ms_to_utc(newest)}")

    return data


# --------------------------------------------------
# Read symbols
# --------------------------------------------------
def get_symbols():
    data = redis_client.get(REDIS_KEY)

    log("============================================")
    log(f"[BACKFILL] Redis raw data length={len(data) if data else 0}")

    if not data:
        log("[BACKFILL] Redis has no symbols yet")
        return []

    coins = json.loads(data)
    log(f"[BACKFILL] Loaded {len(coins)} symbols from Redis")

    return coins


# --------------------------------------------------
# ROUND ROBIN BACKFILL ENGINE
# --------------------------------------------------
def backfill_all_symbols(tf, start_date=None, end_date=None):

    now_utc = datetime.now(timezone.utc)

    # --------------------------------------------------
    # Resolve Date Range
    # --------------------------------------------------
    if start_date is None and end_date is None:
        end_date = now_utc
        start_date = now_utc - timedelta(days=365)

    elif start_date is not None and end_date is None:
        end_date = now_utc

    elif start_date is None and end_date is not None:
        start_date = end_date - timedelta(days=365)

    # Ensure UTC
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    # Safety swap if reversed
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    start_ts = datetime_to_ms(start_date)
    end_ts   = datetime_to_ms(end_date)

    symbols = get_symbols()
    if not symbols:
        return

    log(f"[START] TF={tf} LIMIT={LIMIT}")
    log(f"[START] Date Range UTC → {start_date} → {end_date}")

    state = {
        symbol: {
            "cursor_end": end_ts,
            "done": False,
        }
        for symbol in symbols
    }

    loop = 0

    while True:

        loop += 1
        log(f"\n================ LOOP {loop} ================")

        active = sum(1 for v in state.values() if not v["done"])
        log(f"[LOOP STATUS] active_symbols={active} completed={len(symbols)-active}")

        for symbol in symbols:

            info = state[symbol]

            if info["done"]:
                log(f"[SKIP] {symbol} already completed")
                continue

            klines = fetch_klines(
                symbol,
                tf,
                start_time=start_ts,
                end_time=info["cursor_end"],
            )

            if not klines:
                log(f"[END] No more history for {symbol}")
                info["done"] = True
                continue

            payloads = build_payloads(symbol, tf, klines)

            log(f"[DB] UPSERT START {symbol} rows={len(payloads)}")
            insert_candles_batch(tf, payloads)

            oldest_open_time = klines[0][0]
            newest_open_time = klines[-1][0]

            log(
                f"[DB] UPSERT DONE {symbol} "
                f"{ms_to_utc(oldest_open_time)} → {ms_to_utc(newest_open_time)}"
            )

            # move cursor backward
            info["cursor_end"] = oldest_open_time - 1

            # stop condition
            if oldest_open_time <= start_ts:
                log(f"[STOP] {symbol} reached start boundary")
                info["done"] = True

            time.sleep(0.12)

        if all(v["done"] for v in state.values()):
            log("[FINISH] All symbols completed")
            break


# --------------------------------------------------
# Entry
# --------------------------------------------------
if __name__ == "__main__":

    # ✅ Default → last 1 year
    # backfill_all_symbols("1d")
    backfill_all_symbols("4h")

    # ✅ Custom range example:
    # from datetime import datetime
    # backfill_all_symbols(
    #     "1d",
    #     start_date=datetime(2024,10,1),
    #     end_date=datetime(2025,10,1),
    # )
