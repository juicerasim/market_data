import requests
import time
import json
from datetime import datetime, timezone

from app.redis_client import redis_client
from app.binance.scripts.insert import insert_candles_batch
from app.binance.payload_builder import build_payloads  

URL = "https://fapi.binance.com/fapi/v1/klines"
LIMIT = 500
REDIS_KEY = "liquid_coins"

# poetry run python -m app.binance.scripts.kline_history


# --------------------------------------------------
# Simple logger helper (UTC aware)
# --------------------------------------------------
def log(msg):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


# --------------------------------------------------
# Convert ms timestamp → UTC readable
# --------------------------------------------------
def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# --------------------------------------------------
# Convert raw kline → readable dict list
# --------------------------------------------------
def normalize_klines(data):
    result = []

    for k in data:
        result.append({
            "open_time_utc": ms_to_utc(k[0]),
            "close_time_utc": ms_to_utc(k[6]),
            "open": k[1],
            "high": k[2],
            "low": k[3],
            "close": k[4],
            "volume": k[5],
            "trades": k[8],
        })

    return result


# --------------------------------------------------
# Fetch klines
# --------------------------------------------------
def fetch_klines(symbol, tf, end_time=None):
    params = {
        "symbol": symbol,
        "interval": tf,
        "limit": LIMIT,
    }

    if end_time:
        params["endTime"] = end_time

    log(f"[API] Request → {symbol} tf={tf} end_time={end_time}")

    resp = requests.get(URL, params=params, timeout=10)
    resp.raise_for_status()

    data = resp.json()

    log(f"[API] Response ← {symbol} candles={len(data)}")

    # ⭐ show UTC time range
    if data:
        oldest = data[0][0]
        newest = data[-1][0]
        log(
            f"[API] Time Range UTC → {ms_to_utc(oldest)}  →  {ms_to_utc(newest)}"
        )

        # ⭐ extra visibility
        span = newest - oldest
        log(f"[API] Window span(ms)={span}")

    # ⭐ convert into readable format
    # modified = normalize_klines(data)

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
def backfill_all_symbols(tf, total_count):

    symbols = get_symbols()
    if not symbols:
        return

    log(f"[START] TF={tf} LIMIT={LIMIT} target_per_symbol={total_count}")

    # ⭐ NEW LOGGING (no logic change)
    log("--------------------------------------------------")
    log(f"[CONFIG] Symbols count = {len(symbols)}")
    log(f"[CONFIG] Total API calls per symbol ≈ {(total_count // LIMIT) + 1}")
    log(f"[CONFIG] Estimated candles overall ≈ {len(symbols) * total_count}")
    log("--------------------------------------------------")

    # state per symbol
    state = {
        symbol: {
            "end_time": None,
            "processed": 0,
            "done": False,
        }
        for symbol in symbols
    }

    max_loops = (total_count // LIMIT) + 1

    for loop in range(max_loops):

        log(f"\n================ LOOP {loop+1}/{max_loops} ================")

        # ⭐ NEW LOOP STATUS LOG
        active = sum(1 for v in state.values() if not v["done"])
        log(f"[LOOP STATUS] active_symbols={active} completed={len(symbols)-active}")

        for symbol in symbols:

            info = state[symbol]

            if info["done"]:
                log(f"[SKIP] {symbol} already completed")
                continue

            if info["processed"] >= total_count:
                log(f"[DONE] Target reached for {symbol}")
                info["done"] = True
                continue

            remaining = total_count - info["processed"]

            log(
                f"[FETCH] {symbol} "
                f"processed={info['processed']}/{total_count} "
                f"remaining≈{remaining} "
                f"cursor={info['end_time']}"
            )

            klines = fetch_klines(symbol, tf, info["end_time"])

            if not klines:
                log(f"[END] No more history for {symbol}")
                info["done"] = True
                continue

            payloads = build_payloads(symbol, tf, klines)

            log(f"[DB] UPSERT START {symbol} rows={len(payloads)}")

            insert_candles_batch(tf, payloads)

            oldest_open_time = klines[0][0]
            newest_open_time = klines[-1][0]

            info["end_time"] = oldest_open_time - 1
            info["processed"] += len(klines)

            log(
                f"[DB] UPSERT DONE {symbol} "
                f"UTC_RANGE={ms_to_utc(oldest_open_time)} → {ms_to_utc(newest_open_time)} "
                f"total_processed={info['processed']}"
            )

            # ⭐ PROGRESS LOG
            percent = (info["processed"] / total_count) * 100
            log(f"[PROGRESS] {symbol} {percent:.1f}% complete")

            if len(klines) < LIMIT:
                log(f"[COMPLETE] Reached oldest history for {symbol}")
                info["done"] = True

            time.sleep(0.12)

        if all(v["done"] for v in state.values()):
            log("[FINISH] All symbols completed")

            # ⭐ FINAL SUMMARY
            log("--------------------------------------------------")
            log("[SUMMARY]")
            for s, v in state.items():
                log(f"{s} processed={v['processed']} done={v['done']}")
            log("--------------------------------------------------")

            break


# --------------------------------------------------
# Entry
# --------------------------------------------------
if __name__ == "__main__":
    backfill_all_symbols("1d", 365)
