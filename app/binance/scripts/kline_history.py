import requests
import time
import json
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func
from app.config import TIMEFRAMES
from app.redis_client import redis_client
from app.binance.scripts.insert import insert_candles_batch, MODEL_MAP
from app.binance.payload_builder import build_payloads
from app.db import SessionLocal


# ======================================================
# CONFIG
# ======================================================

URL = "https://fapi.binance.com/fapi/v1/klines"
LIMIT = 500
REDIS_KEY = "liquid_coins"
TEST_SYMBOLS = ["BTCUSDT"]
TEST_MODE = False
IST = ZoneInfo("Asia/Kolkata")

MAX_GLOBAL_LOOPS = 100000

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"backfill_{RUN_ID}.jsonl")

print(f"[LOGGER] Writing logs → {LOG_FILE}")


# ======================================================
# LOGGER
# ======================================================

def log(msg, **extra):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    record = {"time": now, "msg": msg, **extra}

    print(f"[{now}] {msg}")

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
        f.flush()


# ======================================================
# TIME HELPERS
# ======================================================

def datetime_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_ist(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(IST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def get_tf_ms(tf):

    if tf in TIMEFRAMES:
        return int(TIMEFRAMES[tf]["tf_ms"])

    fallback = {
        # "1m": 60_000,
        "5m": 300_000,
        "15m": 900_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
    }

    return fallback.get(tf)


def align_timestamp(ts, tf_ms):
    """
    Align timestamp to timeframe boundary
    """
    return ts - (ts % tf_ms)


# ======================================================
# CANDLE CLOSE WAITER
# ======================================================

def wait_until_next_candle_close(tf: str, buffer_seconds: int = 3):

    tf_ms = get_tf_ms(tf)

    if not tf_ms:
        raise ValueError(f"Unsupported timeframe: {tf}")

    now = datetime.now(timezone.utc)
    now_ms = datetime_to_ms(now)

    next_close_ms = ((now_ms // tf_ms) + 1) * tf_ms
    target_ms = next_close_ms + (buffer_seconds * 1000)

    sleep_seconds = (target_ms - now_ms) / 1000

    if sleep_seconds > 0:
        log(
            "[WAITING FOR CANDLE CLOSE]",
            tf=tf,
            wake_up_ist=ms_to_ist(target_ms),
            sleep_seconds=round(sleep_seconds, 2),
        )

        time.sleep(sleep_seconds)


# ======================================================
# SYMBOL LOADING
# ======================================================

def normalize_symbols(symbols):
    return sorted({s.strip().upper() for s in symbols if s and s.strip()})


def get_symbols_from_redis():

    raw = redis_client.get(REDIS_KEY)

    if not raw:
        return []

    try:
        data = json.loads(raw)
    except Exception:
        log("[REDIS ERROR] Invalid JSON")
        return []

    return normalize_symbols(data if isinstance(data, list) else [])

def get_symbols_from_db():

    session = SessionLocal()

    try:

        rows = session.execute("SELECT name FROM symbols").fetchall()

        symbols = [r[0] for r in rows]

        if symbols:
            log("[SYMBOL SOURCE] DB", count=len(symbols))

        return normalize_symbols(symbols)

    except Exception as e:

        log("[DB SYMBOL ERROR]", error=str(e))
        return []

    finally:
        session.close()


def get_symbols(symbols=None, btc_only=False):

    if symbols:
        return normalize_symbols(symbols)

    if btc_only:
        return TEST_SYMBOLS

    # 1️⃣ Try DB first
    db_symbols = get_symbols_from_db()

    if db_symbols:
        return db_symbols

    # 2️⃣ fallback to redis
    log("[SYMBOL FALLBACK] Redis")

    return get_symbols_from_redis()


# ======================================================
# DATABASE
# ======================================================

def get_last_open_time_from_db(symbol: str, tf: str):

    Model = MODEL_MAP.get(tf)

    if not Model:
        return None

    session = SessionLocal()

    try:
        return (
            session.query(func.max(Model.open_time))
            .filter(Model.symbol == symbol)
            .scalar()
        )

    finally:
        session.close()


# ======================================================
# BINANCE FETCH
# ======================================================

def fetch_klines(symbol, tf, start_time, end_time):

    params = {
        "symbol": symbol,
        "interval": tf,
        "startTime": start_time,
        "endTime": end_time,
        "limit": LIMIT,
    }

    log(
        "[API REQUEST]",
        symbol=symbol,
        interval=tf,
        start_ms=start_time,
        end_ms=end_time,
        start_ist=ms_to_ist(start_time),
        end_ist=ms_to_ist(end_time),
        limit=LIMIT,
    )

    try:

        resp = requests.get(URL, params=params, timeout=10)
        resp.raise_for_status()

        data = resp.json()

        if data:
            log(
                "[API RESPONSE]",
                symbol=symbol,
                candles=len(data),
                oldest=ms_to_ist(data[0][0]),
                newest=ms_to_ist(data[-1][0]),
            )

        return data

    except Exception as e:

        log("[API ERROR]", symbol=symbol, error=str(e))

        return []


# ======================================================
# MAIN BACKFILL ENGINE
# ======================================================

def backfill_all_symbols(tf, start_date=None, symbols=None, btc_only=TEST_MODE):

    tf_ms = get_tf_ms(tf)

    if not tf_ms:
        raise ValueError(f"Unsupported timeframe: {tf}")

    now_utc = datetime.now(timezone.utc)

    if start_date is None:
        start_date = now_utc - timedelta(days=365)

    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    start_ts = datetime_to_ms(start_date)

    symbols = get_symbols(symbols=symbols, btc_only=btc_only)

    if not symbols:
        log("[EXIT] No symbols found")
        return

    log("[START]", tf=tf, symbols=len(symbols))

    state = {}

    for symbol in symbols:

        last_ts = get_last_open_time_from_db(symbol, tf)

        if last_ts:
            cursor = align_timestamp(last_ts, tf_ms) + tf_ms
            log("[DB RESUME]", symbol=symbol, last=ms_to_ist(last_ts))
        else:
            cursor = align_timestamp(start_ts, tf_ms)
            log("[DB FRESH]", symbol=symbol, start=ms_to_ist(cursor))

        state[symbol] = {
            "cursor": cursor,
            "done": False,
            "fetched": 0,
        }

    loop = 0

    while True:

        loop += 1

        if loop > MAX_GLOBAL_LOOPS:
            log("[GLOBAL SAFETY EXIT]")
            break

        active = 0

        for symbol in symbols:

            info = state[symbol]

            if info["done"]:
                continue

            active += 1

            now_ms = datetime_to_ms(datetime.now(timezone.utc))
            end_ts = align_timestamp(now_ms, tf_ms)

            if info["cursor"] > end_ts:
                info["done"] = True
                continue

            previous_cursor = info["cursor"]

            klines = fetch_klines(
                symbol,
                tf,
                start_time=info["cursor"],
                end_time=end_ts,
            )

            if not klines:
                info["done"] = True
                continue

            payloads = build_payloads(symbol, tf, klines)

            insert_candles_batch(tf, payloads)

            info["fetched"] += len(klines)

            newest_open_time = klines[-1][0]

            info["cursor"] = align_timestamp(newest_open_time, tf_ms) + tf_ms

            if info["cursor"] <= previous_cursor:
                log("[SAFETY STOP - CURSOR STALLED]", symbol=symbol)
                info["done"] = True
                continue

            if len(klines) < LIMIT:
                info["done"] = True

            time.sleep(0.12)

        if active == 0:
            break

    log("======== FINAL SUMMARY ========")

    for sym, info in state.items():
        log("[SUMMARY]", symbol=sym, total_fetched=info["fetched"])


# ======================================================
# ENTRY
# ======================================================

# ======================================================
# ENTRY
# ======================================================

if __name__ == "__main__":

    TFS = ["1d", "4h", "1h", "15m", "5m"]
    # TFS = ["1d", "4h", "1h"]

    first_run = True

    while True:

        try:

            for TF in TFS:

                # -----------------------------------------
                # FIRST RUN → DO NOT WAIT
                # -----------------------------------------
                if not first_run:
                    wait_until_next_candle_close(TF, buffer_seconds=4)

                log("[BACKFILL RUN]", tf=TF, first_run=first_run)

                backfill_all_symbols(TF)

            first_run = False

        except Exception as e:

            log("[RUNTIME ERROR]", error=str(e))

            time.sleep(5)