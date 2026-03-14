import requests
import time
import json
import os
import traceback
import signal

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text, func
from requests.adapters import HTTPAdapter

from app.db import SessionLocal
from app.config import TIMEFRAMES
from app.binance.payload_builder import build_payloads
from app.binance.scripts.insert import insert_candles_batch, MODEL_MAP


# ==========================================================
# CONFIG
# ==========================================================

URL = "https://fapi.binance.com/fapi/v1/klines"

LIMIT = 500
MAX_RETRIES = 5
API_SLEEP = 0.05

CANDLE_BUFFER_MS = 3000

TFS = ["5m", "15m", "1h", "4h", "1d"]

IST = ZoneInfo("Asia/Kolkata")

RUNNING = True
START_TIME = time.time()

# ==========================================================
# HTTP SESSION (connection pooling)
# ==========================================================

session_http = requests.Session()

adapter = HTTPAdapter(
    pool_connections=50,
    pool_maxsize=50
)

session_http.mount("https://", adapter)

# ==========================================================
# LOGGING SETUP
# ==========================================================

LOG_DIR = os.path.join("logs", "collector")
os.makedirs(LOG_DIR, exist_ok=True)

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

LOG_FILE = os.path.join(LOG_DIR, f"collector_{RUN_ID}.jsonl")

log_fp = open(LOG_FILE, "a", buffering=1)


def log(level, event, **data):

    record = {
        "ts": datetime.now(timezone.utc)
        .astimezone(IST)
        .strftime("%Y-%m-%d %H:%M:%S"),

        "level": level,
        "event": event,
        "run_id": RUN_ID,
        "pid": os.getpid(),
        "uptime_sec": int(time.time() - START_TIME),
        **data
    }

    line = json.dumps(record)

    print(line)

    log_fp.write(line + "\n")


# ==========================================================
# SIGNAL HANDLER (graceful shutdown)
# ==========================================================

def shutdown_handler(signum, frame):

    global RUNNING

    log("INFO", "SHUTDOWN_SIGNAL", signal=signum)

    RUNNING = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ==========================================================
# TIME HELPERS
# ==========================================================

def datetime_to_ms(dt):

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1000)


def get_tf_ms(tf):

    if tf in TIMEFRAMES:
        return int(TIMEFRAMES[tf]["tf_ms"])

    fallback = {
        "5m": 300000,
        "15m": 900000,
        "1h": 3600000,
        "4h": 14400000,
        "1d": 86400000
    }

    return fallback.get(tf)


def align(ts, tf_ms):

    return ts - (ts % tf_ms)


# ==========================================================
# SYMBOL SOURCE (DB only)
# ==========================================================

def get_symbols():

    session = SessionLocal()

    try:

        rows = session.execute(
            text("SELECT name FROM symbols")
        ).fetchall()

        symbols = [r[0] for r in rows]

        return symbols

    finally:
        session.close()


# ==========================================================
# BULK LAST CANDLES
# ==========================================================

def get_last_candles_bulk(tf):

    Model = MODEL_MAP.get(tf)

    if not Model:
        return {}

    session = SessionLocal()

    try:

        rows = (
            session.query(Model.symbol, func.max(Model.open_time))
            .group_by(Model.symbol)
            .all()
        )

        return {symbol: ts for symbol, ts in rows}

    finally:
        session.close()


# ==========================================================
# BINANCE FETCH
# ==========================================================

def fetch_klines(symbol, tf, start_time):

    params = {
        "symbol": symbol,
        "interval": tf,
        "startTime": start_time,
        "limit": LIMIT
    }

    for retry in range(MAX_RETRIES):

        try:

            r = session_http.get(
                URL,
                params=params,
                timeout=(3, 10)
            )

            if r.status_code == 429:

                sleep = 2 ** retry

                log(
                    "WARN",
                    "RATE_LIMIT",
                    symbol=symbol,
                    sleep=sleep
                )

                time.sleep(sleep)

                continue

            r.raise_for_status()

            return r.json()

        except Exception:

            log(
                "ERROR",
                "API_ERROR",
                symbol=symbol,
                trace=traceback.format_exc()
            )

            time.sleep(2 ** retry)

    return []


# ==========================================================
# PROCESS SYMBOL
# ==========================================================

def process_symbol(symbol, tf, safe_now, last_ts_map):

    tf_ms = get_tf_ms(tf)

    last_ts = last_ts_map.get(symbol)

    if last_ts:
        cursor = align(last_ts, tf_ms) + tf_ms
    else:
        return 0

    end_ts = align(safe_now, tf_ms)

    if cursor > end_ts:
        return 0

    total = 0

    while cursor <= end_ts:

        klines = fetch_klines(symbol, tf, cursor)

        if not klines:
            break

        payloads = build_payloads(symbol, tf, klines)

        payloads = [
            p for p in payloads
            if p["open_time"] >= cursor
        ]

        if payloads:

            try:

                insert_candles_batch(tf, payloads)

            except Exception:

                log(
                    "ERROR",
                    "DB_INSERT_FAILED",
                    symbol=symbol,
                    tf=tf,
                    trace=traceback.format_exc()
                )

        total += len(payloads)

        last_open_time = klines[-1][0]

        if last_open_time <= cursor:

            log(
                "WARN",
                "CURSOR_STALLED",
                symbol=symbol,
                tf=tf,
                cursor=cursor
            )

            break

        cursor = last_open_time + tf_ms

        time.sleep(API_SLEEP)

    return total


# ==========================================================
# RUN TIMEFRAME
# ==========================================================

def run_tf(tf, symbols):

    now_ms = datetime_to_ms(datetime.now(timezone.utc))

    safe_now = now_ms - CANDLE_BUFFER_MS

    log("INFO", "TF_CHECK", tf=tf)

    last_ts_map = get_last_candles_bulk(tf)

    for symbol in symbols:

        try:

            count = process_symbol(symbol, tf, safe_now, last_ts_map)

            if count > 0:

                log(
                    "INFO",
                    "CANDLES_INSERTED",
                    tf=tf,
                    symbol=symbol,
                    candles=count
                )

        except Exception:

            log(
                "ERROR",
                "SYMBOL_PROCESS_ERROR",
                symbol=symbol,
                tf=tf,
                trace=traceback.format_exc()
            )


# ==========================================================
# SCHEDULER
# ==========================================================

def run_collector():

    log("INFO", "COLLECTOR_STARTED")

    tf_intervals = {tf: get_tf_ms(tf) for tf in TFS}

    next_run = {}

    now_ms = datetime_to_ms(datetime.now(timezone.utc))

    for tf in TFS:
        next_run[tf] = now_ms

    last_heartbeat = time.time()

    while RUNNING:

        try:

            now_ms = datetime_to_ms(datetime.now(timezone.utc))

            symbols = get_symbols()

            for tf in TFS:

                if now_ms >= next_run[tf]:

                    try:

                        run_tf(tf, symbols)

                    except Exception:

                        log(
                            "ERROR",
                            "TF_CRASH",
                            tf=tf,
                            trace=traceback.format_exc()
                        )

                    tf_ms = tf_intervals[tf]

                    next_close = ((now_ms // tf_ms) + 1) * tf_ms

                    next_run[tf] = next_close + CANDLE_BUFFER_MS

            if time.time() - last_heartbeat > 60:

                log("INFO", "COLLECTOR_HEARTBEAT")

                last_heartbeat = time.time()

        except Exception:

            log(
                "CRITICAL",
                "COLLECTOR_LOOP_CRASH",
                trace=traceback.format_exc()
            )

        time.sleep(1)

    log("INFO", "COLLECTOR_STOPPED")


# ==========================================================
# ENTRY
# ==========================================================

if __name__ == "__main__":

    run_collector()