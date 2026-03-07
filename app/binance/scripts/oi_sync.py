import os
import json
import time
import requests

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from sqlalchemy import text

from app.db import SessionLocal


# =========================================================
# CONFIG
# =========================================================

OI_URL = "https://fapi.binance.com/futures/data/openInterestHist"
TIME_URL = "https://fapi.binance.com/fapi/v1/time"

IST = ZoneInfo("Asia/Kolkata")

API_DELAY = 0.15
OFFSET_REFRESH = 600

LOG_DIR = "logs/health"
os.makedirs(LOG_DIR, exist_ok=True)

OI_TFS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
}

OI_TABLES = {
    "5m": "open_interest_5m",
    "15m": "open_interest_15m",
    "1h": "open_interest_1h",
}

# TF specific buffers (Binance delay protection)
BUFFER_MAP = {
    "5m": 60_000,
    "15m": 90_000,
    "1h": 120_000,
}


# =========================================================
# LOGGING
# =========================================================

log_file = None


def start_cycle_log():

    global log_file

    ts = datetime.now(timezone.utc).astimezone(IST)

    filename = ts.strftime("oi_health %Y-%m-%d %H-%M-%S.jsonl")

    path = os.path.join(LOG_DIR, filename)

    log_file = open(path, "a")

    print(f"Logging → {path}")


def close_cycle_log():

    global log_file

    if log_file:
        log_file.close()
        log_file = None


def log(event, tf=None, symbol=None, payload=None, response=None, **extra):

    ts = datetime.now(timezone.utc).astimezone(IST)

    record = {
        "component": "OI",
        "event": event,
        "time_local": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "tf": tf,
        "symbol": symbol,
        "payload": payload,
        "response": response,
        **extra
    }

    print(record)

    if log_file:
        log_file.write(json.dumps(record) + "\n")
        log_file.flush()


# =========================================================
# EXCHANGE TIME
# =========================================================

def get_exchange_offset():

    r = requests.get(TIME_URL, timeout=5)

    r.raise_for_status()

    server_time = r.json()["serverTime"]

    local_time = int(time.time() * 1000)

    offset = server_time - local_time

    log("exchange_offset", response={"offset_ms": offset})

    return offset


def exchange_now(offset):

    return int(time.time() * 1000) + offset


# =========================================================
# SYMBOLS
# =========================================================

def get_symbols():

    session = SessionLocal()

    try:

        rows = session.execute(
            text("SELECT name FROM symbols")
        ).fetchall()

        symbols = [r[0] for r in rows]

        log("symbols_loaded", response={"count": len(symbols)})

        return symbols

    finally:
        session.close()


# =========================================================
# LAST OI MAP
# =========================================================

def get_last_oi_map(tf):

    table = OI_TABLES[tf]

    session = SessionLocal()

    try:

        rows = session.execute(
            text(f"""
            SELECT symbol, MAX(open_time) AS last_ts
            FROM {table}
            GROUP BY symbol
            """)
        ).fetchall()

        result = {}

        for r in rows:
            result[r[0]] = r[1]

        return result

    finally:
        session.close()


# =========================================================
# INSERT
# =========================================================

def insert_rows(rows, tf, symbol):

    if not rows:
        return

    table = OI_TABLES[tf]

    session = SessionLocal()

    try:

        session.execute(
            text(f"""
            INSERT INTO {table}
            (symbol, open_time, open_interest, oi_notional, open_time_utc)

            VALUES
            (:symbol, :open_time, :open_interest, :oi_notional, :open_time_utc)

            ON CONFLICT DO NOTHING
            """),
            rows
        )

        session.commit()

        log(
            "db_insert",
            tf=tf,
            symbol=symbol,
            response={"rows": len(rows)}
        )

    finally:
        session.close()


# =========================================================
# FETCH OI
# =========================================================

def fetch_oi(symbol, tf, start, end):

    all_rows = []

    while True:

        payload = {
            "symbol": symbol,
            "period": tf,
            "limit": 500
        }

        if start is not None:
            payload["startTime"] = start

        if end is not None:
            payload["endTime"] = end

        log(
            "api_request",
            tf=tf,
            symbol=symbol,
            payload=payload
        )

        r = requests.get(OI_URL, params=payload, timeout=(3, 10))

        r.raise_for_status()

        data = r.json()

        if not data:

            log("api_empty", tf=tf, symbol=symbol)

            break

        log("api_response", tf=tf, symbol=symbol, response={"rows": len(data)})

        rows = []

        for d in data:

            ts = int(d["timestamp"])

            rows.append({
                "symbol": symbol,
                "open_time": ts,
                "open_interest": float(d["sumOpenInterest"]),
                "oi_notional": float(d["sumOpenInterestValue"]),
                "open_time_utc": datetime.fromtimestamp(ts / 1000, timezone.utc)
            })

        all_rows.extend(rows)

        last_ts = rows[-1]["open_time"]

        start = last_ts + 1

        if len(data) < 500:
            break

    return all_rows


# =========================================================
# PROCESS SYMBOL
# =========================================================

def process_symbol(symbol, tf, tf_ms, expected, last_map):

    last_ts = last_map.get(symbol)

    if last_ts is None:
        start = None
    else:
        start = last_ts + 1

    end = expected

    if start is not None and start >= end:
        return

    gap_ms = None
    gap_candles = None

    if last_ts:
        gap_ms = expected - last_ts
        gap_candles = gap_ms // tf_ms

    log(
        "symbol_gap",
        tf=tf,
        symbol=symbol,
        response={
            "last_ts": last_ts,
            "expected_ts": expected,
            "gap_ms": gap_ms,
            "gap_candles": gap_candles,
            "last_local":
                datetime.fromtimestamp(last_ts / 1000, timezone.utc)
                .astimezone(IST)
                .strftime("%Y-%m-%d %H:%M:%S")
                if last_ts else None
        }
    )

    rows = fetch_oi(symbol, tf, start, end)

    insert_rows(rows, tf, symbol)

    time.sleep(API_DELAY)


# =========================================================
# RUN TF
# =========================================================

def run_tf(tf, tf_ms, offset):

    log("tf_start", tf=tf)

    symbols = get_symbols()

    last_map = get_last_oi_map(tf)

    if last_map:

        latest_ts = max(last_map.values())

        log(
            "tf_db_snapshot",
            tf=tf,
            response={
                "symbols_in_db": len(last_map),
                "latest_open_time": latest_ts,
                "latest_open_time_local":
                    datetime.fromtimestamp(latest_ts / 1000, timezone.utc)
                    .astimezone(IST)
                    .strftime("%Y-%m-%d %H:%M:%S")
            }
        )

    now = exchange_now(offset)

    buffer = BUFFER_MAP[tf]

    expected = ((now - buffer) // tf_ms) * tf_ms

    for symbol in symbols:

        try:

            process_symbol(
                symbol,
                tf,
                tf_ms,
                expected,
                last_map
            )

        except Exception as e:

            log("symbol_error", tf=tf, symbol=symbol, response={"error": str(e)})

    log("tf_complete", tf=tf)


# =========================================================
# SCHEDULER
# =========================================================

def scheduler():

    offset = get_exchange_offset()

    last_offset_update = time.time()

    last_run = {tf: 0 for tf in OI_TFS}

    while True:

        cycle_start = time.time()

        now = exchange_now(offset)

        if time.time() - last_offset_update > OFFSET_REFRESH:

            try:
                offset = get_exchange_offset()
                last_offset_update = time.time()

            except Exception as e:
                log("offset_refresh_failed", response={"error": str(e)})

        cycle_started = False

        for tf, tf_ms in OI_TFS.items():

            buffer = BUFFER_MAP[tf]

            expected = ((now - buffer) // tf_ms) * tf_ms

            if expected > last_run[tf]:

                if not cycle_started:
                    start_cycle_log()
                    log("cycle_start")

                cycle_started = True

                run_tf(tf, tf_ms, offset)

                last_run[tf] = expected

        if cycle_started:

            duration = round(time.time() - cycle_start, 2)

            log(
                "cycle_complete",
                response={"duration_sec": duration}
            )

            close_cycle_log()

        time.sleep(1)


# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":

    try:
        scheduler()

    except Exception as e:

        log("worker_crashed", response={"error": str(e)})

        close_cycle_log()

        raise