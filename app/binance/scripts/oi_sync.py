import requests
import json
import time
import logging
from datetime import datetime, timezone
from sqlalchemy import text
from app.db import SessionLocal
from app.redis_client import redis_client


# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# =====================================================
# CONSTANTS
# =====================================================

LIMIT = 1000
REDIS_KEY = "liquid_coins"

OI_URL = "https://fapi.binance.com/futures/data/openInterestHist"
SERVER_TIME_URL = "https://fapi.binance.com/fapi/v1/time"

TIMEFRAMES = ["5m", "15m", "1h"]
BASE_TF = "5m"


# =====================================================
# TIMEFRAME HELPERS
# =====================================================

def timeframe_to_ms(tf: str) -> int:
    unit = tf[-1]
    value = int(tf[:-1])

    if unit == "m":
        return value * 60 * 1000
    elif unit == "h":
        return value * 60 * 60 * 1000
    elif unit == "d":
        return value * 24 * 60 * 60 * 1000
    else:
        raise ValueError(f"Unsupported timeframe: {tf}")


def get_binance_server_time() -> int:
    resp = requests.get(SERVER_TIME_URL, timeout=5)
    resp.raise_for_status()
    return resp.json()["serverTime"]


def get_latest_closed_tf_ms(interval_ms: int) -> int:
    server_time = get_binance_server_time()
    closed = server_time - (server_time % interval_ms)
    return closed


# =====================================================
# WAIT FOR NEXT CANDLE CLOSE
# =====================================================

def wait_until_next_candle_close(tf: str, buffer_seconds: int = 3):

    interval_ms = timeframe_to_ms(tf)
    server_time = get_binance_server_time()

    next_close = ((server_time // interval_ms) + 1) * interval_ms
    target = next_close + (buffer_seconds * 1000)

    sleep_seconds = (target - server_time) / 1000

    if sleep_seconds > 0:
        wake_utc = datetime.fromtimestamp(target / 1000, tz=timezone.utc)

        logger.info(
            f"Waiting for {tf} close | wake at {wake_utc} | sleep {round(sleep_seconds,2)}s"
        )

        time.sleep(sleep_seconds)


# =====================================================
# DETECT CLOSED TIMEFRAMES
# =====================================================

def get_closed_timeframes(server_time_ms, timeframes):

    closed = []

    for tf in timeframes:

        interval = timeframe_to_ms(tf)

        if server_time_ms % interval < 5000:
            closed.append(tf)

    return closed


# =====================================================
# DATABASE INSERT
# =====================================================

def insert_oi_batch(table_name: str, rows: list):

    if not rows:
        return

    db = SessionLocal()

    try:

        q = text(f"""
            INSERT INTO {table_name}
            (symbol, open_time, open_time_utc,
             open_interest, oi_notional)
            VALUES (:symbol, :open_time, :open_time_utc,
                    :open_interest, :oi_notional)
            ON CONFLICT (symbol, open_time)
            DO UPDATE SET
                open_interest = EXCLUDED.open_interest,
                oi_notional = EXCLUDED.oi_notional,
                open_time_utc = EXCLUDED.open_time_utc
        """)

        db.execute(q, rows)
        db.commit()

        logger.info(f"{table_name} | upserted {len(rows)} rows")

    except Exception as e:

        logger.error(f"{table_name} | DB error: {e}")
        db.rollback()

    finally:

        db.close()


# =====================================================
# FETCH FROM BINANCE
# =====================================================

def fetch_oi(symbol: str, timeframe: str, start_time: int, end_time: int):

    params = {
        "symbol": symbol,
        "period": timeframe,
        "startTime": start_time,
        "endTime": end_time,
        "limit": LIMIT
    }

    resp = requests.get(OI_URL, params=params, timeout=10)

    if resp.status_code != 200:

        logger.error(f"{symbol} | API error {resp.status_code}")
        return []

    data = resp.json()

    logger.info(f"{symbol} | rows fetched {len(data)}")

    return data


# =====================================================
# MAIN SYNC FUNCTION
# =====================================================

def sync_open_interest(
    timeframe: str,
    coins: list[str] | None = None,
    backfill_days: int = 7
):

    logger.info(f"Starting OI sync | TF={timeframe}")

    interval_ms = timeframe_to_ms(timeframe)
    max_window_ms = LIMIT * interval_ms

    table_name = f"open_interest_{timeframe}"

    if coins:
        symbols = coins
    else:
        symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")

    if not symbols:
        logger.warning("No symbols found")
        return

    latest_closed = get_latest_closed_tf_ms(interval_ms)

    start_base = latest_closed - (
        backfill_days * 24 * 60 * 60 * 1000
    )

    start_base -= (start_base % interval_ms)

    for symbol in symbols:

        logger.info(f"Processing {symbol}")

        start_ts = start_base
        end_ts = latest_closed

        while start_ts < end_ts:

            window_end = min(start_ts + max_window_ms, end_ts)

            oi_data = fetch_oi(
                symbol,
                timeframe,
                start_ts,
                window_end
            )

            if not oi_data:
                break

            rows = []

            for item in oi_data:

                ts = int(item["timestamp"])
                ts -= (ts % interval_ms)

                rows.append({
                    "symbol": item["symbol"],
                    "open_time": ts,
                    "open_time_utc": datetime.fromtimestamp(
                        ts / 1000,
                        tz=timezone.utc
                    ),
                    "open_interest": float(item["sumOpenInterest"]),
                    "oi_notional": float(item["sumOpenInterestValue"]),
                })

            insert_oi_batch(table_name, rows)

            last_ts = int(oi_data[-1]["timestamp"])
            last_ts -= (last_ts % interval_ms)

            start_ts = last_ts + interval_ms

            if len(oi_data) < LIMIT:
                break


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":

    first_run = True

    while True:

        try:

            if first_run:
                logger.info("First run → immediate sync")

                for tf in TIMEFRAMES:
                    sync_open_interest(tf, backfill_days=7)

                first_run = False

            wait_until_next_candle_close(BASE_TF, buffer_seconds=3)

            server_time = get_binance_server_time()

            closed_tfs = get_closed_timeframes(server_time, TIMEFRAMES)

            for tf in closed_tfs:

                logger.info(f"{tf} candle closed → syncing")

                sync_open_interest(
                    timeframe=tf,
                    backfill_days=1
                )

        except Exception as e:

            logger.error(f"Runtime error: {e}")

            time.sleep(5)