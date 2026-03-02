import requests
import json
import logging
from datetime import datetime, timezone
from sqlalchemy import text
from app.db import SessionLocal
from app.redis_client import redis_client


# =====================================================
# LOGGING CONFIG
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
    logger.info(f"Latest closed candle: {closed}")
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
        logger.info(f"{table_name} | Inserted/Updated {len(rows)} rows")

    except Exception as e:
        logger.error(f"{table_name} | DB error: {e}")
        db.rollback()
    finally:
        db.close()


# =====================================================
# BINANCE FETCH
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
        logger.error(f"{symbol} | API {resp.status_code} | {resp.text}")
        return []

    data = resp.json()
    logger.info(f"{symbol} | API rows: {len(data)}")
    return data


# =====================================================
# MAIN FLEXIBLE FUNCTION
# =====================================================

def sync_open_interest(
    timeframe: str,
    coins: list[str] | None = None,
    backfill_days: int = 7
):
    """
    timeframe: '5m', '1h', etc.
    coins: optional list like ['BTCUSDT', 'ETHUSDT']
    backfill_days: number of days to backfill
    """

    logger.info(f"Starting OI sync | TF={timeframe}")

    interval_ms = timeframe_to_ms(timeframe)
    max_window_ms = LIMIT * interval_ms
    table_name = f"open_interest_{timeframe}"

    # -------------------------------------------------
    # Determine symbols
    # -------------------------------------------------

    if coins:
        symbols = coins
        logger.info(f"Using passed coin list ({len(symbols)})")
    else:
        symbols = json.loads(redis_client.get(REDIS_KEY) or "[]")
        logger.info(f"Using Redis symbols ({len(symbols)})")

    if not symbols:
        logger.warning("No symbols found.")
        return

    # -------------------------------------------------
    # Calculate time range
    # -------------------------------------------------

    latest_closed = get_latest_closed_tf_ms(interval_ms)

    start_base = latest_closed - (
        backfill_days * 24 * 60 * 60 * 1000
    )
    start_base -= (start_base % interval_ms)

    # -------------------------------------------------
    # Process each symbol
    # -------------------------------------------------

    for symbol in symbols:

        logger.info(f"Processing {symbol}")

        start_ts = start_base
        end_ts = latest_closed
        total_inserted = 0

        while start_ts < end_ts:

            window_end = min(start_ts + max_window_ms, end_ts)

            logger.info(
                f"{symbol} | Window {start_ts} -> {window_end}"
            )

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
            total_inserted += len(rows)

            last_ts = int(oi_data[-1]["timestamp"])
            last_ts -= (last_ts % interval_ms)

            start_ts = last_ts + interval_ms

            if len(oi_data) < LIMIT:
                break

        logger.info(f"{symbol} | Total inserted: {total_inserted}")

    logger.info("OI sync completed.")


# =====================================================
# OPTIONAL ENTRY POINT
# =====================================================

if __name__ == "__main__":

    # Example usage:
    sync_open_interest(
        timeframe="1h",
        backfill_days=7
    )