import time
from datetime import datetime, timezone

from sqlalchemy import text
from app.db import SessionLocal
from app.config import TIMEFRAMES
from app.binance.engine.time_utils import get_exchange_time_ms, floor_time
from app.binance.scripts.kline_history import fetch_klines, log
from app.binance.scripts.insert import insert_candles_batch
from app.binance.payload_builder import build_payloads


def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# --------------------------------------------------
# Generic Backfill
# --------------------------------------------------
def backfill_symbol(symbol, tf, table, tf_ms, start_ms, end_ms):

    log(
        "[WATCHDOG BACKFILL START]",
        symbol=symbol,
        tf=tf,
        start=ms_to_utc(start_ms),
        end=ms_to_utc(end_ms),
    )

    cursor_end = end_ms
    total_inserted = 0

    while True:

        klines = fetch_klines(
            symbol=symbol,
            tf=tf,
            start_time=start_ms,
            end_time=cursor_end,
        )

        if not klines:
            break

        payloads = build_payloads(symbol, tf, klines)
        insert_candles_batch(tf, payloads)

        total_inserted += len(payloads)

        oldest_open = klines[0][0]
        cursor_end = oldest_open - 1

        if oldest_open <= start_ms:
            break

        time.sleep(0.12)

    log(
        "[WATCHDOG BACKFILL DONE]",
        symbol=symbol,
        tf=tf,
        inserted=total_inserted,
    )


# --------------------------------------------------
# Multi-Timeframe Watchdog
# --------------------------------------------------
def run_gap_watchdog():

    log("[WATCHDOG] Started (Multi-TF Mode)")

    while True:

        time.sleep(60)

        exchange_now = get_exchange_time_ms()
        db = SessionLocal()

        try:
            rows = db.execute(text("""
                SELECT DISTINCT symbol FROM candles_1m
            """)).fetchall()

            symbols = [r[0] for r in rows]

            if not symbols:
                log("[WATCHDOG] No symbols in 1m table. Waiting...")
                continue

            for tf, config in TIMEFRAMES.items():

                # 🔥 SKIP DERIVED TF (like 2m)
                if not config.get("api", False):
                    log("[WATCHDOG] Skipping derived TF", tf=tf)
                    continue

                table = config["table"]
                tf_ms = config["tf_ms"]

                grace_ms = tf_ms
                expected_last = floor_time(exchange_now - grace_ms, tf_ms) - tf_ms

                log("[CHECK TF]", tf=tf, expected=ms_to_utc(expected_last))

                for symbol in symbols:

                    last_open = db.execute(text(f"""
                        SELECT MAX(open_time)
                        FROM {table}
                        WHERE symbol = :symbol
                    """), {"symbol": symbol}).scalar()

                    if not last_open:
                        continue

                    if last_open < expected_last:

                        log(
                            "[WATCHDOG GAP DETECTED]",
                            symbol=symbol,
                            tf=tf,
                            db_last=ms_to_utc(last_open),
                            expected=ms_to_utc(expected_last),
                        )

                        gap_start = last_open + tf_ms
                        gap_end = expected_last

                        backfill_symbol(
                            symbol,
                            tf,
                            table,
                            tf_ms,
                            gap_start,
                            gap_end,
                        )

        finally:
            db.close()
