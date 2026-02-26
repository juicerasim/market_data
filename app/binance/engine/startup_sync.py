from sqlalchemy import text
from app.db import SessionLocal
from app.config import TIMEFRAMES
from app.binance.engine.time_utils import get_exchange_time_ms, floor_time
from app.binance.engine.gap_watchdog import backfill_symbol
from app.logging_config import get_logger

logger = get_logger("market_data.binance.startup_sync")


def run_startup_sync():

    logger.info("STARTUP BACKFILL ENGINE STARTED")

    exchange_now = get_exchange_time_ms()
    db = SessionLocal()

    try:
        # -------------------------------------------------
        # STEP 1 — 1m is the ONLY source of truth
        # -------------------------------------------------
        rows = db.execute(text("""
            SELECT DISTINCT symbol FROM candles_1m
        """)).fetchall()

        symbols = [r[0] for r in rows]

        if not symbols:
            logger.info("No symbols found in 1m table. Skipping startup backfill.")
            return

        logger.info("Source symbols from 1m: %d", len(symbols))

        # -------------------------------------------------
        # STEP 2 — Fix 1m first
        # -------------------------------------------------
        tf_1m = TIMEFRAMES["1m"]
        tf_ms_1m = tf_1m["tf_ms"]
        table_1m = tf_1m["table"]

        expected_last_1m = floor_time(exchange_now, tf_ms_1m) - tf_ms_1m

        logger.info("Checking 1m gaps")

        for symbol in symbols:

            last_open = db.execute(text("""
                SELECT MAX(open_time)
                FROM candles_1m
                WHERE symbol = :symbol
            """), {"symbol": symbol}).scalar()

            if last_open and last_open < expected_last_1m:
                logger.warning("1m GAP detected %s", symbol)

                backfill_symbol(
                    symbol,
                    "1m",
                    table_1m,
                    tf_ms_1m,
                    last_open + tf_ms_1m,
                    expected_last_1m,
                )

        logger.info("1m sync completed")

        # -------------------------------------------------
        # STEP 3 — Fix Higher TFs
        # -------------------------------------------------
        for tf, config in TIMEFRAMES.items():

            if tf == "1m":
                continue

            # 🔥 SKIP DERIVED TF (like 2m)
            if not config.get("api", False):
                logger.debug("Skipping derived TF=%s", tf)
                continue

            table = config["table"]
            tf_ms = config["tf_ms"]

            expected_last = floor_time(exchange_now, tf_ms) - tf_ms

            logger.info("Checking TF=%s", tf)

            for symbol in symbols:

                last_open = db.execute(text(f"""
                    SELECT MAX(open_time)
                    FROM {table}
                    WHERE symbol = :symbol
                """), {"symbol": symbol}).scalar()

                if not last_open:
                    logger.info("INIT BACKFILL %s TF=%s", symbol, tf)

                    backfill_symbol(
                        symbol,
                        tf,
                        table,
                        tf_ms,
                        0,
                        expected_last,
                    )

                elif last_open < expected_last:
                    logger.warning("GAP BACKFILL %s TF=%s", symbol, tf)

                    backfill_symbol(
                        symbol,
                        tf,
                        table,
                        tf_ms,
                        last_open + tf_ms,
                        expected_last,
                    )

        logger.info("BOOT PHASE 1 COMPLETED SUCCESSFULLY")

    finally:
        db.close()
