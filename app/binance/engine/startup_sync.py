from sqlalchemy import text
from app.db import SessionLocal
from app.config import TIMEFRAMES
from app.binance.engine.time_utils import get_exchange_time_ms, floor_time
from app.binance.engine.gap_watchdog import backfill_symbol


def run_startup_sync():

    print("\n=================================================")
    print("[BOOT PHASE 1] STARTUP BACKFILL ENGINE STARTED")
    print("=================================================\n")

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
            print("[SYNC] No symbols found in 1m table.")
            print("[SYNC] Skipping startup backfill.")
            print("=================================================\n")
            return

        print(f"[SYNC] Source symbols from 1m: {len(symbols)}")

        # -------------------------------------------------
        # STEP 2 — Fix 1m first
        # -------------------------------------------------
        tf_1m = TIMEFRAMES["1m"]
        tf_ms_1m = tf_1m["tf_ms"]
        table_1m = tf_1m["table"]

        expected_last_1m = floor_time(exchange_now, tf_ms_1m) - tf_ms_1m

        print("\n[SYNC] Checking 1m gaps")

        for symbol in symbols:

            last_open = db.execute(text("""
                SELECT MAX(open_time)
                FROM candles_1m
                WHERE symbol = :symbol
            """), {"symbol": symbol}).scalar()

            if last_open and last_open < expected_last_1m:
                print(f"[1m GAP] {symbol}")
                backfill_symbol(
                    symbol,
                    "1m",
                    table_1m,
                    tf_ms_1m,
                    last_open + tf_ms_1m,
                    expected_last_1m,
                )

        print("[SYNC] 1m sync completed.")

        # -------------------------------------------------
        # STEP 3 — Fix Higher TFs using SAME symbols
        # -------------------------------------------------
        for tf, config in TIMEFRAMES.items():

            if tf == "1m":
                continue

            table = config["table"]
            tf_ms = config["tf_ms"]

            expected_last = floor_time(exchange_now, tf_ms) - tf_ms

            print(f"\n[SYNC] Checking TF={tf}")

            for symbol in symbols:

                last_open = db.execute(text(f"""
                    SELECT MAX(open_time)
                    FROM {table}
                    WHERE symbol = :symbol
                """), {"symbol": symbol}).scalar()

                # Table empty for this symbol
                if not last_open:
                    print(f"[INIT BACKFILL] {symbol} | TF={tf}")
                    backfill_symbol(
                        symbol,
                        tf,
                        table,
                        tf_ms,
                        0,
                        expected_last,
                    )

                # Partial gap
                elif last_open < expected_last:
                    print(f"[GAP BACKFILL] {symbol} | TF={tf}")
                    backfill_symbol(
                        symbol,
                        tf,
                        table,
                        tf_ms,
                        last_open + tf_ms,
                        expected_last,
                    )

        print("\n=================================================")
        print("[BOOT PHASE 1 COMPLETED SUCCESSFULLY]")
        print("=================================================\n")

    finally:
        db.close()
