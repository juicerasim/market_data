from sqlalchemy import text
from app.db import SessionLocal
from app.binance.engine.time_utils import get_exchange_time_ms, floor_time
from app.config import TIMEFRAMES
from .gap_watchdog import backfill_symbol


def run_startup_sync():

    print("\n=================================================")
    print("[BOOT PHASE 1] STARTUP BACKFILL ENGINE STARTED")
    print("=================================================\n")

    exchange_now = get_exchange_time_ms()
    db = SessionLocal()

    total_tf_processed = 0
    total_symbols_processed = 0
    total_gaps_detected = 0

    try:
        for tf, config in TIMEFRAMES.items():

            table = config["table"]
            tf_ms = config["tf_ms"]

            print("-------------------------------------------------")
            print(f"[TF START] {tf} | Table={table}")
            print("-------------------------------------------------")

            # if not table_exists(db, table):
            #     print(f"[TF SKIP] {tf} → Table not found")
            #     continue

            expected_last = floor_time(exchange_now, tf_ms) - tf_ms

            rows = db.execute(text(f"""
                SELECT DISTINCT symbol FROM {table}
            """)).fetchall()

            symbols = [r[0] for r in rows]

            print(f"[TF INFO] {tf} → symbols_found={len(symbols)}")

            tf_gap_count = 0
            tf_symbol_checked = 0

            for symbol in symbols:

                tf_symbol_checked += 1
                total_symbols_processed += 1

                print(f"[SYMBOL CHECK] TF={tf} | Symbol={symbol}")

                last_open = db.execute(text(f"""
                    SELECT MAX(open_time)
                    FROM {table}
                    WHERE symbol = :symbol
                """), {"symbol": symbol}).scalar()

                if not last_open:
                    print(f"[SYMBOL SKIP] {symbol} → No data")
                    continue

                if last_open < expected_last:

                    tf_gap_count += 1
                    total_gaps_detected += 1

                    print(
                        f"[GAP DETECTED] TF={tf} | Symbol={symbol} | "
                        f"db_last={last_open} | expected={expected_last}"
                    )

                    gap_start = last_open + tf_ms
                    gap_end = expected_last

                    print(
                        f"[BACKFILL START] TF={tf} | Symbol={symbol} | "
                        f"Range=({gap_start} → {gap_end})"
                    )

                    backfill_symbol(
                        symbol,
                        tf,
                        table,
                        tf_ms,
                        gap_start,
                        gap_end,
                    )

                    print(f"[BACKFILL DONE] TF={tf} | Symbol={symbol}")

            print("-------------------------------------------------")
            print(
                f"[TF COMPLETE] {tf} | "
                f"checked={tf_symbol_checked} | gaps={tf_gap_count}"
            )
            print("-------------------------------------------------\n")

            total_tf_processed += 1

    finally:
        db.close()

    print("=================================================")
    print("[BOOT PHASE 1 COMPLETED]")
    print(
        f"TFs_processed={total_tf_processed} | "
        f"symbols_checked={total_symbols_processed} | "
        f"total_gaps={total_gaps_detected}"
    )
    print("=================================================\n")
