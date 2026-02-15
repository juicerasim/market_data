from sqlalchemy import text
from app.db import SessionLocal
from app.binance.engine.time_utils import get_exchange_time_ms, floor_time
from app.config import TIMEFRAMES
from .gap_watchdog import backfill_symbol


def run_startup_sync():

    print("\n==============================")
    print("[BOOT PHASE 1] STARTUP SYNC")
    print("==============================")

    exchange_now = get_exchange_time_ms()

    db = SessionLocal()

    try:
        for tf, config in TIMEFRAMES.items():

            table = config["table"]
            tf_ms = config["tf_ms"]

            if not table_exists(db, table):
                print(f"[SYNC] SKIP → Table {table} not found")
                continue

            expected_last = floor_time(exchange_now, tf_ms) - tf_ms

            print(f"\n[SYNC] Checking TF={tf} | expected_last={expected_last}")

            rows = db.execute(text(f"""
                SELECT DISTINCT symbol FROM {table}
            """)).fetchall()

            symbols = [r[0] for r in rows]

            print(f"[SYNC] Found {len(symbols)} symbols for TF={tf}")

            for symbol in symbols:

                last_open = db.execute(text(f"""
                    SELECT MAX(open_time)
                    FROM {table}
                    WHERE symbol = :symbol
                """), {"symbol": symbol}).scalar()

                if not last_open:
                    continue

                if last_open < expected_last:

                    print(
                        f"[SYNC GAP] {symbol} | TF={tf} | "
                        f"db_last={last_open} | expected={expected_last}"
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

    print("\n[SYNC] Startup sync completed\n")

def table_exists(db, table_name):
    result = db.execute(text("""
        SELECT to_regclass(:tbl)
    """), {"tbl": f"public.{table_name}"}).scalar()

    return result is not None
