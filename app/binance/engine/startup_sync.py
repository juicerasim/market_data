from sqlalchemy import text
from app.db import SessionLocal
from .time_utils import get_exchange_time_ms, TF_TO_MS, floor_time
from .htf_engine import try_build_htf
from app.binance.repo import insert_candle

def run_startup_sync():
    print("[SYNC] Startup sync started")

    exchange_now = get_exchange_time_ms()
    one_minute = 60_000

    db = SessionLocal()
    try:
        symbols = db.execute(text("""
            SELECT DISTINCT symbol FROM candles_1m
        """)).fetchall()

        symbols = [s[0] for s in symbols]

        for symbol in symbols:
            last_1m = db.execute(text("""
                SELECT MAX(open_time)
                FROM candles_1m
                WHERE symbol = :symbol
            """), {"symbol": symbol}).scalar()

            if not last_1m:
                continue

            expected_last = floor_time(exchange_now, one_minute) - one_minute

            if last_1m < expected_last:
                print(f"[SYNC] Gap detected for {symbol}")

                # REST backfill should be called here
                # Use your existing kline_history logic

        # Rebuild HTF
        for symbol in symbols:
            last_1m = db.execute(text("""
                SELECT MAX(open_time)
                FROM candles_1m
                WHERE symbol = :symbol
            """), {"symbol": symbol}).scalar()

            if not last_1m:
                continue

            for tf, tf_ms in TF_TO_MS.items():
                expected_htf = floor_time(last_1m, tf_ms)
                try_build_htf(symbol, tf, expected_htf)

    finally:
        db.close()

    print("[SYNC] Startup sync completed")
