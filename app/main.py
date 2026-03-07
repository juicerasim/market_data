import subprocess
import sys
import signal
import time
from sqlalchemy import text

from app.logging_config import setup_logging, get_logger, install_exception_hook
from app.db import SessionLocal


RUNNING = True
processes = []


# ------------------------------------------------------
# Start worker
# ------------------------------------------------------
def start_worker(module):

    logger = get_logger("market_data.main")

    logger.info("Starting worker: %s", module)

    proc = subprocess.Popen(
        [sys.executable, "-m", module]
    )

    processes.append(proc)

    return proc


# ------------------------------------------------------
# Wait for symbols
# ------------------------------------------------------
def wait_for_symbols():

    logger = get_logger("market_data.main")

    logger.info("Waiting for symbols in DB...")

    while True:

        session = SessionLocal()

        try:

            count = session.execute(
                text("SELECT count(*) FROM symbols")
            ).scalar()

            if count and count > 0:

                logger.info("Symbols ready: %s", count)

                return

        finally:
            session.close()

        time.sleep(2)


# ------------------------------------------------------
# Shutdown handler
# ------------------------------------------------------
def shutdown_handler(sig, frame):

    global RUNNING

    logger = get_logger("market_data.main")

    logger.info("Shutdown signal received")

    RUNNING = False

    for p in processes:
        try:
            p.send_signal(signal.SIGINT)
        except Exception:
            pass

    for p in processes:
        try:
            p.wait(timeout=10)
        except Exception:
            pass

    logger.info("All workers stopped")

    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
def main():

    setup_logging()
    install_exception_hook()

    logger = get_logger("market_data.main")

    logger.info("Booting market-data pipeline")

    start_worker("app.binance.coins_with_liquidity")

    wait_for_symbols()

    start_worker("app.binance.scripts.kline_history")
    start_worker("app.binance.scripts.oi_sync")
    start_worker("app.binance.scripts.funding")
    # start_worker("app.binance.health.funding_health")

    logger.info("All workers started")

    try:

        while RUNNING:
            time.sleep(1)

    except KeyboardInterrupt:
        shutdown_handler(None, None)


if __name__ == "__main__":
    main()