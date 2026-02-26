import subprocess
import sys
import signal
import time
import threading

from app.logging_config import setup_logging, get_logger, install_exception_hook
from app.binance.engine.startup_sync import run_startup_sync
from app.binance.engine.gap_watchdog import run_gap_watchdog

# ------------------------------------------------------
# Workers to spawn as separate processes
# ------------------------------------------------------
WORKERS = [
    "app.binance.coins_with_liquidity",
    "app.binance.ws.ws_engine",
]

processes = []
RUNNING = True


# ------------------------------------------------------
# Start subprocess worker
# ------------------------------------------------------
def start_worker(module: str):
    logger = get_logger("market_data.main")
    logger.info("Starting worker %s", module)
    return subprocess.Popen([sys.executable, "-m", module])


# ------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------
def shutdown_handler(sig, frame):
    global RUNNING
    logger = get_logger("market_data.main")
    logger.info("Shutdown signal received: %s", sig)
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
# MAIN BOOT FLOW
# ------------------------------------------------------
def main():
    global processes

    setup_logging()
    install_exception_hook()
    logger = get_logger("market_data.main")

    logger.info("Booting market-data pipeline")

    # ---------------------------------------------
    # PHASE 1 — STARTUP SYNC
    
    try:
        logger.info("Running startup sync...")
        run_startup_sync()
        logger.info("Startup sync completed")
    except Exception:
        logger.exception("Startup sync failed; continuing with live mode")

    # ---------------------------------------------
    # PHASE 3 — GAP WATCHDOG (background thread)
    # ---------------------------------------------
    logger.info("Starting gap watchdog thread")
    watchdog_thread = threading.Thread(
        target=run_gap_watchdog,
        daemon=True,
    )
    watchdog_thread.start()

    # ---------------------------------------------
    # PHASE 2 — START WORKERS
    # ---------------------------------------------
    logger.info("Starting workers")

    processes = [start_worker(w) for w in WORKERS]

    logger.info("All workers started successfully")

    # ---------------------------------------------
    # Keep main process alive
    # ---------------------------------------------
    try:
        while RUNNING:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown_handler(None, None)


if __name__ == "__main__":
    main()
