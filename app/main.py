import subprocess
import sys
import signal
import time
import threading

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
    print(f"[MAIN] Starting worker → {module}")
    return subprocess.Popen([sys.executable, "-m", module])


# ------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------
def shutdown_handler(sig, frame):
    global RUNNING
    print("\n[MAIN] Shutdown signal received")
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

    print("[MAIN] All workers stopped")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ------------------------------------------------------
# MAIN BOOT FLOW
# ------------------------------------------------------
def main():
    global processes

    print("[MAIN] Booting market-data pipeline")

    # ---------------------------------------------
    # PHASE 1 — STARTUP SYNC
    
    try:
        print("[MAIN] Running startup sync...")
        run_startup_sync()
        print("[MAIN] Startup sync completed\n")
    except Exception as e:
        print("[MAIN] Startup sync failed:", e)
        print("[MAIN] Continuing with live mode...\n")

    # ---------------------------------------------
    # PHASE 3 — GAP WATCHDOG (background thread)
    # ---------------------------------------------
    print("[MAIN] Starting gap watchdog thread...")
    watchdog_thread = threading.Thread(
        target=run_gap_watchdog,
        daemon=True,
    )
    watchdog_thread.start()

    # ---------------------------------------------
    # PHASE 2 — START WORKERS
    # ---------------------------------------------
    print("[MAIN] Starting workers...\n")

    processes = [start_worker(w) for w in WORKERS]

    print("\n[MAIN] All workers started successfully\n")

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
