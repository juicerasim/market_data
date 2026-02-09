import subprocess
import sys
import signal
import time

WORKERS = [
    "app.binance.coins_with_liquidity",
    "app.binance.ws.ws_engine",
]


def start(module):
    print(f"[MAIN] Starting worker → {module}")
    return subprocess.Popen([sys.executable, "-m", module])


def main():
    print("[MAIN] Booting market-data pipeline...")
    processes = [start(w) for w in WORKERS]

    print("[MAIN] All workers started")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[MAIN] Shutdown requested")
        for p in processes:
            p.send_signal(signal.SIGINT)
        for p in processes:
            p.wait()

        print("[MAIN] Shutdown complete")


if __name__ == "__main__":
    main()
