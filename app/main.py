import subprocess
import sys
import signal
import time

WORKERS = [
    "app.binance.coins_with_liquidity",
]

def start_worker(module_name):
    print(f"Starting worker: {module_name}")
    return subprocess.Popen([sys.executable, "-m", module_name])

def main():
    processes = [start_worker(w) for w in WORKERS]

    print("All workers started.")

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping all workers...")

        for p in processes:
            p.send_signal(signal.SIGINT)

        for p in processes:
            p.wait()

        print("Shutdown complete.")

if __name__ == "__main__":
    main()
