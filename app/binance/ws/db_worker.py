import signal
from queue import Empty

from app.binance.ws.queue import candle_queue
from app.binance.repo import insert_candle

RUNNING = True


def shutdown_handler(sig, frame):
    global RUNNING
    print("[DB] Shutdown signal received")
    RUNNING = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def run():
    print("[DB] Worker started")

    while RUNNING:
        try:
            tf, payload = candle_queue.get(timeout=1)
            print(f"[DB] Inserting candle → {payload['symbol']} {tf}")
            insert_candle(tf, payload)
        except Empty:
            continue
        except Exception as e:
            print("[DB] ERROR:", e)

    print("[DB] Worker stopped")
