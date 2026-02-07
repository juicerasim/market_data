import time
from app.binance.repo import insert_candle
from app.binance.ws.queue import candle_queue


def run():
    print("DB Worker Started")

    while True:
        try:
            tf, payload = candle_queue.get(timeout=1)
            insert_candle(tf, payload)

        except Exception:
            time.sleep(0.1)
