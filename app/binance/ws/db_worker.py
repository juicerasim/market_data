from app.binance.ws.queue import candle_queue
from app.binance.repo import insert_candle
import json


def run():
    print("🔥 DB Worker started...")

    while True:

        try:
            tf, payload = candle_queue.get()
            insert_candle(tf, payload)
        except Exception as e:
            print(f"Error inserting candle: {e}")


if __name__ == "__main__":
    run()
