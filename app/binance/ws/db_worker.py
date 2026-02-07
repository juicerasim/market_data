from app.binance.ws.queue import candle_queue
from app.binance.repo import insert_candle
import json


def run():
    print("🔥 DB Worker started...")

    while True:

        tf, payload = candle_queue.get()


        insert_candle(tf, payload)


if __name__ == "__main__":
    run()
