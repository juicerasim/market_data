import websocket
import json
import time
import threading

from app.redis_client import redis_client
from app.binance.ws.handlers import kline_handler

BASE_URL = "wss://fstream.binance.com/ws"
REDIS_KEY = "liquid_coins"

ws_app = None
current_symbols = set()
request_id = 1


# -----------------------------
# Redis Symbols
# -----------------------------
def get_symbols():
    data = redis_client.get(REDIS_KEY)

    if not data:
        return set()

    coins = json.loads(data)
    return set(coins.keys())


# -----------------------------
# WS Events
# -----------------------------
def on_message(ws, message):
    msg = json.loads(message)

    # Binance sends control responses too
    if "data" in msg:
        kline_handler.handle(msg["data"])


def on_open(ws):
    print("WS Connected")

    # subscribe initial symbols
    subscribe_symbols(ws, current_symbols)


def on_error(ws, error):
    print("WS Error:", error)


def on_close(ws, a, b):
    print("WS Closed")


# -----------------------------
# Live Subscribe / Unsubscribe
# -----------------------------
def subscribe_symbols(ws, symbols):
    global request_id

    if not symbols:
        return

    params = [f"{s.lower()}@kline_1m" for s in symbols]

    payload = {
        "method": "SUBSCRIBE",
        "params": params,
        "id": request_id
    }

    request_id += 1

    print("SUBSCRIBE:", params)

    ws.send(json.dumps(payload))


def unsubscribe_symbols(ws, symbols):
    global request_id

    if not symbols:
        return

    params = [f"{s.lower()}@kline_1m" for s in symbols]

    payload = {
        "method": "UNSUBSCRIBE",
        "params": params,
        "id": request_id
    }

    request_id += 1

    print("UNSUBSCRIBE:", params[:5], "...")

    ws.send(json.dumps(payload))


# -----------------------------
# Symbol Watcher (NO RECONNECT)
# -----------------------------
def watch_symbols():
    global current_symbols, ws_app

    while True:
        time.sleep(15)

        new_symbols = get_symbols()

        if not ws_app:
            continue

        to_add = new_symbols - current_symbols
        to_remove = current_symbols - new_symbols

        if to_add:
            subscribe_symbols(ws_app, to_add)

        if to_remove:
            unsubscribe_symbols(ws_app, to_remove)

        if to_add or to_remove:
            print("Symbol update:",
                  "ADD", len(to_add),
                  "REMOVE", len(to_remove))

        current_symbols = new_symbols


# -----------------------------
# Run Socket
# -----------------------------
def run():
    global ws_app, current_symbols

    current_symbols = get_symbols()

    ws_app = websocket.WebSocketApp(
        BASE_URL,
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close
    )

    # watcher thread
    threading.Thread(target=watch_symbols, daemon=True).start()

    ws_app.run_forever()


if __name__ == "__main__":
    run()
