import websocket
import json
import time
import threading
import signal

from app.redis_client import redis_client
from app.binance.ws.handlers import kline_handler
from app.binance.ws.db_worker import run as db_run

BASE_URL = "wss://fstream.binance.com/ws"
REDIS_KEY = "liquid_coins"

ws_app = None
RUNNING = True

current_symbols_list = []
current_symbols_set = set()

INTERVALS = ["1m", "15m", "1h", "4h", "1d"]
request_id = 1


def shutdown_handler(sig, frame):
    global RUNNING, ws_app
    print("[WS] Shutdown signal received")
    RUNNING = False
    if ws_app:
        ws_app.close()


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def get_symbols():
    data = redis_client.get(REDIS_KEY)
    if not data:
        print("[WS] Redis has no symbols yet")
        return []

    coins = json.loads(data)
    print(f"[WS] Loaded {len(coins)} symbols from Redis")
    return coins


def subscribe(ws, symbols):
    global request_id

    if not symbols:
        print("[WS] No symbols to subscribe")
        return

    params = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in INTERVALS]

    print(f"[WS] SUBSCRIBE → {len(params)} streams")

    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": params,
        "id": request_id
    }))

    request_id += 1


def unsubscribe(ws, symbols):
    global request_id

    params = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in INTERVALS]

    print(f"[WS] UNSUBSCRIBE → {len(params)} streams")

    ws.send(json.dumps({
        "method": "UNSUBSCRIBE",
        "params": params,
        "id": request_id
    }))

    request_id += 1


def on_message(ws, message):
    try:
        msg = json.loads(message)
    except Exception:
        print("[WS] Invalid JSON received")
        return

    if "result" in msg:
        print("[WS] Subscription ACK received")
        return

    if "e" not in msg:
        return

    # print("[WS] Candle event received")
    kline_handler.handle(msg)


def on_open(ws):
    global current_symbols_list, current_symbols_set

    print("[WS] Connected to Binance")

    current_symbols_list = get_symbols()
    current_symbols_set = set(current_symbols_list)

    subscribe(ws, current_symbols_list)


def on_close(ws, a, b):
    print("[WS] Connection closed")


def watch_symbols():
    global current_symbols_list, current_symbols_set

    print("[WS] Symbol watcher started")

    while RUNNING:
        time.sleep(15)

        new_list = get_symbols()
        new_set = set(new_list)

        to_add = new_set - current_symbols_set
        to_remove = current_symbols_set - new_set

        if to_add:
            print(f"[WS] Adding {len(to_add)} symbols")
            subscribe(ws_app, list(to_add))

        if to_remove:
            print(f"[WS] Removing {len(to_remove)} symbols")
            unsubscribe(ws_app, list(to_remove))

        current_symbols_list = new_list
        current_symbols_set = new_set


def run():
    global ws_app

    print("[WS] Starting DB worker thread")
    threading.Thread(target=db_run, daemon=True).start()

    print("[WS] Starting symbol watcher thread")
    threading.Thread(target=watch_symbols, daemon=True).start()

    while RUNNING:
        try:
            print(f"[WS] Connecting → {BASE_URL}")

            ws_app = websocket.WebSocketApp(
                BASE_URL,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
            )

            ws_app.run_forever(ping_interval=20, ping_timeout=10)

        except Exception as e:
            print("[WS] Connection error:", e)
            time.sleep(5)

    print("[WS] Engine stopped")


if __name__ == "__main__":
    print("[WS] WS Engine booting...")
    run()
