import websocket
import json
import time
import threading

from app.redis_client import redis_client
from app.binance.ws.handlers import kline_handler
from app.binance.ws.db_worker import run as db_run

BASE_URL = "wss://fstream.binance.com/ws"
REDIS_KEY = "liquid_coins"

ws_app = None

# ⭐ maintain both list + set
current_symbols_list = []
current_symbols_set = set()

# Timeframes to subscribe to
INTERVALS = ["1m", "15m", "1h", "4h", "1d"]

request_id = 1


# --------------------------------------------------
# Load symbols from Redis (ORDER PRESERVED)
# --------------------------------------------------
def get_symbols():
    data = redis_client.get(REDIS_KEY)

    if not data:
        return []

    coins = json.loads(data)

    # safety if single string pushed
    if isinstance(coins, str):
        return [coins]

    return coins


# --------------------------------------------------
# Subscribe helpers
# --------------------------------------------------
def subscribe_symbols(ws, symbols):
    global request_id

    if isinstance(symbols, str):
        symbols = [symbols]

    if not symbols:
        return

    params = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in INTERVALS]

    payload = {
        "method": "SUBSCRIBE",
        "params": params,
        "id": request_id
    }

    request_id += 1

    print("Sending SUBSCRIBE:", params[:20], "... total:", len(params))

    ws.send(json.dumps(payload))


def unsubscribe_symbols(ws, symbols):
    global request_id

    if isinstance(symbols, str):
        symbols = [symbols]

    if not symbols:
        return

    params = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in INTERVALS]

    payload = {
        "method": "UNSUBSCRIBE",
        "params": params,
        "id": request_id
    }

    request_id += 1

    print("Sending UNSUBSCRIBE:", params[:20])

    ws.send(json.dumps(payload))


# --------------------------------------------------
# WS EVENTS
# --------------------------------------------------
def on_message(ws, message):
    try:
        # ⚠️ SECURITY: Validate JSON parsing with error handling
        msg = json.loads(message)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON received: {e}")
        return
    except Exception as e:
        print(f"Unexpected error parsing message: {e}")
        return

    # Ignore subscribe responses
    if "result" in msg:
        return

    # RAW kline stream sends payload directly
    if "e" not in msg:
        return

    try:
        kline_handler.handle(msg)
    except Exception as e:
        print(f"Error handling kline message: {e}")


def on_open(ws):
    global current_symbols_list, current_symbols_set

    print("WS Connected")

    current_symbols_list = get_symbols()
    current_symbols_set = set(current_symbols_list)

    print("Initial symbols:", current_symbols_list[:20])

    subscribe_symbols(ws, current_symbols_list)


def on_error(ws, error):
    print("WS Error:", error)


def on_close(ws, a, b):
    print("WS Closed")


# --------------------------------------------------
# Watch Redis for Symbol Updates
# --------------------------------------------------
def watch_symbols():
    global current_symbols_list, current_symbols_set, ws_app

    while True:
        time.sleep(15)

        if not ws_app:
            continue

        new_list = get_symbols()
        new_set = set(new_list)

        to_add = new_set - current_symbols_set
        to_remove = current_symbols_set - new_set

        if to_add:
            subscribe_symbols(ws_app, list(to_add))

        if to_remove:
            unsubscribe_symbols(ws_app, list(to_remove))

        if to_add or to_remove:
            print(
                "Symbol Update → ADD:",
                len(to_add),
                "REMOVE:",
                len(to_remove)
            )

        current_symbols_list = new_list
        current_symbols_set = new_set


# --------------------------------------------------
# RUN SOCKET
# --------------------------------------------------
def run():
    global ws_app

    # ⭐ IMPORTANT FIX:
    # Start DB worker as THREAD (same process)
    threading.Thread(target=db_run, daemon=True).start()

    # Watch Redis for updates
    threading.Thread(target=watch_symbols, daemon=True).start()

    # ⭐ RECONNECTION WITH 5 SECOND DELAY
    reconnect_delay = 5
    max_retries = 10
    retry_count = 0

    while True:
        try:
            print(f"[WS] Connecting to {BASE_URL}...")
            
            ws_app = websocket.WebSocketApp(
                BASE_URL,
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close
            )

            retry_count = 0  # Reset on successful connection
            ws_app.run_forever()

        except KeyboardInterrupt:
            # ⭐ ALLOW CTRL+C TO WORK
            print("\n[WS] Shutdown signal received. Closing...")
            if ws_app:
                ws_app.close()
            break

        except Exception as e:
            print(f"[WS] Connection error: {e}")
            
            retry_count += 1
            if retry_count > max_retries:
                print(f"[WS] Max retries ({max_retries}) exceeded. Stopping.")
                break

            print(f"[WS] Reconnecting in {reconnect_delay} seconds... (attempt {retry_count}/{max_retries})")
            time.sleep(reconnect_delay)
            time.sleep(reconnect_delay)


if __name__ == "__main__":
    run()
