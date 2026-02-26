import websocket
import json
import time
import threading
import signal

from app.logging_config import get_logger, setup_logging
from app.redis_client import redis_client
from app.binance.ws.handlers import kline_handler
from app.binance.ws.db_worker import run as db_run

BASE_URL = "wss://fstream.binance.com/ws"
REDIS_KEY = "liquid_coins"

ws_app = None
RUNNING = True

current_symbols_list = []
current_symbols_set = set()

# INTERVALS = ["1m", "15m", "1h", "4h", "1d"]
INTERVALS = ["1m"]
request_id = 1
setup_logging()
logger = get_logger("market_data.binance.ws")


def shutdown_handler(sig, frame):
    global RUNNING, ws_app
    logger.info("Shutdown signal received", extra={"signal": sig})
    RUNNING = False
    if ws_app:
        ws_app.close()


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def get_symbols():
    data = redis_client.get(REDIS_KEY)
    if not data:
        logger.debug("Redis has no symbols yet")
        return []

    coins = json.loads(data)
    logger.info("Loaded symbols from Redis: %d", len(coins))
    return coins[:2]


def subscribe(ws, symbols):
    global request_id

    if not symbols:
        logger.debug("No symbols to subscribe")
        return

    params = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in INTERVALS]

    logger.info("SUBSCRIBE → %d streams", len(params))

    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": params,
        "id": request_id
    }))

    request_id += 1


def unsubscribe(ws, symbols):
    global request_id

    params = [f"{s.lower()}@kline_{tf}" for s in symbols for tf in INTERVALS]

    logger.info("UNSUBSCRIBE → %d streams", len(params))

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
        logger.warning("Invalid JSON received")
        return

    if "result" in msg:
        print("[WS] Subscription ACK received")
        return

    if "e" not in msg:
        return

    # candle event received
    kline_handler.handle(msg)


def on_open(ws):
    global current_symbols_list, current_symbols_set

    logger.info("Connected to Binance")

    current_symbols_list = get_symbols()
    current_symbols_set = set(current_symbols_list)

    subscribe(ws, current_symbols_list)


def on_close(ws, a, b):
    logger.info("Connection closed: %s %s", a, b)


def watch_symbols():
    global current_symbols_list, current_symbols_set
    logger.info("Symbol watcher started")

    while RUNNING:
        time.sleep(15)

        new_list = get_symbols()
        new_set = set(new_list)

        to_add = new_set - current_symbols_set
        to_remove = current_symbols_set - new_set

        if to_add:
            logger.info("Adding %d symbols", len(to_add))
            subscribe(ws_app, list(to_add))

        if to_remove:
            logger.info("Removing %d symbols", len(to_remove))
            unsubscribe(ws_app, list(to_remove))

        current_symbols_list = new_list
        current_symbols_set = new_set


def run():
    global ws_app

    logger.info("Starting DB worker thread")
    threading.Thread(target=db_run, daemon=True).start()

    logger.info("Starting symbol watcher thread")
    threading.Thread(target=watch_symbols, daemon=True).start()

    while RUNNING:
        try:
            logger.info("Connecting → %s", BASE_URL)

            ws_app = websocket.WebSocketApp(
                BASE_URL,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
            )

            ws_app.run_forever(ping_interval=20, ping_timeout=10)

        except Exception:
            logger.exception("Connection error; retrying in 5s")
            time.sleep(5)

    logger.info("Engine stopped")


if __name__ == "__main__":
    logger.info("WS Engine booting...")
    run()
