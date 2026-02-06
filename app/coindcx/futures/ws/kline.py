import socketio
import json
import time

from app.coindcx.futures.instruments import INSTRUMENTS
from app.coindcx.futures.ws.db_worker import CDX_DB_QUEUE, start_db_worker

socketEndpoint = "wss://stream.coindcx.com"
TF = "1m"

# ⭐ STATE STORE (symbol-wise)
CANDLE_STATE = {}

sio = socketio.Client(
    logger=True,
    engineio_logger=False,
    reconnection=True,
    reconnection_attempts=0,
    reconnection_delay=3,
)


@sio.event
def connect():
    print("✅ Connected!")

    for instrument in INSTRUMENTS:
        channel = f"{instrument}_{TF}-futures"
        print(channel)

        sio.emit("join", {
            "channelName": channel
        })


@sio.on("candlestick")
def on_candlestick(response):

    data = response.get("data")

    if isinstance(data, str):
        data = json.loads(data)

    candle = data["data"][0]
    symbol = candle["pair"]

    open_time = candle["open_time"]

    # ⭐ Initialize state
    if symbol not in CANDLE_STATE:
        CANDLE_STATE[symbol] = {
            "last_open_time": open_time,
            "last_candle": candle
        }
        return

    last_open = CANDLE_STATE[symbol]["last_open_time"]

    # ⭐ Candle closed detection
    if open_time != last_open:
        closed_candle = CANDLE_STATE[symbol]["last_candle"]

        print("✅ Previous candle CLOSED")
        print(closed_candle)

        # ⭐ Non-blocking queue push
        try:
            CDX_DB_QUEUE.put_nowait((closed_candle, True))
        except:
            print("⚠️ DB queue full, candle dropped:", symbol)

        CANDLE_STATE[symbol]["last_open_time"] = open_time

    # update latest snapshot
    CANDLE_STATE[symbol]["last_candle"] = candle


@sio.on("*")
def catch_all(event, data):
    print(f"\n🔹 EVENT: {event}")


@sio.event
def disconnect():
    print("❌ Disconnected")


def main():
    start_db_worker()

    while True:
        try:
            if not sio.connected:
                print("🔌 Connecting to CoinDCX stream...")
                sio.connect(socketEndpoint, transports=["websocket"])

            sio.wait()

        except Exception as e:
            print("❌ Connection error:", e)

        print("⏳ Reconnecting in 5 seconds...")
        time.sleep(5)


if __name__ == "__main__":
    main()
