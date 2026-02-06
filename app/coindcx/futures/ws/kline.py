import socketio
import json
from app.coindcx.futures.instruments import INSTRUMENTS
from app.repository.cdx_repo import insert_cdx_candle

socketEndpoint = "wss://stream.coindcx.com"
TF = "1m"

# ⭐ STATE STORE (symbol-wise)
CANDLE_STATE = {}

sio = socketio.Client(
    logger=True,
    engineio_logger=False
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

    open_time = candle["open_time"]  # ⭐ keep as BIGINT

    # ⭐ Initialize state for new symbol
    if symbol not in CANDLE_STATE:
        CANDLE_STATE[symbol] = {
            "last_open_time": open_time,
            "last_candle": candle
        }
        return

    last_open = CANDLE_STATE[symbol]["last_open_time"]

    # ⭐ NEW CANDLE ARRIVED -> PREVIOUS IS CLOSED
    if open_time != last_open:
        closed_candle = CANDLE_STATE[symbol]["last_candle"]

        print("✅ Previous candle CLOSED")
        print(closed_candle)

        # ⭐ GENERIC INSERT
        insert_cdx_candle(closed_candle, is_closed=True)

        CANDLE_STATE[symbol]["last_open_time"] = open_time

    # always update latest candle snapshot
    CANDLE_STATE[symbol]["last_candle"] = candle


@sio.on("*")
def catch_all(event, data):
    print(f"\n🔹 EVENT: {event}")


@sio.event
def disconnect():
    print("❌ Disconnected")


def main():
    sio.connect(socketEndpoint, transports=["websocket"])
    sio.wait()


if __name__ == "__main__":
    main()
