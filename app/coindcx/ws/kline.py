import socketio
import json

# -----------------------------------
# CONFIG
# -----------------------------------
socketEndpoint = "wss://stream.coindcx.com"

# Example:
# instrument_name = B-BTC_USDT
# resolutions = 1m, 5m, 1h, 1d etc
CHANNEL_NAME = "B-BTC_USDT_1m-futures"

# -----------------------------------
# SOCKET CLIENT
# -----------------------------------
sio = socketio.Client(
    logger=True,           # helpful logs
    engineio_logger=False
)


# -----------------------------------
# CONNECT
# -----------------------------------
@sio.event
def connect():
    print("✅ Connected!")

    # Join candlestick channel
    sio.emit("join", {
        "channelName": CHANNEL_NAME
    })


# -----------------------------------
# CANDLESTICK EVENT
# -----------------------------------
@sio.on("candlestick")
def on_candlestick(response):
    print("\n📊 CANDLE EVENT RECEIVED")

    data = response.get("data")

    # CoinDCX sometimes sends JSON string
    if isinstance(data, str):
        data = json.loads(data)

    print(data[''])


# -----------------------------------
# DEBUG: Catch ALL Events (optional but VERY useful)
# -----------------------------------
@sio.on("*")
def catch_all(event, data):
    print(f"\n🔹 EVENT: {event}")


# -----------------------------------
# DISCONNECT
# -----------------------------------
@sio.event
def disconnect():
    print("❌ Disconnected")


# -----------------------------------
# MAIN
# -----------------------------------
def main():
    try:
        sio.connect(socketEndpoint, transports=["websocket"])
        sio.wait()   # keep connection alive
    except Exception as e:
        print("Error connecting:", e)
        raise


if __name__ == "__main__":
    main()
