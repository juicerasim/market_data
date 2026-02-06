import socketio
import json
from app.coindcx.futures.instruments import INSTRUMENTS

socketEndpoint = "wss://stream.coindcx.com"
TF = "5m" 

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
    print("\n📊 CANDLE EVENT RECEIVED")

    data = response.get("data")

    if isinstance(data, str):
        data = json.loads(data)

    # print(data['pair'])
    print(data['data'][0]['pair'])
    print()


@sio.on("*")
def catch_all(event, data):
    print(f"\n🔹 EVENT: {event}")



@sio.event
def disconnect():
    print("❌ Disconnected")


def main():
    try:
        sio.connect(socketEndpoint, transports=["websocket"])
        sio.wait()
    except Exception as e:
        print("Error connecting:", e)
        raise


if __name__ == "__main__":
    main()
