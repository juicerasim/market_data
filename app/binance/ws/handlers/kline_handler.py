from app.binance.ws.handlers.tf import tf_1m

# ⭐ TF → handler mapping
TF_MAP = {
    "1m": tf_1m.handle,
}


def handle(data):
    # Binance kline payload
    k = data["k"]
    event_time = data["E"]

    tf = k["i"]

    handler = TF_MAP.get(tf)

    if handler:
        handler(k, event_time)   # ⭐ PASS BOTH PARAMS
    else:
        print(f"No TF handler for {tf}")
