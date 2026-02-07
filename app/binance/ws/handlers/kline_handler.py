from app.binance.ws.handlers.tf import tf_1m

# ⭐ TF → handler mapping
TF_MAP = {
    "1m": tf_1m.handle,
    # "5m": tf_5m.handle,
    # "15m": tf_15m.handle,
}


def handle(data):
    k = data["k"]

    tf = k["i"]

    handler = TF_MAP.get(tf)

    if handler:
        handler(k)
    else:
        print(f"No TF handler for {tf}")
