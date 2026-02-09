from app.binance.ws.handlers.tf import candle_common

TF_MAP = {
    "1m": candle_common.handle,
    "15m": candle_common.handle,
    "1h": candle_common.handle,
    "4h": candle_common.handle,
    "1d": candle_common.handle,
}


def handle(data):
    k = data["k"]
    event_time = data["E"]
    tf = k["i"]

    handler = TF_MAP.get(tf)
    if handler:
        handler(k, event_time)
    else:
        print(f"No handler for TF {tf}")
