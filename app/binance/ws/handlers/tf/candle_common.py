from queue import Full
from app.binance.ws.queue import candle_queue


def handle(k, event_time):
    if not k["x"]:
        return

    payload = {
        "event_time": event_time,
        "symbol": k["s"],
        "open_time": k["t"],
        "interval": k["i"],
        "close_time": k["T"],
        "first_trade_id": k["f"],
        "last_trade_id": k["L"],
        "open_price": float(k["o"]),
        "high_price": float(k["h"]),
        "low_price": float(k["l"]),
        "close_price": float(k["c"]),
        "base_volume": float(k["v"]),
        "quote_volume": float(k["q"]),
        "taker_buy_base_volume": float(k["V"]),
        "taker_buy_quote_volume": float(k["Q"]),
        "trade_count": k["n"],
        "is_closed": k["x"],
    }

    try:
        candle_queue.put_nowait((k["i"], payload))
        print(f"[QUEUE] Added → {k['s']} {k['i']}")
    except Full:
        print(f"[QUEUE] FULL → Dropping {k['s']} {k['i']}")
