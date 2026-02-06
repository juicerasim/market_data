TF_BUCKET = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

AGG_STATE = {}


def get_bucket_open(open_time, minutes):
    return open_time - (open_time % (minutes * 60))


def aggregate(symbol, candle):

    results = []

    for tf, size in TF_BUCKET.items():

        bucket_open = get_bucket_open(candle["open_time"], size)
        key = (symbol, tf)

        if key not in AGG_STATE:
            AGG_STATE[key] = {
                "open_time": bucket_open,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle["volume"]),
                "quote_volume": float(candle["quote_volume"]),
            }
            continue

        agg = AGG_STATE[key]

        if agg["open_time"] != bucket_open:

            closed = {
                "pair": candle["pair"],
                "symbol": candle["symbol"],
                "duration": tf,
                "open_time": agg["open_time"],
                "close_time": agg["open_time"] + size * 60 - 1,
                "open": agg["open"],
                "high": agg["high"],
                "low": agg["low"],
                "close": agg["close"],
                "volume": agg["volume"],
                "quote_volume": agg["quote_volume"],
            }

            results.append(closed)

            AGG_STATE[key] = {
                "open_time": bucket_open,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle["volume"]),
                "quote_volume": float(candle["quote_volume"]),
            }

        else:
            agg["high"] = max(agg["high"], float(candle["high"]))
            agg["low"] = min(agg["low"], float(candle["low"]))
            agg["close"] = float(candle["close"])
            agg["volume"] += float(candle["volume"])
            agg["quote_volume"] += float(candle["quote_volume"])

    return results
