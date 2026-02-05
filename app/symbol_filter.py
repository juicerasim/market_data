import requests

TICKER_24H_URL = "https://api.binance.com/api/v3/ticker/24hr"

MIN_QUOTE_VOL = 10_000_000      # 10M USDT
MIN_TRADES = 10_000
TOP_N = 50


def get_strong_symbols():
    resp = requests.get(TICKER_24H_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    filtered = []

    for item in data:
        symbol = item["symbol"]

        # only USDT perpetual spot-like pairs
        if not symbol.endswith("USDT"):
            continue

        quote_vol = float(item["quoteVolume"])
        trades = int(item["count"])
        status = item.get("status", "TRADING")

        if (
            quote_vol >= MIN_QUOTE_VOL
            and trades >= MIN_TRADES
            and status == "TRADING"
        ):
            filtered.append({
                "symbol": symbol.lower(),
                "quoteVolume": quote_vol
            })

    # sort by strongest volume
    filtered.sort(key=lambda x: x["quoteVolume"], reverse=True)

    return [x["symbol"] for x in filtered[:TOP_N]]
