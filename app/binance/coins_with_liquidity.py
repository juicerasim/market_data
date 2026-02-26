import requests
import time
import json
from app.redis_client import redis_client

BASE_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
# BASE_URL = "https://api.delta.exchange/v2/tickers"
####################
#This is for binance
####################
def get_top_liquid_coins(percent=0.05):
    print("[LIQ] Fetching market tickers...")
    r = requests.get(BASE_URL, timeout=10)
    r.raise_for_status()

    parsed = []
    for t in r.json():
        symbol = t["symbol"]
        if symbol.endswith("USDT") and symbol.isascii():
            parsed.append((symbol, float(t["quoteVolume"])))

    parsed.sort(key=lambda x: x[1], reverse=True)
    top_n = int(len(parsed) * percent)

    result = [s for s, _ in parsed[:top_n]]
    print(f"[LIQ] Selected {len(result)} liquid symbols")
    print(f"[LIQ] Selected  liquid symbols: {result}")
    return result




if __name__ == "__main__":
    print("[LIQ] Liquidity worker started")

    while True:
        try:
            coins = get_top_liquid_coins()
            redis_client.set("liquid_coins", json.dumps(coins))
            print(f"[LIQ] Redis updated with {len(coins)} symbols")
        except Exception as e:
            print("[LIQ] ERROR:", e)

        time.sleep(600)
