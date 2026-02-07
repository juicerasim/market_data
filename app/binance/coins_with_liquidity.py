
"""
Worker: Binance Liquidity Snapshot

Fetch 24h futures ticker data, select top liquid coins,
and store snapshot in Redis (key: liquid_coins). update after evry 10 min.

Reads: Binance API
Writes: Redis
Runs: Background worker
"""

import requests
import time
from app.redis_client import redis_client
import json

BASE_URL = "https://fapi.binance.com"
ENDPOINT = "/fapi/v1/ticker/24hr"

session = requests.Session()


def fetch_all_market_tickers():
    url = BASE_URL + ENDPOINT
    response = session.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def get_top_liquid_coins(percent=0.20):
    tickers = fetch_all_market_tickers()

    parsed = []

    for t in tickers:
        if not t["symbol"].endswith("USDT"):
            continue

        parsed.append({
            "symbol": t["symbol"],
            "q": float(t["quoteVolume"])
        })

    parsed.sort(key=lambda x: x["q"], reverse=True)

    top_n = int(len(parsed) * percent)

    # ⭐ convert to {'BTC': q} format
    result = {
        c["symbol"].replace("USDT", ""): c["q"]
        for c in parsed[:top_n]
    }

    return result


if __name__ == "__main__":
    while True:
        coins = get_top_liquid_coins(0.30)

        redis_client.set("liquid_coins", json.dumps(coins))

        moving_coins = redis_client.get("liquid_coins")
        json_data_coins = json.loads(moving_coins)

        print("Top1 Liquid Coins:", json_data_coins)
        print("Total:", len(json_data_coins))

        keys = redis_client.keys("*")
        print(keys)

        time.sleep(600)
