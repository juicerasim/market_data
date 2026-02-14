import requests
from datetime import datetime, timezone

BASE_URL = "https://fapi.binance.com"


def get_exchange_time_ms():
    r = requests.get(f"{BASE_URL}/fapi/v1/time", timeout=5)
    r.raise_for_status()
    return r.json()["serverTime"]


def floor_time(ts_ms: int, tf_ms: int) -> int:
    return (ts_ms // tf_ms) * tf_ms


TF_TO_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}
