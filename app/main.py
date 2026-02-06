from app.repository.repository import upsert_kline_1m

sample_payload = {
    "e": "kline",
    "E": 1770054684442,
    "s": "BTCUSDT",
    "k": {
        "t": 1770054660000,
        "T": 1770054719999,
        "s": "BTCUSDT",
        "i": "1m",
        "f": 7182319799,
        "L": 7182322619,
        "o": "78828.70",
        "c": "78741.10",
        "h": "78828.80",
        "l": "78741.10",
        "v": "55.289",
        "n": 2820,
        "x": False,
        "q": "4355601.29060",
        "V": "21.488",
        "Q": "1692722.54870",
        "B": "0",
    },
}

upsert_kline_1m(sample_payload)
print("Kline saved")
