from app.binance.scripts.helpers import open_time_ms_to_ist


def build_payloads(symbol, interval, klines):
    rows = []

    for k in klines:

        open_time = k[0]

        rows.append({
            "symbol": symbol,
            "interval": interval,
            "event_time": None,  # REST backfill

            "open_time": open_time,
            "lk_at": open_time_ms_to_ist(open_time),
            "close_time": k[6],

            "first_trade_id": None,
            "last_trade_id": None,

            # OHLC
            "open_price": float(k[1]),
            "high_price": float(k[2]),
            "low_price": float(k[3]),
            "close_price": float(k[4]),

            # Volumes
            "base_volume": float(k[5]),
            "quote_volume": float(k[7]),
            "taker_buy_base_volume": float(k[9]),
            "taker_buy_quote_volume": float(k[10]),

            # Stats
            "trade_count": int(k[8]),
            "is_closed": True,
        })

    return rows