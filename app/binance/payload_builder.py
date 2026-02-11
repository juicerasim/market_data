def build_payloads(symbol, interval, klines):
    rows = []

    for k in klines:
        rows.append({
            "symbol": symbol,
            "interval": interval,
            "event_time": None,      # REST backfill
            "open_time": k[0],
            "close_time": k[6],

            "first_trade_id": None,
            "last_trade_id": None,

            "open_price": float(k[1]),
            "high_price": float(k[2]),
            "low_price": float(k[3]),
            "close_price": float(k[4]),

            "base_volume": float(k[5]),
            "quote_volume": float(k[7]),

            "taker_buy_base_volume": float(k[9]),
            "taker_buy_quote_volume": float(k[10]),

            "trade_count": k[8],
            "is_closed": True,
        })

    return rows
