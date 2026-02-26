from app.binance.scripts.helpers import open_time_ms_to_ist


def build_payloads(symbol, interval, klines, futures_data_map=None):
    rows = []

    for k in klines:

        open_time = k[0]

        # Futures data (if available)
        futures_row = (
            futures_data_map.get(open_time)
            if futures_data_map
            else {}
        )

        rows.append({
            "symbol": symbol,
            "interval": interval,
            "event_time": None,  # REST backfill

            "open_time": open_time,
            "lk_at": open_time_ms_to_ist(open_time),
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

            # -------------------------
            # Futures Fields
            # -------------------------
            "open_interest": (
                float(futures_row["open_interest"])
                if futures_row and futures_row.get("open_interest") is not None
                else None
            ),
            "oi_delta_percent": (
                float(futures_row["oi_delta_percent"])
                if futures_row and futures_row.get("oi_delta_percent") is not None
                else None
            ),
            "funding_rate": (
                float(futures_row["funding_rate"])
                if futures_row and futures_row.get("funding_rate") is not None
                else None
            ),
        })

    return rows