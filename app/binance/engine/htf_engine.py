from sqlalchemy import text
from app.db import SessionLocal
from app.binance.repo import insert_candle
from .time_utils import TF_TO_MS, floor_time
from app.binance.scripts.helpers import open_time_ms_to_ist

HTF_LIST = ["15m", "1h", "4h", "1d"]


def handle_new_1m(symbol: str, open_time: int):
    for tf in HTF_LIST:
        try_build_htf(symbol, tf, open_time)


def try_build_htf(symbol: str, tf: str, last_1m_open: int):
    tf_ms = TF_TO_MS[tf]

    # boundary check
    bucket_start = floor_time(last_1m_open, tf_ms)
    bucket_end = bucket_start + tf_ms - 60_000

    if last_1m_open != bucket_end:
        return

    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT open_time, open_price, high_price,
                   low_price, close_price,
                   base_volume, quote_volume,
                   taker_buy_base_volume,
                   taker_buy_quote_volume,
                   trade_count
            FROM candles_1m
            WHERE symbol = :symbol
            AND open_time BETWEEN :start AND :end
            ORDER BY open_time ASC
        """), {
            "symbol": symbol,
            "start": bucket_start,
            "end": bucket_end,
        }).fetchall()

        expected_count = tf_ms // 60_000

        if len(rows) != expected_count:
            return

        open_price = rows[0].open_price
        close_price = rows[-1].close_price
        high_price = max(r.high_price for r in rows)
        low_price = min(r.low_price for r in rows)

        base_volume = sum(r.base_volume for r in rows)
        quote_volume = sum(r.quote_volume for r in rows)
        taker_buy_base_volume = sum(r.taker_buy_base_volume for r in rows)
        taker_buy_quote_volume = sum(r.taker_buy_quote_volume for r in rows)
        trade_count = sum(r.trade_count for r in rows)

        payload = {
            "event_time": bucket_end,
            "symbol": symbol,
            "interval": tf,
            "open_time": bucket_start,
            "close_time": bucket_end + 59_999,
            "first_trade_id": 0,
            "last_trade_id": 0,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "base_volume": base_volume,
            "quote_volume": quote_volume,
            "taker_buy_base_volume": taker_buy_base_volume,
            "taker_buy_quote_volume": taker_buy_quote_volume,
            "trade_count": trade_count,
            "is_closed": True,
        }

        insert_candle(tf, payload)

    finally:
        db.close()
