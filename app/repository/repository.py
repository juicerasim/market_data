from sqlalchemy.dialects.postgresql import insert
from app.db import engine
from app import models


MODEL_MAP = {
    "1m": models.Candle1M,
    "15m": models.Candle15M,
    "1h": models.Candle1H,
    "4h": models.Candle4H,
    "1d": models.Candle1D,
}


def upsert_klines(symbol: str, interval: str, klines: list):
    model = MODEL_MAP[interval]

    with engine.begin() as conn:
        for k in klines:
            stmt = insert(model).values(
                id=k[0],
                event_time=k[6],
                symbol=symbol,
                open_time=k[0],
                close_time=k[6],
                open_price=float(k[1]),
                high_price=float(k[2]),
                low_price=float(k[3]),
                close_price=float(k[4]),
                base_volume=float(k[5]),
                quote_volume=float(k[7]),
                trade_count=int(k[8]),
                is_closed=True,
            ).on_conflict_do_update(
                index_elements=["symbol", "open_time"],
                set_={
                    "close_price": stmt.excluded.close_price,
                    "high_price": stmt.excluded.high_price,
                    "low_price": stmt.excluded.low_price,
                    "base_volume": stmt.excluded.base_volume,
                    "quote_volume": stmt.excluded.quote_volume,
                    "trade_count": stmt.excluded.trade_count,
                },
            )
            conn.execute(stmt)
