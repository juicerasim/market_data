from sqlalchemy.dialects.postgresql import insert
from app.db import engine
from app.models import (
    CDXCandle1M,
    CDXCandle15M,
    CDXCandle1H,
    CDXCandle4H,
    CDXCandle1D,
)

CDX_MODEL_MAP = {
    "1m": CDXCandle1M,
    "15m": CDXCandle15M,
    "1h": CDXCandle1H,
    "4h": CDXCandle4H,
    "1d": CDXCandle1D,
}


def upsert_klines(klines: list):
    """
    klines = list of payload dicts from websocket / aggregator
    """

    if not klines:
        return

    interval = klines[0]["duration"]
    model = CDX_MODEL_MAP.get(interval)

    if not model:
        return

    with engine.begin() as conn:
        for k in klines:

            stmt = insert(model).values(
                pair=k["pair"],
                symbol=k["symbol"],
                duration=k["duration"],

                open_time=int(k["open_time"]),
                close_time=int(k["close_time"]),

                open_price=float(k["open"]),
                high_price=float(k["high"]),
                low_price=float(k["low"]),
                close_price=float(k["close"]),

                base_volume=float(k["volume"]),
                quote_volume=float(k["quote_volume"]),

                is_closed=True,
            )

            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol", "open_time"],
                set_={
                    "close_price": stmt.excluded.close_price,
                    "high_price": stmt.excluded.high_price,
                    "low_price": stmt.excluded.low_price,
                    "base_volume": stmt.excluded.base_volume,
                    "quote_volume": stmt.excluded.quote_volume,
                },
            )

            conn.execute(stmt)
