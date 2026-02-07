from sqlalchemy.dialects.postgresql import insert
from app.db import SessionLocal


from app.models import (
    Candle1M,
    Candle15M,
    Candle1H,
    Candle4H,
    Candle1D,
)

# ⭐ TF → ORM Model
CANDLE_MODEL_MAP = {
    "1m": Candle1M,
    "15m": Candle15M,
    "1h": Candle1H,
    "4h": Candle4H,
    "1d": Candle1D,
}


def insert_candle(tf: str, payload: dict):
    """
    Generic candle insert supporting all timeframes.
    Uses composite PK (symbol, open_time).
    Safe for websocket streaming.
    """

    model = CANDLE_MODEL_MAP.get(tf)

    if not model:
        raise ValueError(f"Unsupported timeframe: {tf}")

    db = SessionLocal()

    try:
        stmt = insert(model).values(**payload)

        # ⭐ Avoid duplicate insert crash
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["symbol", "open_time"]
        )

        db.execute(stmt)
        db.commit()

    finally:
        db.close()
