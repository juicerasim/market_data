from sqlalchemy.dialects.postgresql import insert
from app.db import SessionLocal
from app.models import Candle1M, Candle2M, Candle15M, Candle1H, Candle4H, Candle1D
from app.logging_config import get_logger

MODEL_MAP = {
    "1m": Candle1M,
    "2m": Candle2M,
    "15m": Candle15M,
    "1h": Candle1H,
    "4h": Candle4H,
    "1d": Candle1D,
}


def insert_candle(tf, payload):
    Model = MODEL_MAP.get(tf)
    if not Model:
        return
    logger = get_logger("market_data.binance.repo")
    logger.debug("payload", extra={"payload": payload})

    db = SessionLocal()
    logger.info("Inserting candle %s %s %s", payload.get("symbol"), tf, payload.get("open_time"))
    try:
        stmt = insert(Model).values(**payload)

        # ⭐ THIS LINE PREVENTS DUPLICATES
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "open_time"],
            set_={
                "event_time": stmt.excluded.event_time,
                "close_time": stmt.excluded.close_time,
                "open_price": stmt.excluded.open_price,
                "high_price": stmt.excluded.high_price,
                "low_price": stmt.excluded.low_price,
                "close_price": stmt.excluded.close_price,
                "base_volume": stmt.excluded.base_volume,
                "quote_volume": stmt.excluded.quote_volume,
                "taker_buy_base_volume": stmt.excluded.taker_buy_base_volume,
                "taker_buy_quote_volume": stmt.excluded.taker_buy_quote_volume,
                "trade_count": stmt.excluded.trade_count,
                "is_closed": stmt.excluded.is_closed,
            },
        )

        db.execute(stmt)
        db.commit()

    except Exception:
        db.rollback()
        logger.exception("UPSERT ERROR for payload", extra={"payload": payload})

    finally:
        db.close()
