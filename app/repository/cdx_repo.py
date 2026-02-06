from sqlalchemy.exc import IntegrityError
from app.db import SessionLocal
from app.models import CDXCandle1M

# duration → model map
CDX_MODEL_MAP = {
    "1m": CDXCandle1M,
}


def insert_cdx_candle(payload: dict, is_closed: bool):

    duration = payload.get("duration")
    model_cls = CDX_MODEL_MAP.get(duration)

    if not model_cls:
        print(f"⚠️ Unsupported duration: {duration}")
        return None

    session = SessionLocal()

    try:
        obj = model_cls(
            pair=payload["pair"],
            symbol=payload["symbol"],
            duration=duration,

            open_time=int(payload["open_time"]),
            close_time=int(payload["close_time"]),

            open_price=float(payload["open"]),
            high_price=float(payload["high"]),
            low_price=float(payload["low"]),
            close_price=float(payload["close"]),

            base_volume=float(payload["volume"]),
            quote_volume=float(payload["quote_volume"]),

            is_closed=is_closed
        )

        session.add(obj)
        session.commit()

        print("💾 INSERTED:", payload["pair"], payload["open_time"])
        return obj

    except IntegrityError:
        session.rollback()
        print("⚠️ Duplicate candle ignored")

    except Exception as e:
        session.rollback()
        print("❌ DB error:", e)
        raise

    finally:
        session.close()
