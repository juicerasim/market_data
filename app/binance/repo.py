from app.db import SessionLocal
from app.models import Candle1M, Candle15M, Candle1H, Candle4H, Candle1D


MODEL_MAP = {
    "1m": Candle1M,
    "15m": Candle15M,
    "1h": Candle1H,
    "4h": Candle4H,
    "1d": Candle1D,
}


def insert_candle(tf, payload):
    print("====1==payload========")
    print(payload)
    print("====1==========")
    Model = MODEL_MAP.get(tf)

    if not Model:
        return

    db = SessionLocal()

    try:
        obj = Model(**payload)

        db.merge(obj)  # ⭐ UPSERT SAFE (composite key)
        db.commit()

    except Exception as e:
        db.rollback()
        print("Insert failed:", e)

    finally:
        db.close()
