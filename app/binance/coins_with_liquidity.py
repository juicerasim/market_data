import requests
import time
import json
from app.redis_client import redis_client
from app.db import SessionLocal
from app.models import Symbol
from sqlalchemy.dialects.postgresql import insert

BASE_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
# BASE_URL = "https://api.delta.exchange/v2/tickers"
####################
#This is for binance
####################
def get_top_liquid_coins(percent=0.05):
    print("[LIQ] Fetching market tickers...")
    r = requests.get(BASE_URL, timeout=10)
    r.raise_for_status()

    parsed = []
    for t in r.json():
        symbol = t["symbol"]
        if symbol.endswith("USDT") and symbol.isascii():
            parsed.append((symbol, float(t["quoteVolume"])))

    parsed.sort(key=lambda x: x[1], reverse=True)
    top_n = int(len(parsed) * percent)

    result = [s for s, _ in parsed[:top_n]]
    print(f"[LIQ] Selected {len(result)} liquid symbols")
    print(f"[LIQ] Selected  liquid symbols: {result}")
    return result


####################
# Insert / Update symbols
####################
def upsert_symbols(symbols):
    db = SessionLocal()

    try:
        values = [{"name": s, "tier": 1} for s in symbols]

        stmt = insert(Symbol).values(values)

        stmt = stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={
                "updated_at": stmt.excluded.updated_at
            }
        )

        db.execute(stmt)
        db.commit()

        print(f"[LIQ] DB upsert completed for {len(symbols)} symbols")

    except Exception as e:
        db.rollback()
        print("[LIQ] DB ERROR:", e)

    finally:
        db.close()



if __name__ == "__main__":
    print("[LIQ] Liquidity worker started")

    while True:
        try:
            coins = get_top_liquid_coins()
            redis_client.set("liquid_coins", json.dumps(coins))
            print(f"[LIQ] Redis updated with {len(coins)} symbols")
            # update DB
            upsert_symbols(coins)
        except Exception as e:
            print("[LIQ] ERROR:", e)

        time.sleep(600)
        # time.sleep(5)
