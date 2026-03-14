import numpy as np
from sqlalchemy import text
from app.db import SessionLocal


# ------------------------------------------------
# PARAMETERS (tuned for crypto markets)
# ------------------------------------------------

PRICE_MOVE_THRESHOLD = 1.5     # %
ATR_PERIOD = 14
VOLUME_SPIKE = 1.2
CANDLE_LIMIT = 50
MOMENTUM_LOOKBACK = 6
DISPLACEMENT_MULT = 1.2
MIN_SCORE = 2


# ------------------------------------------------
# Fetch candles
# ------------------------------------------------

def fetch_candles(db, table, symbol):

    rows = db.execute(text(f"""
        SELECT close_price, high_price, low_price, base_volume
        FROM {table}
        WHERE symbol = :symbol
        ORDER BY open_time DESC
        LIMIT :limit
    """), {"symbol": symbol, "limit": CANDLE_LIMIT}).fetchall()

    return rows[::-1]


# ------------------------------------------------
# Price momentum
# ------------------------------------------------

def price_move(rows):

    first = rows[-MOMENTUM_LOOKBACK].close_price
    last = rows[-1].close_price

    return (last - first) / first * 100


def ema(values, period):
    alpha = 2 / (period + 1)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = alpha * v + (1 - alpha) * ema_val
    return ema_val

# ------------------------------------------------
# ATR calculation
# ------------------------------------------------

def compute_atr(rows):

    highs = np.array([r.high_price for r in rows])
    lows = np.array([r.low_price for r in rows])
    closes = np.array([r.close_price for r in rows])

    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1])
        )
    )

    atr_now = ema(tr[-ATR_PERIOD:], ATR_PERIOD)
    atr_prev = ema(tr[-ATR_PERIOD-1:-1], ATR_PERIOD)

    return atr_now, atr_prev


# ------------------------------------------------
# ATR expansion
# ------------------------------------------------

def atr_expansion(rows):

    atr_now, atr_prev = compute_atr(rows)

    return atr_now > atr_prev, atr_now


# ------------------------------------------------
# Volume spike
# ------------------------------------------------

def volume_spike(rows):

    vols = [r.base_volume for r in rows]

    avg = np.mean(vols[-10:-1])
    last = vols[-1]

    return last > VOLUME_SPIKE * avg


# ------------------------------------------------
# Displacement candle
# ------------------------------------------------

def displacement(rows, atr):

    last = rows[-1]

    candle_range = last.high_price - last.low_price

    return candle_range > DISPLACEMENT_MULT * atr


# ------------------------------------------------
# Market scanner
# ------------------------------------------------

def run_scanner():

    with SessionLocal() as db:

        rows = db.execute(text("""
            SELECT DISTINCT symbol
            FROM candles_1h
        """)).fetchall()

        symbols = [r[0] for r in rows]

        print(f"[SCAN] scanning {len(symbols)} symbols")

        watchlist = []

        for symbol in symbols:

            try:

                candles_1h = fetch_candles(db, "candles_1h", symbol)
                candles_15m = fetch_candles(db, "candles_15m", symbol)

                if len(candles_1h) < 30 or len(candles_15m) < 30:
                    continue

                move = price_move(candles_1h)

                atr_up, atr_now = atr_expansion(candles_1h)

                vol_spike = volume_spike(candles_15m)

                disp = displacement(candles_1h, atr_now)

                # -----------------------------------
                # score system
                # -----------------------------------

                score = 0

                if abs(move) >= PRICE_MOVE_THRESHOLD:
                    score += 1

                if atr_up:
                    score += 1

                if vol_spike:
                    score += 1

                if disp:
                    score += 1

                print(
                    f"{symbol} | move={move:.2f}% "
                    f"| atr_up={atr_up} "
                    f"| vol_spike={vol_spike} "
                    f"| disp={disp} "
                    f"| score={score}"
                )

                if score >= MIN_SCORE:
                    watchlist.append(symbol)

            except Exception as e:

                print(f"[ERROR] {symbol}: {e}")
                continue

        print("\n[SCAN RESULT]")
        print("----------------")

        for s in watchlist:
            print(s)

        return watchlist


# ------------------------------------------------
# Entry point
# ------------------------------------------------

if __name__ == "__main__":
    run_scanner()