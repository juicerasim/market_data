import pandas as pd
import numpy as np
import logging
from sqlalchemy import text
from app.db import SessionLocal


# =============================
# LOGGING (Clean Output)
# =============================

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


# =============================
# CONFIG
# =============================

LOOKBACK = 200
COMPRESSION_PERIOD = 20
ATR_PERIOD = 14
VOLUME_MULTIPLIER = 1.8
OI_DELTA_THRESHOLD = 0.005   # 0.5%
BUY_RATIO_THRESHOLD = 0.55
FUNDING_LIMIT = 0.002


# =============================
# LOAD SYMBOL LIST
# =============================

def get_symbols():
    db = SessionLocal()
    try:
        q = text("""
            SELECT DISTINCT symbol
            FROM candles_1h
        """)
        rows = db.execute(q).fetchall()
        return [r[0] for r in rows]
    finally:
        db.close()


# =============================
# LOAD DATA
# =============================

def load_symbol_data(symbol: str):
    db = SessionLocal()
    try:
        q = text(f"""
            SELECT *
            FROM candles_1h
            WHERE symbol = :symbol
            ORDER BY open_time DESC
            LIMIT {LOOKBACK}
        """)

        df = pd.read_sql(q, db.bind, params={"symbol": symbol})
        df = df.sort_values("open_time").reset_index(drop=True)
        return df
    finally:
        db.close()


# =============================
# INDICATORS
# =============================

def compute_atr(df, period=14):
    high = df["high_price"]
    low = df["low_price"]
    close = df["close_price"]

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()

    return atr


# =============================
# PRE-EXPLOSION DETECTION
# =============================

def detect_pre_explosion(df):

    if len(df) < 50:
        return None

    df["atr"] = compute_atr(df, ATR_PERIOD)
    df["avg_volume"] = df["base_volume"].rolling(20).mean()
    df["avg_oi"] = df["open_interest"].rolling(20).mean()
    df["buy_ratio"] = df["taker_buy_base_volume"] / df["base_volume"]

    i = len(df) - 1
    current = df.iloc[i]
    window = df.iloc[i-COMPRESSION_PERIOD:i]

    atr_now = current["atr"]
    if pd.isna(atr_now):
        return None

    # -----------------------------
    # Evaluate Conditions
    # -----------------------------

    range_20 = window["high_price"].max() - window["low_price"].min()
    compression_ok = range_20 <= 3 * atr_now

    breakout_level = window["high_price"].max()
    breakout_ok = current["close_price"] > breakout_level

    avg_vol = current["avg_volume"] if not pd.isna(current["avg_volume"]) else 0
    vol_ratio = (current["base_volume"] / avg_vol) if avg_vol > 0 else 0
    volume_ok = vol_ratio >= VOLUME_MULTIPLIER

    oi_delta = current["oi_delta_percent"] if current["oi_delta_percent"] is not None else 0
    avg_oi = current["avg_oi"] if not pd.isna(current["avg_oi"]) else 0
    oi_ok = (
        oi_delta >= OI_DELTA_THRESHOLD and
        current["open_interest"] >= avg_oi
    )

    funding_ok = abs(current["funding_rate"]) <= FUNDING_LIMIT

    buy_ratio = current["buy_ratio"] if not pd.isna(current["buy_ratio"]) else 0
    buy_ok = buy_ratio >= BUY_RATIO_THRESHOLD

    all_ok = all([
        compression_ok,
        breakout_ok,
        volume_ok,
        oi_ok,
        funding_ok,
        buy_ok
    ])

    status = "PASS" if all_ok else "FAIL"

    # -----------------------------
    # Clean Structured Log
    # -----------------------------


    logger.info(
        f"\n{current['symbol']} | {status}\n"
        f"  Range20   : {range_20:.4f} (<= {3*atr_now:.4f})\n"
        f"  Breakout  : {breakout_ok}\n"
        f"  Volume    : {vol_ratio:.2f}x (>= {VOLUME_MULTIPLIER})\n"
        f"  OI Delta  : {oi_delta:.4f} (>= {OI_DELTA_THRESHOLD})\n"
        f"  Funding   : {current['funding_rate']:.4f} (<= {FUNDING_LIMIT})\n"
        f"  BuyRatio  : {buy_ratio:.2f} (>= {BUY_RATIO_THRESHOLD})\n"
        f"{'-'*50}"
    )

    if not all_ok:
        return None

    return {
        "symbol": current["symbol"],
        "open_time": current["open_time"],
        "close_price": current["close_price"],
        "oi_delta": oi_delta,
        "volume_ratio": vol_ratio,
        "buy_ratio": buy_ratio
    }


# =============================
# MAIN SCANNER
# =============================

if __name__ == "__main__":

    symbols = get_symbols()
    signals = []

    logger.info(f"\nScanning {len(symbols)} symbols...\n")

    for symbol in symbols:
        df = load_symbol_data(symbol)
        signal = detect_pre_explosion(df)
        if signal:
            signals.append(signal)

    signals = sorted(signals, key=lambda x: x["oi_delta"], reverse=True)

    logger.info("\n==============================")
    if signals:
        logger.info("🔥 PRE-EXPLOSION SIGNALS FOUND:")
        for s in signals:
            logger.info(s)
    else:
        logger.info("No signals detected.")