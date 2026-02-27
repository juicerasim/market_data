import os
import json
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from app.db import SessionLocal


# =============================
# LOGGING
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
OI_DELTA_THRESHOLD = 0.005
BUY_RATIO_THRESHOLD = 0.55
FUNDING_LIMIT = 0.002


# =============================
# DB FUNCTIONS
# =============================

def get_symbols():
    db = SessionLocal()
    try:
        q = text("SELECT DISTINCT symbol FROM candles_1h")
        rows = db.execute(q).fetchall()
        return [r[0] for r in rows]
    finally:
        db.close()


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
    return tr.rolling(period).mean()


# =============================
# EVALUATION ENGINE (PURE NUMERIC)
# =============================

def evaluate_symbol(df, symbol_name):

    def safe_float(value):
        return float(value) if value is not None and pd.notna(value) else 0.0

    # If insufficient data, still return numeric structure
    if len(df) < 50:
        return {
            "symbol": symbol_name,
            "range_20": 0.0,
            "atr_x3_limit": 0.0,
            "breakout": False,
            "volume_ratio": 0.0,
            "volume_threshold": VOLUME_MULTIPLIER,
            "oi_delta": 0.0,
            "oi_threshold": OI_DELTA_THRESHOLD,
            "funding_rate": 0.0,
            "funding_limit": FUNDING_LIMIT,
            "buy_ratio": 0.0,
            "buy_threshold": BUY_RATIO_THRESHOLD
        }

    df["atr"] = compute_atr(df, ATR_PERIOD)
    df["avg_volume"] = df["base_volume"].rolling(20).mean()
    df["avg_oi"] = df["open_interest"].rolling(20).mean()
    df["buy_ratio"] = df["taker_buy_base_volume"] / df["base_volume"]

    i = len(df) - 1
    current = df.iloc[i]
    window = df.iloc[i - COMPRESSION_PERIOD:i]

    atr_now = safe_float(current["atr"])
    range_20 = safe_float(window["high_price"].max() - window["low_price"].min())
    breakout_level = safe_float(window["high_price"].max())
    close_price = safe_float(current["close_price"])

    avg_vol = safe_float(current["avg_volume"])
    base_volume = safe_float(current["base_volume"])
    vol_ratio = (base_volume / avg_vol) if avg_vol > 0 else 0.0

    oi_delta = safe_float(current["oi_delta_percent"])
    funding_rate = safe_float(current["funding_rate"])
    buy_ratio = safe_float(current["buy_ratio"])

    return {
        "symbol": symbol_name,
        "range_20": range_20,
        "atr_x3_limit": 3 * atr_now,
        "breakout": close_price > breakout_level,
        "volume_ratio": vol_ratio,
        "volume_threshold": VOLUME_MULTIPLIER,
        "oi_delta": oi_delta,
        "oi_threshold": OI_DELTA_THRESHOLD,
        "funding_rate": funding_rate,
        "funding_limit": FUNDING_LIMIT,
        "buy_ratio": buy_ratio,
        "buy_threshold": BUY_RATIO_THRESHOLD
    }


# =============================
# PROMPT GENERATOR
# =============================

def generate_prompt(symbols_data):

    return f"""
You are evaluating quantitative breakout scan data.

No labels are provided.
Use only numeric values relative to their thresholds.

Tasks:

1. Rank strongest expansion candidates
2. Detect near-breakout setups
3. Identify contraction regime patterns
4. Highlight symbols closest to threshold alignment
5. Detect any outlier derivatives positioning

Execution context:
- Futures market
- 1H timeframe

Here is the scan data:

{json.dumps(symbols_data, indent=2)}
""".strip()


# =============================
# MAIN
# =============================

if __name__ == "__main__":

    symbols = get_symbols()
    evaluation_results = []

    for symbol in symbols:
        df = load_symbol_data(symbol)
        result = evaluate_symbol(df, symbol)
        evaluation_results.append(result)

    os.makedirs("signals", exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"signals/quant_scan_{timestamp}.json"

    output = {
        "timeframe": "1H",
        "symbols": evaluation_results,
        "analysis_prompt": generate_prompt(evaluation_results)
    }

    with open(filename, "w") as f:
        json.dump(output, f, indent=4)

    print("\nQuantitative scan file created:")
    print(filename)