import os
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import text
from app.db import SessionLocal


# =============================
# CONFIG
# =============================

LOOKBACK = 200
COMPRESSION_PERIOD = 20
ATR_PERIOD = 14


# =============================
# LOGGING
# =============================

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


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
# CORE EVALUATION ENGINE
# =============================

def evaluate_symbol(df, symbol_name):

    def safe(x):
        return float(x) if pd.notna(x) else 0.0

    if len(df) < 60:
        return None

    df = df.copy()

    df["atr"] = compute_atr(df, ATR_PERIOD)
    df["avg_volume"] = df["base_volume"].rolling(20).mean()
    df["ema20"] = df["close_price"].ewm(span=20).mean()

    df["price_change"] = df["close_price"].pct_change()
    df["oi_change"] = df["open_interest"].pct_change()
    df["funding_delta"] = df["funding_rate"].diff()

    i = len(df) - 1
    current = df.iloc[i]
    window = df.iloc[i - COMPRESSION_PERIOD:i]

    # =============================
    # EXPANSION
    # =============================

    range_20 = safe(window["high_price"].max() - window["low_price"].min())
    atr_now = safe(current["atr"])
    avg_vol = safe(current["avg_volume"])
    base_volume = safe(current["base_volume"])

    range_ratio = (range_20 / (3 * atr_now)) if atr_now > 0 else 0.0
    volume_ratio = (base_volume / avg_vol) if avg_vol > 0 else 0.0

    expansion_score = (
        (1 - np.tanh(range_ratio)) * 0.5 +
        (1 - np.tanh(volume_ratio)) * 0.5
    ) * 100

    # =============================
    # DIRECTION
    # =============================

    price_change = safe(current["price_change"])
    oi_change = safe(current["oi_change"])

    if price_change > 0 and oi_change > 0:
        oi_price_relation = 1
    elif price_change < 0 and oi_change > 0:
        oi_price_relation = -1
    else:
        oi_price_relation = 0

    taker_buy = safe(current.get("taker_buy_base_volume", 0.0))
    taker_sell = safe(base_volume - taker_buy)
    taker_ratio = taker_buy / taker_sell if taker_sell > 0 else 0.0

    funding_delta = safe(current["funding_delta"])

    directional_score = (
        oi_price_relation * 0.4 +
        np.tanh(taker_ratio - 1) * 0.4 +
        np.tanh(funding_delta * 100) * 0.2
    ) * 100

    # =============================
    # EXHAUSTION
    # =============================

    ema20 = safe(current["ema20"])
    close_price = safe(current["close_price"])

    extension = (close_price - ema20) / ema20 if ema20 > 0 else 0.0
    exhaustion_score = abs(np.tanh(extension * 5)) * 100

    return {
        "symbol": symbol_name,
        "expansion_score": round(expansion_score, 2),
        "directional_score": round(directional_score, 2),
        "exhaustion_score": round(exhaustion_score, 2),
        "range_ratio": round(range_ratio, 4),
        "volume_ratio": round(volume_ratio, 4),
        "oi_price_relation": oi_price_relation,
        "taker_ratio": round(taker_ratio, 4),
        "funding_delta": round(funding_delta, 6)
    }


# =============================
# MODEL SCAN REPORT
# =============================

def print_scan_report(results):

    high_expansion = []
    directional_extreme = []
    exhaustion_risk = []

    for r in results:
        if r["expansion_score"] > 65:
            high_expansion.append(r["symbol"])

        if abs(r["directional_score"]) > 50:
            directional_extreme.append(r["symbol"])

        if r["exhaustion_score"] > 70:
            exhaustion_risk.append(r["symbol"])

    print("\n===== MODEL SCAN REPORT =====")
    print("High Expansion Candidates:", high_expansion)
    print("Directional Extremes:", directional_extreme)
    print("Exhaustion Risk:", exhaustion_risk)
    print("================================\n")


# =============================
# DETERMINISTIC GPT PROMPT
# =============================

def generate_analysis_prompt():
    return """
Quantitative Scan – Perpetual Futures – 1H

Input:
Array of symbols with:
- expansion_score (0–100)
- directional_score (-100 to +100)
- exhaustion_score (0–100)
- range_ratio
- volume_ratio
- oi_price_relation (-1, 0, 1)
- taker_ratio
- funding_delta

Rules (STRICT NUMERIC LOGIC ONLY):

Expansion Classification:
>70 = High Expansion
60–70 = Building
<60 = Compression

Directional Classification:
>50 = Strong Long
30–50 = Moderate Long
-30 to 30 = Neutral
-30 to -50 = Moderate Short
<-50 = Strong Short

Exhaustion:
>70 = Exhausted
50–70 = Elevated
<50 = Healthy

Watchlist:
LONG if Expansion >65 AND Directional >40 AND Exhaustion <60
SHORT if Expansion >65 AND Directional <-40 AND Exhaustion <60
SQUEEZE if Expansion >65 AND abs(Directional)>50 AND volume_ratio<1
EXHAUSTION_ALERT if Exhaustion >70


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
        if result:
            evaluation_results.append(result)

    print_scan_report(evaluation_results)

    os.makedirs("signals", exist_ok=True)

    timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S")
    filename = f"signals/quant_scan_raw_{timestamp}.json"

    output = {
        "timeframe": "1H",
        "symbols": evaluation_results,
        "analysis_prompt": generate_analysis_prompt()
    }

    with open(filename, "w") as f:
        json.dump(output, f, indent=4)

    print("Raw scan exported:", filename)