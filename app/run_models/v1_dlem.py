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
LIQUIDITY_LOOKBACK = 48
MIN_RR = 1.5


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
              AND open_interest IS NOT NULL
              AND funding_rate IS NOT NULL
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
# CORE ENGINE
# =============================

def evaluate_symbol(df, symbol_name):

    if df is None or len(df) < max(60, ATR_PERIOD + 20):
        return None

    df = df.copy()

    # ---------- Indicators ----------
    df["atr"] = compute_atr(df, ATR_PERIOD)
    df["avg_volume"] = df["base_volume"].rolling(20).mean()
    df["ema20"] = df["close_price"].ewm(span=20).mean()

    df["price_change"] = df["close_price"].pct_change()
    df["oi_change"] = df["open_interest"].pct_change()
    df["funding_delta"] = df["funding_rate"].diff()

    df = df.dropna().reset_index(drop=True)
    if len(df) < 60:
        return None

    i = len(df) - 1
    current = df.iloc[i]
    window = df.iloc[i - COMPRESSION_PERIOD:i]

    # =============================
    # EXPANSION (Compression Detection)
    # =============================

    range_20 = window["high_price"].max() - window["low_price"].min()
    atr_now = current["atr"]
    avg_vol = current["avg_volume"]
    base_volume = current["base_volume"]

    if atr_now <= 0 or avg_vol <= 0:
        return None

    range_ratio = range_20 / (3 * atr_now)
    volume_ratio = base_volume / avg_vol

    expansion_score = (
        (1 - np.tanh(range_ratio)) * 0.5 +
        (1 - np.tanh(volume_ratio)) * 0.5
    ) * 100

    # =============================
    # OI STRUCTURE CLASSIFICATION
    # =============================

    price_change = current["price_change"]
    oi_change = current["oi_change"]

    if price_change > 0 and oi_change > 0:
        oi_state = "LongBuild"
        oi_component = 1
    elif price_change > 0 and oi_change < 0:
        oi_state = "ShortCover"
        oi_component = 0.3
    elif price_change < 0 and oi_change > 0:
        oi_state = "ShortBuild"
        oi_component = -0.7
    else:
        oi_state = "LongUnwind"
        oi_component = -0.3

    # =============================
    # TAKER AGGRESSION
    # =============================

    taker_buy = current.get("taker_buy_base_volume", 0.0)
    taker_sell = base_volume - taker_buy

    if taker_sell <= 0:
        return None

    taker_ratio = taker_buy / taker_sell

    funding_delta = current["funding_delta"]

    directional_score = (
        oi_component * 0.4 +
        np.tanh(taker_ratio - 1) * 0.4 +
        np.tanh(funding_delta * 100) * 0.2
    ) * 100

    # =============================
    # EXHAUSTION
    # =============================

    ema20 = current["ema20"]
    close_price = current["close_price"]

    extension = (close_price - ema20) / ema20
    exhaustion_score = abs(np.tanh(extension * 5)) * 100

    # =============================
    # LIQUIDITY MAPPING
    # =============================

    recent_high = df["high_price"].rolling(LIQUIDITY_LOOKBACK).max().iloc[-2]
    recent_low = df["low_price"].rolling(LIQUIDITY_LOOKBACK).min().iloc[-2]

    distance_to_resistance = recent_high - close_price
    liquidity_ratio = (
        distance_to_resistance / range_20 if range_20 > 0 else 0
    )

    # =============================
    # R:R FILTER
    # =============================

    risk = close_price - window["low_price"].min()
    reward = distance_to_resistance

    rr_ratio = reward / risk if risk > 0 else 0

    # =============================
    # BREAKOUT CONFIRMATION
    # =============================

    range_high = window["high_price"].max()

    candle_body = abs(current["close_price"] - current["open_price"])
    candle_range = current["high_price"] - current["low_price"]

    strong_body = (candle_body / candle_range) > 0.7 if candle_range > 0 else False

    breakout_confirmed = (
        close_price > range_high and
        base_volume > 1.5 * avg_vol and
        oi_change > 0 and
        strong_body
    )

    return {
        "symbol": symbol_name,
        "expansion_score": round(expansion_score, 2),
        "directional_score": round(directional_score, 2),
        "exhaustion_score": round(exhaustion_score, 2),
        "range_ratio": round(range_ratio, 4),
        "volume_ratio": round(volume_ratio, 4),
        "oi_state": oi_state,
        "taker_ratio": round(taker_ratio, 4),
        "liquidity_ratio": round(liquidity_ratio, 2),
        "rr_ratio": round(rr_ratio, 2),
        "breakout_confirmed": breakout_confirmed
    }


# =============================
# SCAN REPORT
# =============================

def print_scan_report(results):

    qualified_longs = []

    for r in results:
        if (
            r["expansion_score"] > 70 and
            r["directional_score"] > 40 and
            r["exhaustion_score"] < 60 and
            r["liquidity_ratio"] >= 1.5 and
            r["rr_ratio"] >= MIN_RR and
            r["breakout_confirmed"]
        ):
            qualified_longs.append(r["symbol"])

    print("\n===== MODEL v2 REPORT =====")
    print("Qualified Explosion Longs:", qualified_longs)
    print("================================\n")


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
    filename = f"signals/quant_scan_v2_{timestamp}.json"

    output = {
        "timeframe": "1H",
        "symbols": evaluation_results
    }

    with open(filename, "w") as f:
        json.dump(output, f, indent=4)

    print("Scan exported:", filename)