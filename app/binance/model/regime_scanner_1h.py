import json
import pandas as pd
import numpy as np
from sqlalchemy import select
from datetime import datetime
from app.db import SessionLocal
from app.models import Candle1H, OpenInterest1H, FundingRate8H



class RADX1H:

    def __init__(self, window=60):
        self.window = window

    # -------------------------------------------------------
    # FETCH LAST N CANDLES PER SYMBOL
    # -------------------------------------------------------
    def fetch_data(self, symbols):
        db = SessionLocal()

        stmt = (
            select(Candle1H)
            .where(Candle1H.symbol.in_(symbols))
            .order_by(Candle1H.symbol, Candle1H.open_time)
        )

        rows = db.execute(stmt).scalars().all()
        db.close()

        df = pd.DataFrame([r.__dict__ for r in rows])
        df = df.drop(columns=["_sa_instance_state"])

        df = (
            df.sort_values(["symbol", "open_time"])
            .groupby("symbol")
            .tail(self.window)
            .reset_index(drop=True)
        )

        return df

    # -------------------------------------------------------
    # CALCULATE INDICATORS
    # -------------------------------------------------------
    def calculate_indicators(self, df):

        # ---------------- EMA ----------------
        df["ema_21"] = df.groupby("symbol")["close_price"] \
            .transform(lambda x: x.ewm(span=21).mean())

        df["ema_slope"] = df.groupby("symbol")["ema_21"] \
            .transform(lambda x: x.pct_change() * 100)

        df["above_ema"] = df["close_price"] > df["ema_21"]
        df["below_ema"] = df["close_price"] < df["ema_21"]

        # ---------------- RANGE RATIO ----------------
        df["rolling_range"] = df.groupby("symbol")["close_price"] \
            .transform(lambda x: x.rolling(20).max() - x.rolling(20).min())

        df["rolling_std"] = df.groupby("symbol")["close_price"] \
            .transform(lambda x: x.rolling(20).std())

        df["range_ratio"] = df["rolling_std"] / df["rolling_range"]

        # ---------------- MACRO STRUCTURE (20 candles) ----------------
        df["swing_high_20"] = df.groupby("symbol")["high_price"] \
            .transform(lambda x: x.rolling(20).max())

        df["swing_low_20"] = df.groupby("symbol")["low_price"] \
            .transform(lambda x: x.rolling(20).min())

        df["bos_down"] = df["close_price"] < df["swing_low_20"].shift(1)
        df["bos_up"] = df["close_price"] > df["swing_high_20"].shift(1)

        df["bos_down_recent"] = df.groupby("symbol")["bos_down"] \
            .transform(lambda x: x.rolling(3).max())

        df["bos_up_recent"] = df.groupby("symbol")["bos_up"] \
            .transform(lambda x: x.rolling(3).max())

        # ---------------- VOLUME IMPULSE ----------------
        df["vol_avg_3"] = df.groupby("symbol")["base_volume"] \
            .transform(lambda x: x.rolling(3).mean())

        df["vol_avg_20"] = df.groupby("symbol")["base_volume"] \
            .transform(lambda x: x.rolling(20).mean())

        df["vol_ratio"] = df["vol_avg_3"] / df["vol_avg_20"]

        # ---------------- OI BUILD (optional) ----------------
        if "open_interest" in df.columns:
            df["oi_build_6h"] = df.groupby("symbol")["open_interest"] \
                .transform(lambda x: x.pct_change(6))
        else:
            df["oi_build_6h"] = 0

        df["oi_build_6h"] = df["oi_build_6h"].fillna(0)

        return df

    # -------------------------------------------------------
    # ANALYSIS ENGINE
    # -------------------------------------------------------
    def analyze(self, df, symbols):

        results = []

        for symbol in symbols:

            row = df[df["symbol"] == symbol].iloc[-1]

            bias = 0
            signals = []

            # ----- BEARISH SIDE -----
            if row["ema_slope"] < -0.01:
                bias -= 25
                signals.append("EMA sloping down")

            if row["below_ema"]:
                bias -= 20
                signals.append("Price below EMA")

            if row["bos_down_recent"]:
                bias -= 40
                signals.append("Recent macro structure break (down)")

            if row["vol_ratio"] > 1.5:
                bias -= 20
                signals.append("Volume expansion during move")

            # ----- BULLISH SIDE -----
            if row["ema_slope"] > 0.01:
                bias += 25
                signals.append("EMA sloping up")

            if row["above_ema"]:
                bias += 20
                signals.append("Price above EMA")

            if row["bos_up_recent"]:
                bias += 40
                signals.append("Recent macro structure break (up)")

            # Clamp bias
            bias = max(min(bias, 100), -100)

            # ----- REGIME CLASSIFICATION -----
            if bias <= -60:
                regime = "Strong Bearish Trend"
                guidance = "Active downtrend. Prefer short continuation or pullback entries."

            elif bias <= -40:
                regime = "Bearish Trend"
                guidance = "Trend bearish. Avoid longs. Look for pullback shorts."

            elif bias >= 60:
                regime = "Strong Bullish Trend"
                guidance = "Active uptrend. Prefer long continuation setups."

            elif bias >= 40:
                regime = "Bullish Trend"
                guidance = "Trend bullish. Avoid shorts."

            elif row["range_ratio"] < 0.8:
                regime = "Compression"
                guidance = "Market compressing. Wait for breakout."

            else:
                regime = "Neutral"
                guidance = "No strong directional edge."

            results.append({
                "symbol": symbol,
                "regime": regime,
                "direction_bias_score": int(bias),
                "range_ratio": round(float(row["range_ratio"]), 2),
                "oi_build_6h": round(float(row["oi_build_6h"]), 2),
                "volume_ratio": round(float(row["vol_ratio"]), 2),
                "structure_signals": signals,
                "decision_guidance": guidance
            })

        output = {
            "meta": {
                "analysis_time_ist": datetime.now(),
                "timeframe": "1H",
                "candles_used_per_symbol": self.window,
                "date_range_ist": {
                    "start": df["open_time"].min(),
                    "end": df["open_time"].max()
                },
                "symbols_analyzed": symbols
            },
            "results": results
        }

        return output


# -------------------------------------------------------
# RUN SCRIPT
# -------------------------------------------------------

if __name__ == "__main__":

    symbols = ["PIPPINUSDT", "RIVERUSDT", "ETHUSDT"]

    engine = RADX1H(window=60)

    df = engine.fetch_data(symbols)
    df = engine.calculate_indicators(df)
    output = engine.analyze(df, symbols)

    print(json.dumps(output, indent=2, default=str))