import json
import pandas as pd
import numpy as np
from sqlalchemy import select, distinct
from datetime import datetime
from app.db import SessionLocal
from app.models import Candle1H


class RADX1H:

    def __init__(self, window=60):
        self.window = window

    # -------------------------------------------------------
    # FETCH DATA
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

        # EMA
        df["ema_21"] = df.groupby("symbol")["close_price"] \
            .transform(lambda x: x.ewm(span=21).mean())

        df["ema_slope"] = df.groupby("symbol")["ema_21"] \
            .transform(lambda x: x.pct_change())

        df["above_ema"] = df["close_price"] > df["ema_21"]
        df["below_ema"] = df["close_price"] < df["ema_21"]

        # RSI
        delta = df.groupby("symbol")["close_price"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.groupby(df["symbol"]).transform(lambda x: x.rolling(14).mean())
        avg_loss = loss.groupby(df["symbol"]).transform(lambda x: x.rolling(14).mean())

        rs = avg_gain / avg_loss
        df["rsi"] = 100 - (100 / (1 + rs))

        # Range ratio
        df["rolling_range"] = df.groupby("symbol")["close_price"] \
            .transform(lambda x: x.rolling(20).max() - x.rolling(20).min())

        df["rolling_std"] = df.groupby("symbol")["close_price"] \
            .transform(lambda x: x.rolling(20).std())

        df["range_ratio"] = df["rolling_std"] / df["rolling_range"]

        # Structure (20 candle)
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

        # Volume impulse
        df["vol_avg_3"] = df.groupby("symbol")["base_volume"] \
            .transform(lambda x: x.rolling(3).mean())

        df["vol_avg_20"] = df.groupby("symbol")["base_volume"] \
            .transform(lambda x: x.rolling(20).mean())

        df["vol_ratio"] = df["vol_avg_3"] / df["vol_avg_20"]

        # Consecutive candles
        df["red_candle"] = df["close_price"] < df["open_price"]
        df["green_candle"] = df["close_price"] > df["open_price"]

        df["consecutive_red"] = df.groupby("symbol")["red_candle"] \
            .transform(lambda x: x.rolling(5).sum())

        df["consecutive_green"] = df.groupby("symbol")["green_candle"] \
            .transform(lambda x: x.rolling(5).sum())

        # EMA distance
        df["ema_distance"] = ((df["close_price"] - df["ema_21"]) / df["ema_21"]).abs()

        # RSI divergence (simple 5-candle)
        df["bullish_divergence"] = False

        for symbol in df["symbol"].unique():
            temp = df[df["symbol"] == symbol]
            if len(temp) >= 5:
                if (
                    temp["close_price"].iloc[-1] < temp["close_price"].iloc[-5] and
                    temp["rsi"].iloc[-1] > temp["rsi"].iloc[-5]
                ):
                    df.loc[temp.index[-1], "bullish_divergence"] = True

        # Funding extreme (if exists)
        if "funding_rate" in df.columns:
            df["funding_extreme"] = df["funding_rate"].abs() > 0.01
        else:
            df["funding_extreme"] = False

        return df

    # -------------------------------------------------------
    # ANALYSIS
    # -------------------------------------------------------
    def analyze(self, df, symbols):

        results = []

        for symbol in symbols:
            row = df[df["symbol"] == symbol].iloc[-1]
            bias = 0
            signals = []

            # Trend Structure
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

            if row["ema_slope"] > 0.01:
                bias += 25
                signals.append("EMA sloping up")

            if row["above_ema"]:
                bias += 20
                signals.append("Price above EMA")

            if row["bos_up_recent"]:
                bias += 40
                signals.append("Recent macro structure break (up)")

            bias = max(min(bias, 100), -100)

            # Regime
            if bias <= -60:
                regime = "Strong Bearish Trend"
                guidance = "Prefer short continuation or pullback entries."
            elif bias <= -40:
                regime = "Bearish Trend"
                guidance = "Avoid longs. Look for pullback shorts."
            elif bias >= 60:
                regime = "Strong Bullish Trend"
                guidance = "Prefer long continuation setups."
            elif bias >= 40:
                regime = "Bullish Trend"
                guidance = "Avoid shorts."
            else:
                regime = "Neutral"
                guidance = "No strong edge."

            # Exhaustion
            exhaustion_score = 0
            if row["ema_distance"] > 0.05:
                exhaustion_score += 30
            if row["consecutive_red"] >= 4:
                exhaustion_score += 25
            if row["vol_ratio"] > 2:
                exhaustion_score += 20

            if exhaustion_score >= 60:
                exhaustion = "High Exhaustion Risk"
            elif exhaustion_score >= 30:
                exhaustion = "Moderate Exhaustion Risk"
            else:
                exhaustion = "Low Exhaustion Risk"

            # Pullback Probability
            pullback_score = 0
            if bias <= -40:
                if row["ema_distance"] > 0.05:
                    pullback_score += 25
                if row["rsi"] < 30:
                    pullback_score += 25
                if row["consecutive_red"] >= 4:
                    pullback_score += 20
                if row["vol_ratio"] > 2:
                    pullback_score += 20

            if pullback_score >= 70:
                pullback_prob = "High Pullback Probability"
            elif pullback_score >= 40:
                pullback_prob = "Moderate Pullback Probability"
            else:
                pullback_prob = "Low Pullback Probability"

            # Reversal Probability
            reversal_score = 0
            if row["bullish_divergence"]:
                reversal_score += 40
            if abs(row["ema_slope"]) < 0.005:
                reversal_score += 20

            if reversal_score >= 50:
                reversal = "Reversal Probability Increasing"
            else:
                reversal = "No Clear Reversal Signal"

            results.append({
                "symbol": symbol,
                "regime": regime,
                "direction_bias_score": int(bias),
                "exhaustion_risk": exhaustion,
                "pullback_probability": pullback_prob,
                "reversal_probability": reversal,
                "funding_extreme": bool(row["funding_extreme"]),
                "structure_signals": signals,
                "decision_guidance": guidance
            })

        return {
            "meta": {
                "analysis_time": str(datetime.now()),
                "timeframe": "1H",
                "candles_used_per_symbol": self.window,
                "date_range": {
                    "start": str(df["open_time"].min()),
                    "end": str(df["open_time"].max())
                },
                "symbols_analyzed": symbols
            },
            "results": results
        }


# -------------------------------------------------------
# RUN
# -------------------------------------------------------

if __name__ == "__main__":

    # -----------------------------------
    # CONFIG
    # -----------------------------------
    USE_USER_INPUT = True   # Change to False for auto mode
    USE_USER_INPUT = False   # Change to False for auto mode

    USER_SYMBOLS = ["PIPPINUSDT", "RIVERUSDT", "ETHUSDT"]

    # -----------------------------------
    # SYMBOL SELECTION
    # -----------------------------------

    if USE_USER_INPUT:
        print("Using user-defined symbols...")
        symbols = USER_SYMBOLS

    else:
        print("Fetching symbols from Candle1H table...")

        db = SessionLocal()

        stmt = select(distinct(Candle1H.symbol))
        rows = db.execute(stmt).all()

        db.close()

        symbols = [row[0] for row in rows]

        if not symbols:
            raise ValueError("No symbols found in Candle1H table.")

        print(f"Loaded {len(symbols)} symbols from database.")

    # -----------------------------------
    # RUN ENGINE
    # -----------------------------------

    engine = RADX1H(window=60)

    df = engine.fetch_data(symbols)
    df = engine.calculate_indicators(df)
    output = engine.analyze(df, symbols)

    print(json.dumps(output, indent=2))