import json
import pandas as pd
import numpy as np
import os
from sqlalchemy import select, distinct
from datetime import datetime,  timedelta
import time
from app.db import SessionLocal
from app.models import Candle1H
from zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")
from app.telegram import send_telegram_message, format_timestamp_ist


class RADX1H:

    def __init__(self, window=60):
        self.window = window

    # -------------------------------------------------------
    # FETCH DATA (UNCHANGED)
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
    # CALCULATE INDICATORS (UPGRADED)
    # -------------------------------------------------------
    def calculate_indicators(self, df):

        # EMA
        df["ema_21"] = df.groupby("symbol")["close_price"].transform(
            lambda x: x.ewm(span=21).mean()
        )

        df["ema_slope"] = df.groupby("symbol")["ema_21"].transform(
            lambda x: x.pct_change()
        )

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

        # Range ratio (UNCHANGED)
        df["rolling_range"] = df.groupby("symbol")["close_price"].transform(
            lambda x: x.rolling(20).max() - x.rolling(20).min()
        )

        df["rolling_std"] = df.groupby("symbol")["close_price"].transform(
            lambda x: x.rolling(20).std()
        )

        df["range_ratio"] = df["rolling_std"] / df["rolling_range"]

        # Structure
        df["swing_high_20"] = df.groupby("symbol")["high_price"].transform(
            lambda x: x.rolling(20).max()
        )

        df["swing_low_20"] = df.groupby("symbol")["low_price"].transform(
            lambda x: x.rolling(20).min()
        )

        df["bos_down"] = df["close_price"] < df["swing_low_20"].shift(1)
        df["bos_up"] = df["close_price"] > df["swing_high_20"].shift(1)

        df["bos_down_recent"] = df.groupby("symbol")["bos_down"].transform(
            lambda x: x.rolling(3).max()
        )

        df["bos_up_recent"] = df.groupby("symbol")["bos_up"].transform(
            lambda x: x.rolling(3).max()
        )

        # Volume impulse
        df["vol_avg_3"] = df.groupby("symbol")["base_volume"].transform(
            lambda x: x.rolling(3).mean()
        )

        df["vol_avg_20"] = df.groupby("symbol")["base_volume"].transform(
            lambda x: x.rolling(20).mean()
        )

        df["vol_ratio"] = df["vol_avg_3"] / df["vol_avg_20"]

        # Consecutive candles
        df["red_candle"] = df["close_price"] < df["open_price"]
        df["green_candle"] = df["close_price"] > df["open_price"]

        df["consecutive_red"] = df.groupby("symbol")["red_candle"].transform(
            lambda x: x.rolling(5).sum()
        )

        df["consecutive_green"] = df.groupby("symbol")["green_candle"].transform(
            lambda x: x.rolling(5).sum()
        )

        # EMA distance
        df["ema_distance"] = (
            (df["close_price"] - df["ema_21"]) / df["ema_21"]
        ).abs()

        # RSI divergence (UNCHANGED)
        df["bullish_divergence"] = False

        for symbol in df["symbol"].unique():
            temp = df[df["symbol"] == symbol]
            if len(temp) >= 5:
                if (
                    temp["close_price"].iloc[-1] < temp["close_price"].iloc[-5]
                    and temp["rsi"].iloc[-1] > temp["rsi"].iloc[-5]
                ):
                    df.loc[temp.index[-1], "bullish_divergence"] = True

        # -------------------------------
        # NEW: ADX TREND STRENGTH
        # -------------------------------
        high = df["high_price"]
        low = df["low_price"]
        close = df["close_price"]

        plus_dm = high.diff()
        minus_dm = low.diff().abs()

        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0

        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(14).mean()

        plus_di = 100 * (plus_dm.rolling(14).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(14).mean() / atr)

        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100

        df["adx"] = dx.rolling(14).mean()

        # -------------------------------
        # NEW: VOLATILITY EXPANSION
        # -------------------------------
        df["atr"] = atr

        df["atr_expansion"] = df.groupby("symbol")["atr"].transform(
            lambda x: x / x.rolling(20).mean()
        )

        # -------------------------------
        # NEW: BEAR FLAG DETECTION
        # -------------------------------
        df["pullback_high_5"] = df.groupby("symbol")["high_price"].transform(
            lambda x: x.rolling(5).max()
        )

        df["bear_flag"] = (
            (df["below_ema"])
            & (df["close_price"] < df["pullback_high_5"])
            & (df["rsi"] > 35)
            & (df["rsi"] < 55)
        )

        # Funding extreme (UNCHANGED)
        if "funding_rate" in df.columns:
            df["funding_extreme"] = df["funding_rate"].abs() > 0.01
        else:
            df["funding_extreme"] = False

        return df

    # -------------------------------------------------------
    # ANALYSIS (UPGRADED)
    # -------------------------------------------------------

    def analyze(self, df, symbols):

        results = []

        start_utc = pd.to_datetime(df["open_time"].min(), unit="ms", utc=True)
        end_utc = pd.to_datetime(df["open_time"].max(), unit="ms", utc=True)

        start_ist = start_utc.astimezone(IST)
        end_ist = end_utc.astimezone(IST)

        for symbol in symbols:

            row = df[df["symbol"] == symbol].iloc[-1]

            bias = 0
            signals = []

            # ---------------- TREND STRUCTURE ----------------
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

            # ---------------- REGIME ----------------
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

            # ---------------- EXHAUSTION ----------------
            exhaustion_score = 0

            if row["ema_distance"] > 0.06:
                exhaustion_score += 25

            if row["rsi"] < 25:
                exhaustion_score += 25

            if row["consecutive_red"] >= 5:
                exhaustion_score += 20

            if row["atr_expansion"] > 1.6:
                exhaustion_score += 20

            if exhaustion_score >= 60:
                exhaustion = "High Exhaustion Risk"
            elif exhaustion_score >= 30:
                exhaustion = "Moderate Exhaustion Risk"
            else:
                exhaustion = "Low Exhaustion Risk"

            # ---------------- PULLBACK PROBABILITY ----------------
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

            # ---------------- REVERSAL ----------------
            reversal_score = 0

            if row["bullish_divergence"]:
                reversal_score += 40

            if abs(row["ema_slope"]) < 0.005:
                reversal_score += 20

            if reversal_score >= 50:
                reversal = "Reversal Probability Increasing"
            else:
                reversal = "No Clear Reversal Signal"

            # ---------------- CONTINUATION ----------------
            continuation_score = 0

            if row["adx"] > 30:
                continuation_score += 25

            if row["below_ema"]:
                continuation_score += 20

            if row["bos_down_recent"]:
                continuation_score += 30

            if row["bear_flag"]:
                continuation_score += 25
                signals.append("Bear flag pullback detected")

            if row["atr_expansion"] > 1.3:
                continuation_score += 20
                signals.append("Volatility expansion detected")

            if continuation_score >= 70:
                continuation_signal = "High Probability Trend Continuation"
            elif continuation_score >= 40:
                continuation_signal = "Moderate Continuation Probability"
            else:
                continuation_signal = "Low Continuation Probability"

            # ---------------- SHORT TRADE SETUP ----------------
            trade_score = 0

            if row["adx"] > 30:
                trade_score += 20

            if row["bos_down_recent"]:
                trade_score += 20

            if row["below_ema"]:
                trade_score += 15

            if row["bear_flag"]:
                trade_score += 20

            if row["atr_expansion"] > 1.3:
                trade_score += 15

            if row["vol_ratio"] > 1.5:
                trade_score += 10

            if exhaustion == "High Exhaustion Risk":
                trade_score -= 20

            trade_score = max(min(trade_score, 100), 0)

            if trade_score >= 75:
                trade_probability = "High Probability Short Setup"
            elif trade_score >= 50:
                trade_probability = "Moderate Short Setup"
            else:
                trade_probability = "Low Quality Setup"

            # ---------------- RESULT ----------------
            results.append({
                "symbol": symbol,
                "regime": regime,
                "direction_bias_score": int(bias),

                "short_setup_score": int(trade_score),
                "short_setup_probability": trade_probability,

                "exhaustion_risk": exhaustion,
                "pullback_probability": pullback_prob,
                "reversal_probability": reversal,
                "continuation_probability": continuation_signal,

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
                    "start_ist": start_ist.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_ist": end_ist.strftime("%Y-%m-%d %H:%M:%S")
                },
                "symbols_analyzed": symbols
            },
            "results": results
        }

def export_report_json(output: dict, folder: str = "reports"):
    """
    Export analysis output as JSON file.

    File format:
    reports/scan_report_YYYY-MM-DD HH-MM-SS.json
    """

    # Ensure folder exists
    os.makedirs(folder, exist_ok=True)

    # Get timestamp
    analysis_time = output["meta"]["analysis_time"]

    # Convert to datetime safely
    if isinstance(analysis_time, str):
        dt_obj = datetime.fromisoformat(analysis_time)
    else:
        dt_obj = analysis_time

    # IMPORTANT:
    # Replace ':' because Windows doesn't allow it in filenames
    formatted_time = dt_obj.strftime("%Y-%m-%d %H-%M-%S")

    filename = f"scan_report_{formatted_time}.json"
    filepath = os.path.join(folder, filename)

    with open(filepath, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"Report exported to: {filepath}")

def check_and_send_alert(output):
    """
    Send Telegram alert if any symbol regime is not Neutral.
    """

    triggered = []

    for result in output["results"]:
        if result["regime"] != "Neutral":
            triggered.append(result)

    if not triggered:
        print("No non-neutral regimes detected. No alert sent.")
        return

    # Build message
    message_lines = []
    message_lines.append("🚨 Market Regime Alert 🚨\n")

    for r in triggered:
        message_lines.append(
            f"Symbol: {r['symbol']}\n"
            f"Regime: {r['regime']}\n"
            f"Bias: {r['direction_bias_score']}\n"
            f"Exhaustion: {r['exhaustion_risk']}\n"
            f"Pullback: {r['pullback_probability']}\n"
            f"Reversal: {r['reversal_probability']}\n"
            f"Guidance: {r['decision_guidance']}\n"
            f"{'-'*30}"
        )

    final_message = "\n".join(message_lines)

    send_telegram_message(final_message)
    print("Telegram alert sent.")

def wait_until_next_hour_close(buffer_minutes: int = 2):
    """
    Wait until next 1H candle close (IST aligned to :30)
    plus configurable buffer in minutes.
    """

    now = datetime.now(IST)

    # Determine next :30 boundary
    if now.minute < 30:
        base_close = now.replace(minute=30, second=0, microsecond=0)
    else:
        base_close = (
            now + timedelta(hours=1)
        ).replace(minute=30, second=0, microsecond=0)

    # Add buffer
    next_close = base_close + timedelta(minutes=buffer_minutes)

    wait_seconds = (next_close - now).total_seconds()

    print(
        f"Waiting {int(wait_seconds)} seconds "
        f"until execution at {next_close}"
    )

    time.sleep(max(wait_seconds, 0))
# def wait_until_next_hour_close():
#     """
#     Testing mode:
#     Wait fixed 30 seconds between runs.
#     """
#     now = datetime.now(IST)
#     print(f"[{now.strftime('%H:%M:%S')}] Waiting 30 seconds for next test cycle...")
#     time.sleep(10)
# -------------------------------------------------------
# RUN
# -------------------------------------------------------

if __name__ == "__main__":
    print("================1====")

    # -----------------------------------
    # CONFIG
    # -----------------------------------
    # USE_USER_INPUT = True   # Change to False for auto mode
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
        print("====================2")

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

    # -------------------------------
    # INITIAL RUN (Immediate)
    # -------------------------------
    print("Running initial scan using latest closed candle")

    try:
        df = engine.fetch_data(symbols)
        df = engine.calculate_indicators(df)
        output = engine.analyze(df, symbols)

        print(json.dumps(output, indent=2))

        export_report_json(output)
        check_and_send_alert(output)

    except Exception as e:
        print("Error during initial scan:", e)

    # -------------------------------
    # PERIODIC RUN
    # -------------------------------
    while True:
        print("====================")

        wait_until_next_hour_close()

        try:
            df = engine.fetch_data(symbols)
            df = engine.calculate_indicators(df)
            output = engine.analyze(df, symbols)

            print(json.dumps(output, indent=2))

            export_report_json(output)
            check_and_send_alert(output)

        except Exception as e:
            print("Error during scan:", e)

    # df = engine.fetch_data(symbols)
    # df = engine.calculate_indicators(df)
    # output = engine.analyze(df, symbols)

    # print(json.dumps(output, indent=2))
    # export_report_json(output)
    # check_and_send_alert(output)