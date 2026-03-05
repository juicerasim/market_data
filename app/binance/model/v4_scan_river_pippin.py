import json
import pandas as pd
import numpy as np
import os
from sqlalchemy import select, distinct
from datetime import datetime, timedelta
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

        # Volume
        df["vol_avg_3"] = df.groupby("symbol")["base_volume"].transform(
            lambda x: x.rolling(3).mean()
        )

        df["vol_avg_20"] = df.groupby("symbol")["base_volume"].transform(
            lambda x: x.rolling(20).mean()
        )

        df["vol_ratio"] = df["vol_avg_3"] / df["vol_avg_20"]

        # Candles
        df["red_candle"] = df["close_price"] < df["open_price"]
        df["green_candle"] = df["close_price"] > df["open_price"]

        df["consecutive_red"] = df.groupby("symbol")["red_candle"].transform(
            lambda x: x.rolling(5).sum()
        )

        df["consecutive_green"] = df.groupby("symbol")["green_candle"].transform(
            lambda x: x.rolling(5).sum()
        )

        # EMA distance
        df["ema_distance"] = ((df["close_price"] - df["ema_21"]) / df["ema_21"]).abs()

        # ATR + ADX
        high = df["high_price"]
        low = df["low_price"]
        close = df["close_price"]

        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(14).mean()

        plus_dm = high.diff()
        minus_dm = low.diff().abs()

        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0

        plus_di = 100 * (plus_dm.rolling(14).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(14).mean() / atr)

        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100

        df["adx"] = dx.rolling(14).mean()
        df["atr"] = atr

        df["atr_expansion"] = df.groupby("symbol")["atr"].transform(
            lambda x: x / x.rolling(20).mean()
        )

        # Bear flag
        df["pullback_high_5"] = df.groupby("symbol")["high_price"].transform(
            lambda x: x.rolling(5).max()
        )

        df["bear_flag"] = (
            (df["below_ema"])
            & (df["close_price"] < df["pullback_high_5"])
            & (df["rsi"] > 35)
            & (df["rsi"] < 55)
        )

        # Bull flag
        df["pullback_low_5"] = df.groupby("symbol")["low_price"].transform(
            lambda x: x.rolling(5).min()
        )

        df["bull_flag"] = (
            (df["above_ema"])
            & (df["close_price"] > df["pullback_low_5"])
            & (df["rsi"] > 45)
            & (df["rsi"] < 65)
        )

        if "funding_rate" in df.columns:
            df["funding_extreme"] = df["funding_rate"].abs() > 0.01
        else:
            df["funding_extreme"] = False

        return df

    # -------------------------------------------------------
    # ANALYZE
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

            if row["below_ema"]:
                bias -= 20

            if row["above_ema"]:
                bias += 20

            if row["bos_down_recent"]:
                bias -= 40

            if row["bos_up_recent"]:
                bias += 40

            bias = max(min(bias, 100), -100)

            regime = "Neutral"

            if bias >= 60:
                regime = "Strong Bullish Trend"
            elif bias <= -60:
                regime = "Strong Bearish Trend"

            # SHORT SCORE
            short_score = 0

            if row["below_ema"]:
                short_score += 15

            if row["bos_down_recent"]:
                short_score += 20

            if row["bear_flag"]:
                short_score += 20

            if row["adx"] > 30:
                short_score += 20

            if row["atr_expansion"] > 1.3:
                short_score += 15

            short_score = max(min(short_score, 100), 0)

            # LONG SCORE
            long_score = 0

            if row["above_ema"]:
                long_score += 15

            if row["bos_up_recent"]:
                long_score += 20

            if row["bull_flag"]:
                long_score += 20

            if row["adx"] > 30:
                long_score += 20

            if row["atr_expansion"] > 1.3:
                long_score += 15

            long_score = max(min(long_score, 100), 0)

            direction = "NONE"

            if long_score > short_score:
                direction = "LONG"
            elif short_score > long_score:
                direction = "SHORT"

            results.append({
                "symbol": symbol,
                "regime": regime,
                "direction_bias_score": bias,
                "long_setup_score": long_score,
                "short_setup_score": short_score,
                "preferred_trade_direction": direction,
                "structure_signals": signals
            })

        return {
            "meta": {
                "analysis_time": str(datetime.now()),
                "symbols_analyzed": symbols
            },
            "results": results
        }


# -------------------------------------------------------
# EXPORT REPORT
# -------------------------------------------------------

def export_report_json(output, folder="reports"):

    os.makedirs(folder, exist_ok=True)

    now = datetime.now()
    filename = f"scan_report_{now.strftime('%Y-%m-%d_%H-%M-%S')}.json"
    filepath = os.path.join(folder, filename)

    with open(filepath, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"Report exported to: {filepath}")


# -------------------------------------------------------
# TELEGRAM ALERT
# -------------------------------------------------------

def check_and_send_alert(output):

    triggered = []

    for r in output["results"]:
        if r["regime"] != "Neutral":
            triggered.append(r)

    if not triggered:
        print("No alerts.")
        return

    lines = ["🚨 Market Regime Alert 🚨\n"]

    for r in triggered:
        lines.append(
            f"{r['symbol']} | {r['regime']} | Bias {r['direction_bias_score']}"
        )

    send_telegram_message("\n".join(lines))
    print("Telegram alert sent")


# -------------------------------------------------------
# WAIT FUNCTION
# -------------------------------------------------------

def wait_until_next_hour_close(buffer_minutes=2):

    now = datetime.now(IST)

    if now.minute < 30:
        base_close = now.replace(minute=30, second=0, microsecond=0)
    else:
        base_close = (now + timedelta(hours=1)).replace(minute=30, second=0, microsecond=0)

    next_close = base_close + timedelta(minutes=buffer_minutes)

    wait_seconds = (next_close - now).total_seconds()

    print(f"Waiting {int(wait_seconds)} seconds until next scan")

    time.sleep(max(wait_seconds, 0))


# -------------------------------------------------------
# RUN
# -------------------------------------------------------

if __name__ == "__main__":

    db = SessionLocal()
    stmt = select(distinct(Candle1H.symbol))
    rows = db.execute(stmt).all()
    db.close()

    symbols = [r[0] for r in rows]

    engine = RADX1H(window=60)

    while True:

        try:

            df = engine.fetch_data(symbols)
            df = engine.calculate_indicators(df)

            output = engine.analyze(df, symbols)

            print(json.dumps(output, indent=2))

            export_report_json(output)
            check_and_send_alert(output)

        except Exception as e:
            print("Error:", e)

        wait_until_next_hour_close()