#!/usr/bin/env python3

# pressure_scan_1h.py

from datetime import datetime
from typing import List, Optional
import logging
import json
import time
import os

import pandas as pd
from sqlalchemy import select
from app.models import Candle1H, OpenInterest1H, FundingRate8H
from app.db import SessionLocal


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# MODEL
# --------------------------------------------------

class DerivativesModel1H:

    def __init__(self, window: int = 60):
        self.window = window

    # --------------------------------------------------
    # FETCH STRICT LAST N CANDLES
    # --------------------------------------------------

    def fetch_data(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:

        db = SessionLocal()

        try:
            stmt = select(Candle1H)

            if symbols:
                stmt = stmt.where(Candle1H.symbol.in_(symbols))

            stmt = stmt.order_by(Candle1H.symbol, Candle1H.open_time)
            candles = db.execute(stmt).scalars().all()

            if not candles:
                raise ValueError("No candle data found.")

            df = pd.DataFrame([c.__dict__ for c in candles])
            df.drop(columns=["_sa_instance_state"], inplace=True)

            df = (
                df.sort_values(["symbol", "open_time"])
                .groupby("symbol")
                .tail(self.window)
            )

            counts = df.groupby("symbol").size()
            if (counts < self.window).any():
                raise ValueError("Insufficient candles for strict window requirement")

            # OI JOIN
            oi_stmt = select(OpenInterest1H)

            if symbols:
                oi_stmt = oi_stmt.where(OpenInterest1H.symbol.in_(symbols))

            oi_records = db.execute(oi_stmt).scalars().all()

            if not oi_records:
                raise ValueError("No OI records found.")

            oi_df = pd.DataFrame([o.__dict__ for o in oi_records])
            oi_df.drop(columns=["_sa_instance_state"], inplace=True)

            oi_df = oi_df.rename(columns={"open_interest": "oi_external"})

            df = df.merge(
                oi_df[["symbol", "open_time", "oi_external"]],
                on=["symbol", "open_time"],
                how="left"
            )

            if df["oi_external"].isna().any():
                raise ValueError("Missing OI values after merge")

            df["open_interest"] = df["oi_external"]
            df.drop(columns=["oi_external"], inplace=True)

            # FUNDING MAP
            funding_stmt = select(FundingRate8H)

            if symbols:
                funding_stmt = funding_stmt.where(FundingRate8H.symbol.in_(symbols))

            funding_records = db.execute(funding_stmt).scalars().all()

            if not funding_records:
                raise ValueError("No funding records found.")

            funding_df = pd.DataFrame([f.__dict__ for f in funding_records])
            funding_df.drop(columns=["_sa_instance_state"], inplace=True)

            funding_df = funding_df.sort_values(["symbol", "funding_time"])
            df = df.sort_values(["symbol", "open_time"])

            df = pd.merge_asof(
                df,
                funding_df,
                left_on="open_time",
                right_on="funding_time",
                by="symbol",
                direction="backward"
            )

            if df["funding_rate"].isna().any():
                raise ValueError("Funding mapping incomplete")

            return df

        finally:
            db.close()

    # --------------------------------------------------
    # INDICATORS
    # --------------------------------------------------

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:

        df["prev_close"] = df.groupby("symbol")["close_price"].shift(1)

        df["tr"] = df.apply(
            lambda row: max(
                row["high_price"] - row["low_price"],
                abs(row["high_price"] - row["prev_close"]),
                abs(row["low_price"] - row["prev_close"]),
            ),
            axis=1,
        )

        df["atr20"] = (
            df.groupby("symbol")["tr"]
            .rolling(20)
            .mean()
            .reset_index(level=0, drop=True)
        )

        df["range_ratio"] = (
            (df["high_price"] - df["low_price"]) / df["atr20"]
        )

        df["oi_delta_percent"] = (
            df.groupby("symbol")["open_interest"]
            .pct_change() * 100
        )

        df["oi_build_6h"] = (
            df.groupby("symbol")["oi_delta_percent"]
            .rolling(6)
            .sum()
            .reset_index(level=0, drop=True)
        )

        df["buy_ratio"] = (
            df["taker_buy_base_volume"] / df["base_volume"]
        )

        return df

    # --------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------

    def detect_structure(self, df: pd.DataFrame) -> pd.DataFrame:

        df["lowest_12h"] = (
            df.groupby("symbol")["low_price"]
            .rolling(12)
            .min()
            .reset_index(level=0, drop=True)
        )

        df["prev_lowest_12h"] = df.groupby("symbol")["lowest_12h"].shift(12)
        df["higher_low"] = df["lowest_12h"] > df["prev_lowest_12h"]

        return df

    # --------------------------------------------------
    # SCORING
    # --------------------------------------------------

    def score(self, row):

        compression_score = 0
        if row["range_ratio"] < 0.8:
            compression_score = min((0.8 - row["range_ratio"]) * 50, 30)

        oi_score = min(abs(row["oi_build_6h"]) * 30, 30)
        structure_score = 20 if row["higher_low"] else 0

        expansion_score = compression_score + oi_score + structure_score

        bias = 0
        if row["oi_build_6h"] > 1:
            bias += 40
        if row["buy_ratio"] > 0.55:
            bias += 20
        if row["higher_low"]:
            bias += 20
        if row["oi_build_6h"] < -1:
            bias -= 40

        bias = max(min(bias, 100), -100)

        return round(expansion_score, 1), bias

    # --------------------------------------------------
    # ANALYZE (STRICT FORMAT)
    # --------------------------------------------------

    def analyze(self, df: pd.DataFrame, symbols: Optional[List[str]] = None):

        latest = df.groupby("symbol").tail(1)
        results = []

        for _, row in latest.iterrows():

            expansion_score, bias = self.score(row)

            if expansion_score > 70:
                condition = "Big move happening or very near"
            elif expansion_score > 40:
                condition = "Pressure building"
            else:
                condition = "No strong setup"

            if expansion_score > 70:
                if bias > 30:
                    comment = "Strong pressure building with bullish positioning. Upside breakout likely."
                elif bias < -30:
                    comment = "Strong pressure building with bearish positioning. Downside breakout likely."
                else:
                    comment = "Strong pressure building but direction is not clear yet."
            elif expansion_score > 40:
                if row["oi_build_6h"] > 1:
                    comment = "Price is moving in a tight range and pressure is building. Open interest is expanding positively. Watch for upside breakout."
                elif row["oi_build_6h"] < -1:
                    comment = "Price is moving in a tight range and pressure is building. Open interest is expanding negatively. Watch for downside breakout."
                else:
                    comment = "Price is moving in a tight range and pressure is building. Open interest is not strongly bullish or bearish yet. Wait for stronger build above +1% or below -1% before taking directional trade."
            else:
                comment = "No strong pressure setup. Market is normal."

            results.append({
                "symbol": row["symbol"],
                "range_ratio": round(row["range_ratio"], 2),
                "ideal_compression": "< 0.8",
                "oi_build_6h": round(row["oi_build_6h"], 2),
                "ideal_oi_build": "> +1% or < -1%",
                "direction_bias_score": bias,
                "bias_scale": "-100 to +100",
                "expansion_score": expansion_score,
                "expansion_scale": "0 to 100",
                "market_condition": condition,
                "comment": comment
            })

        return {
            "meta": {
                "analysis_time_ist": datetime.now().isoformat(),
                "timeframe": "1H",
                "window_used": f"{self.window} candles",
                "symbols_analyzed": symbols if symbols else list(latest["symbol"].unique()),
                "date_range_ist": {
                    "start": df["lk_at"].min().isoformat(),
                    "end": df["lk_at"].max().isoformat(),
                },
                "total_symbols": len(results),
                "total_rows_analyzed": len(df)
            },
            "results": results
        }


# --------------------------------------------------
# MARKDOWN EXPORT
# --------------------------------------------------

def export_report_to_markdown(report: dict, output_dir: str = "reports"):

    os.makedirs(output_dir, exist_ok=True)

    dt = datetime.fromisoformat(report["meta"]["analysis_time_ist"])
    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")

    filename = f"scan_report_{formatted_time}.md"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:

        f.write("# 📊 Pressure Scan Report\n\n")

        # --------------------------------------------------
        # META
        # --------------------------------------------------
        f.write("## 🔎 Meta Information\n\n")
        f.write(f"- **Analysis Time (IST):** `{report['meta']['analysis_time_ist']}`\n")
        f.write(f"- **Timeframe:** `{report['meta']['timeframe']}`\n")
        f.write(f"- **Window Used:** `{report['meta']['window_used']}`\n")
        f.write(f"- **Symbols Analyzed:** `{', '.join(report['meta']['symbols_analyzed'])}`\n")
        f.write(f"- **Date Range:** `{report['meta']['date_range_ist']['start']} → {report['meta']['date_range_ist']['end']}`\n")
        f.write(f"- **Total Symbols:** `{report['meta']['total_symbols']}`\n")
        f.write(f"- **Total Rows Analyzed:** `{report['meta']['total_rows_analyzed']}`\n\n")

        # --------------------------------------------------
        # RESULTS
        # --------------------------------------------------
        f.write("## 📈 Scan Results\n\n")

        for result in report["results"]:

            expansion = result["expansion_score"]
            bias = result["direction_bias_score"]

            # Expansion highlight
            if expansion > 70:
                expansion_display = f"🔴 **{expansion}**"
            elif expansion > 40:
                expansion_display = f"🟡 **{expansion}**"
            else:
                expansion_display = f"{expansion}"

            # Bias highlight
            if bias > 40:
                bias_display = f"🟢 **{bias}**"
            elif bias < -40:
                bias_display = f"🔻 **{bias}**"
            else:
                bias_display = f"{bias}"

            f.write(f"### 🪙 {result['symbol']}\n\n")

            f.write(f"- **Range Ratio:** `{result['range_ratio']}`  _(Ideal: {result['ideal_compression']})_\n")
            f.write(f"- **OI Build (6H):** `{result['oi_build_6h']}`  _(Ideal: {result['ideal_oi_build']})_\n")
            f.write(f"- **Direction Bias Score:** {bias_display}  _(Scale: {result['bias_scale']})_\n")
            f.write(f"- **Expansion Score:** {expansion_display}  _(Scale: {result['expansion_scale']})_\n")
            f.write(f"- **Market Condition:** **{result['market_condition']}**\n\n")

            f.write(f"> {result['comment']}\n\n")
            f.write("---\n\n")

    logger.info(f"Markdown report exported: {filepath}")


# --------------------------------------------------
# SCHEDULER
# --------------------------------------------------

def sleep_until_next_interval(interval_minutes: int):

    if 60 % interval_minutes != 0:
        raise ValueError("interval_minutes must divide 60 evenly")

    now = datetime.now()
    next_minute = ((now.minute // interval_minutes) + 1) * interval_minutes

    if next_minute >= 60:
        next_run = now.replace(hour=(now.hour + 1) % 24, minute=0, second=0, microsecond=0)
    else:
        next_run = now.replace(minute=next_minute, second=0, microsecond=0)

    sleep_seconds = (next_run - now).total_seconds()
    if sleep_seconds < 0:
        sleep_seconds = 0

    logger.info(f"Sleeping {int(sleep_seconds)} seconds until next cycle...")
    time.sleep(sleep_seconds)


# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

if __name__ == "__main__":

    engine = DerivativesModel1H(window=60)
    symbols_to_scan = ['PIPPINUSDT']
    interval_minutes = 30

    logger.info("Pressure Scan 1H Service Started")

    while True:
        try:
            logger.info("Running scan...")

            df = engine.fetch_data(symbols=symbols_to_scan)
            df = engine.calculate_indicators(df)
            df = engine.detect_structure(df)

            output = engine.analyze(df, symbols=symbols_to_scan)

            print(json.dumps(output, indent=2, default=str))

            export_report_to_markdown(output)

        except Exception as e:
            logger.exception(f"Scan failed: {e}")

        sleep_until_next_interval(interval_minutes)