#!/usr/bin/env python3

from datetime import datetime
from typing import List, Optional
import logging
import json
import time

import pandas as pd
from sqlalchemy import select
from app.models import Candle1H, OpenInterest1H, FundingRate8H
from app.db import SessionLocal


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


class DerivativesModel1H:

    def __init__(self, window: int = 60):
        self.window = window

    # --------------------------------------------------
    # FETCH DATA
    # --------------------------------------------------

    def fetch_data(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:

        db = SessionLocal()

        try:
            # ---------------- CANDLES ----------------
            stmt = select(Candle1H)

            if symbols:
                stmt = stmt.where(Candle1H.symbol.in_(symbols))

            stmt = stmt.order_by(Candle1H.symbol, Candle1H.open_time)

            candles = db.execute(stmt).scalars().all()
            if not candles:
                raise ValueError("No candle data found.")

            df = pd.DataFrame([c.__dict__ for c in candles])
            df.drop(columns=["_sa_instance_state"], inplace=True)

            df["open_time"] = pd.to_datetime(df["open_time"])

            df = (
                df.sort_values(["symbol", "open_time"])
                .groupby("symbol")
                .tail(self.window)
                .reset_index(drop=True)
            )

            # ---------------- OPEN INTEREST ----------------
            oi_stmt = select(OpenInterest1H)

            if symbols:
                oi_stmt = oi_stmt.where(OpenInterest1H.symbol.in_(symbols))

            oi_records = db.execute(oi_stmt).scalars().all()
            if not oi_records:
                raise ValueError("No OI records found.")

            oi_df = pd.DataFrame([o.__dict__ for o in oi_records])
            oi_df.drop(columns=["_sa_instance_state"], inplace=True)
            oi_df["open_time"] = pd.to_datetime(oi_df["open_time"])

            df = df.merge(
                oi_df[["symbol", "open_time", "open_interest"]],
                on=["symbol", "open_time"],
                how="left"
            )

            if df["open_interest"].isna().any():
                raise ValueError("Missing OI values after merge")

            # ---------------- FUNDING ----------------
            funding_stmt = select(FundingRate8H)

            if symbols:
                funding_stmt = funding_stmt.where(FundingRate8H.symbol.in_(symbols))

            funding_records = db.execute(funding_stmt).scalars().all()
            if not funding_records:
                raise ValueError("No funding records found.")

            funding_df = pd.DataFrame([f.__dict__ for f in funding_records])
            funding_df.drop(columns=["_sa_instance_state"], inplace=True)
            funding_df["funding_time"] = pd.to_datetime(funding_df["funding_time"])

            merged_frames = []

            for sym in df["symbol"].unique():

                left = (
                    df[df["symbol"] == sym]
                    .sort_values("open_time")
                    .reset_index(drop=True)
                )

                right = (
                    funding_df[funding_df["symbol"] == sym]
                    .sort_values("funding_time")
                    .reset_index(drop=True)
                    .drop(columns=["symbol"])
                )

                merged = pd.merge_asof(
                    left,
                    right,
                    left_on="open_time",
                    right_on="funding_time",
                    direction="backward"
                )

                if merged["funding_rate"].isna().any():
                    raise ValueError(f"Funding mapping incomplete for {sym}")

                merged_frames.append(merged)

            df = pd.concat(merged_frames).reset_index(drop=True)

            return df

        finally:
            db.close()

    # --------------------------------------------------
    # INDICATORS
    # --------------------------------------------------

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)

        df["prev_close"] = df.groupby("symbol")["close_price"].shift(1)

        df["tr"] = (
            pd.concat([
                df["high_price"] - df["low_price"],
                (df["high_price"] - df["prev_close"]).abs(),
                (df["low_price"] - df["prev_close"]).abs()
            ], axis=1)
            .max(axis=1)
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
    # SCORING
    # --------------------------------------------------

    def score(self, row):

        compression_score = 0
        if row["range_ratio"] < 0.8:
            compression_score = min((0.8 - row["range_ratio"]) * 50, 30)

        oi_score = min(abs(row["oi_build_6h"]) * 30, 30)

        expansion_score = compression_score + oi_score

        bias = 0
        if row["oi_build_6h"] > 1:
            bias += 40
        if row["buy_ratio"] > 0.55:
            bias += 20
        if row["oi_build_6h"] < -1:
            bias -= 40

        bias = max(min(bias, 100), -100)

        return round(expansion_score, 1), bias

    # --------------------------------------------------
    # COMMENT
    # --------------------------------------------------

    def generate_comment(self, row, expansion_score, bias):

        if expansion_score > 70:
            if bias > 30:
                return "Strong pressure building with bullish positioning. Upside breakout likely."
            elif bias < -30:
                return "Strong pressure building with bearish positioning. Downside breakout likely."
            else:
                return "Strong pressure building but direction is not clear yet."

        elif expansion_score > 40:
            if row["oi_build_6h"] > 1:
                return "Price is moving in a tight range and pressure is building. Open interest is expanding positively. Watch for upside breakout."
            elif row["oi_build_6h"] < -1:
                return "Price is moving in a tight range and pressure is building. Open interest is expanding negatively. Watch for downside breakout."
            else:
                return "Price is moving in a tight range and pressure is building. Open interest is not strongly bullish or bearish yet. Wait for stronger build above +1% or below -1% before taking directional trade."

        else:
            return "No strong pressure setup. Market is normal."

    # --------------------------------------------------
    # ANALYZE
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

            comment = self.generate_comment(row, expansion_score, bias)

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
                    "start": df["open_time"].min().isoformat(),
                    "end": df["open_time"].max().isoformat(),
                },
                "total_symbols": len(results),
                "total_rows_analyzed": len(df)
            },
            "results": results
        }


# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

if __name__ == "__main__":

    engine = DerivativesModel1H(window=60)
    symbols_to_scan = ['ETHUSDT', 'PIPPINUSDT']
    # symbols_to_scan = None

    logger.info("Pressure Scan 1H Service Started")

    while True:
        try:
            logger.info("Running scan...")

            df = engine.fetch_data(symbols=symbols_to_scan)
            df = engine.calculate_indicators(df)

            output = engine.analyze(df, symbols=symbols_to_scan)

            print(json.dumps(output, indent=2, default=str))

        except Exception as e:
            logger.exception(f"Scan failed: {e}")

        time.sleep(1800)