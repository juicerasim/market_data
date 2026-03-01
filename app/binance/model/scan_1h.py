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

            # ---------------- FUNDING (PER SYMBOL SAFE MERGE) ----------------
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
                )

                # VERY IMPORTANT: Drop symbol from right
                right = right.drop(columns=["symbol"])

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
    # ANALYZE
    # --------------------------------------------------

    def analyze(self, df: pd.DataFrame):

        latest = df.groupby("symbol").tail(1)

        results = []

        for _, row in latest.iterrows():

            expansion_score = 0
            if row["range_ratio"] < 0.8:
                expansion_score += 30
            expansion_score += min(abs(row["oi_build_6h"]) * 30, 30)
            if row["oi_build_6h"] > 1:
                bias = 40
            elif row["oi_build_6h"] < -1:
                bias = -40
            else:
                bias = 0

            results.append({
                "symbol": row["symbol"],
                "range_ratio": round(row["range_ratio"], 2),
                "oi_build_6h": round(row["oi_build_6h"], 2),
                "expansion_score": round(expansion_score, 1),
                "direction_bias_score": bias
            })

        return {
            "meta": {
                "analysis_time_ist": datetime.now().isoformat(),
                "timeframe": "1H"
            },
            "results": results
        }


# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

if __name__ == "__main__":

    engine = DerivativesModel1H(window=60)
    symbols_to_scan = ['ETHUSDT', 'PIPPINUSDT']

    logger.info("Pressure Scan 1H Service Started")

    while True:
        try:
            logger.info("Running scan...")

            df = engine.fetch_data(symbols=symbols_to_scan)
            df = engine.calculate_indicators(df)

            output = engine.analyze(df)

            print(json.dumps(output, indent=2, default=str))

        except Exception as e:
            logger.exception(f"Scan failed: {e}")

        time.sleep(1800)