# Application API and Code Overview

This document provides an overview of the code under the `app/` package and documents the external APIs that the application interacts with.

---

## 🔍 Package Structure

- `app/__init__.py` – package initializer.
- `app/config.py` – configuration using environment variables (`API_KEY`, `API_SECRET`, `SOCKET_ENDPOINT`, `DATABASE_URL`).
- `app/db.py` – SQLAlchemy engine and session setup.
- `app/base.py` – base declarative class for ORM models.
- `app/models.py` – database models representing candle data, open interest, funding rates, etc.
- `app/redis_client.py` – Redis helper (not shown here, but used by scripts).
- `app/symbol_filter.py` – logic for whitelisting/blacklisting trading symbols.
- `app/logging_config.py` – logging setup, exception hooks.
- `app/main.py` – entry point launching workers and housekeeping threads.

### Subpackages

- `app/binance/` – Binance-specific logic:
  - `scripts/` – batch/backfill utilities (`kline_history.py`, `insert.py` etc.).
  - `payload_builder.py` – transforms raw kline arrays into DB payload dictionaries.
  - `repo.py` – helper for writing data to PostgreSQL.
  - `engine/` – realtime components (WebSocket engine, gap watchdog, startup sync).
  - `coins_with_liquidity.py` – process market data for liquid symbols.
  - `ws/` – wrappers around Binance websocket streams.

- `app/coindcx/` – code targeting the CoinDCX exchange.
- `app/repository/` – data access layer (upsert helpers for cdx and normal candles).
- `app/run_models/` – model execution code (e.g., `v1_dlem.py`).

---

## 📦 External APIs

The application communicates with one or more exchange REST APIs to fetch historical / live market data.

### Binance Futures REST API

| Endpoint | Purpose |
|---------|---------|
| `GET https://fapi.binance.com/fapi/v1/klines` | Retrieve candlestick (kline) data. Used in `app/binance/scripts/kline_history.py`.

#### Parameters

- `symbol` (string) – trading pair (e.g. `BTCUSDT`).
- `interval` (string) – timeframe (`1m`, `15m`, `1h`, etc.).
- `startTime` / `endTime` (ms since epoch) – optional date range.
- `limit` (integer) – maximum number of candles per request (up to 1000; script uses 500).

The response is a JSON array of arrays; `payload_builder.build_payloads` maps fields into the internal schema.

### CoinDCX / other exchanges

(If additional API endpoints are used by `app/coindcx` or in other modules, document them here similarly.)

---

## 🛠 Internal API / Helper Functions

While not a web‑service, the package exposes several programmatic interfaces useful for automation:

### `app.repository.upsert_klines(klines: list)`
Insert or update batches of candle records into the `cdx_candles_*` tables.

### `app.binance.payload_builder.build_payloads(symbol, interval, klines)`
Convert raw REST API klines into dictionaries ready for database insertion.

### Database models in `app/models.py`
Enumerate the available ORM classes and important columns:
- `Candle1M`, `Candle15M`, `Candle1H`, etc. with composite PK `(symbol, open_time)`.
- `CDXCandle1M` … with `id`, unique constraint on `(symbol, open_time)`.
- `OpenInterest1H`, `FundingRate8H`.

Refer to the source file for full schema details and default values.

---

## ⚙️ Configuration & Environment

All credentials and service URLs are provided via environment variables. The most important keys are:
- `DATABASE_URL`: SQLAlchemy connection string for PostgreSQL.
- `API_KEY`, `API_SECRET`: Binance API credentials (warns on missing in `config.py`).
- `SOCKET_ENDPOINT`: WebSocket endpoint for live feeds (if used).

`SETUP_AFTER_SECURITY_FIX.md` in the repository contains guidance for regenerating and supplying API keys securely.

---

## 🚀 Running the Pipeline

1. Ensure `DATABASE_URL` and exchange credentials are set.
2. Apply migrations with `poetry run alembic upgrade head`.
3. Execute `poetry run python -m app.main` to start the supervisor process (launches sync, workers, gap watchdog).
4. For backfill: run `poetry run python -m app.binance.scripts.kline_history` with appropriate arguments.

---

## 📄 Further Documentation

- Add per-module docstrings where missing.
- Consider generating Sphinx/Markdown docs from docstrings if the API surface expands.

This file should be updated whenever new endpoints or internal modules are added.
