# Market Data Pipeline

This repository contains a service for consuming and storing cryptocurrency market data.

See [app/API_DOCUMENTATION.md](app/API_DOCUMENTATION.md) for a detailed overview of the `app/` package and the external APIs it uses.

## Getting Started

1. Install dependencies using Poetry: `poetry install`.
2. Configure environment variables (`DATABASE_URL`, `API_KEY`, `API_SECRET`, etc.).
3. Run database migrations: `poetry run alembic upgrade head`.
4. Launch the pipeline with `poetry run python -m app.main` or run individual scripts from `app/binance/scripts`.
