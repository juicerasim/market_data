import os
from dotenv import load_dotenv

load_dotenv()

# ⚠️ SECURITY WARNING: Never commit .env files with real credentials to version control!
# Use environment variables or a secure secrets manager in production.

class Settings:
    # ⚠️ SECURITY: These should come from environment variables, not defaults
    key = os.getenv("API_KEY")
    secret = os.getenv("API_SECRET")
    socketEndpoint = os.getenv("SOCKET_ENDPOINT")

    # Validate that required secrets are present
    if not key or not secret:
        import warnings
        warnings.warn(
            "API credentials (API_KEY, API_SECRET) not found in environment variables. "
            "Please set them before running in production.",
            RuntimeWarning
        )

settings = Settings()

TIMEFRAMES = {
    "1m":  {"tf_ms": 60_000,        "table": "candles_1m",  "api": True},
    # "2m":  {"tf_ms": 120_000,       "table": "candles_2m",  "api": False},
    "5m":  {"tf_ms": 300_000,       "table": "candles_5m",  "api": True},  # derived
    "15m": {"tf_ms": 900_000,       "table": "candles_15m", "api": True},
    "1h":  {"tf_ms": 3_600_000,     "table": "candles_1h",  "api": True},
    # "4h":  {"tf_ms": 14_400_000,    "table": "candles_4h",  "api": True},
    # "1d":  {"tf_ms": 86_400_000,    "table": "candles_1d",  "api": True},
}


