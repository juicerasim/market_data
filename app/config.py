import os
from dotenv import load_dotenv

load_dotenv()

# ⚠️ SECURITY WARNING: Never commit .env files with real credentials to version control!
# Use environment variables or a secure secrets manager in production.

class Settings:
    # ⚠️ SECURITY: These should come from environment variables, not defaults
    key = os.getenv("API_KEY", "")
    secret = os.getenv("API_SECRET", "")
    socketEndpoint = os.getenv("SOCKET_ENDPOINT", "")

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
"1m": {"table": "candles_1m", "tf_ms": 60_000},
"15m": {"table": "candles_15m", "tf_ms": 15 * 60_000},
"1h": {"table": "candles_1h", "tf_ms": 60 * 60_000},
"4h": {"table": "candles_4h", "tf_ms": 4 * 60 * 60_000},
"1d": {"table": "candles_1d", "tf_ms": 24 * 60 * 60_000},
}
