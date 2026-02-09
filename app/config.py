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
