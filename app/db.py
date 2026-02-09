import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# ⚠️ SECURITY: Load DATABASE_URL from environment variable
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://trader:admin1234@localhost:5432/market_data"  # Default only for development
)

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable not set. "
        "Please set it before running the application."
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
    },
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)


