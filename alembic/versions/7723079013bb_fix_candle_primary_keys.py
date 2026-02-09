"""fix candle primary keys

Revision ID: 7723079013bb
Revises: 1c8a3c89ad8b
Create Date: 2026-02-10
"""

from typing import Sequence, Union
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '7723079013bb'
down_revision: Union[str, Sequence[str], None] = '1c8a3c89ad8b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TABLES = [
    "candles_1m",
    "candles_15m",
    "candles_1h",
    "candles_4h",
    "candles_1d",
]


def upgrade() -> None:
    """Apply composite PK (symbol, open_time)"""

    for table in TABLES:
        # Drop existing PK (likely on id)
        op.execute(f"""
            ALTER TABLE {table}
            DROP CONSTRAINT IF EXISTS {table}_pkey;
        """)

        # Add new composite PK
        op.execute(f"""
            ALTER TABLE {table}
            ADD PRIMARY KEY (symbol, open_time);
        """)


def downgrade() -> None:
    """Rollback (remove composite PK only)"""

    for table in TABLES:
        op.execute(f"""
            ALTER TABLE {table}
            DROP CONSTRAINT IF EXISTS {table}_pkey;
        """)
