from sqlalchemy import (
    Column, BigInteger, Integer, String, Float, Boolean,
    Index, UniqueConstraint, DateTime, Identity
)
import sqlalchemy as sa
from app.base import Base


from sqlalchemy import (
    Column, BigInteger, Integer, String, Float, Boolean
)
from sqlalchemy.sql import func


class CDXCandleBase:
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Exchange identity
    pair = Column(String(30), nullable=False)
    symbol = Column(String(20), nullable=False)

    duration = Column(String(10), nullable=False)

    # Exchange timestamps (unix seconds)
    open_time = Column(BigInteger, nullable=False)
    close_time = Column(BigInteger, nullable=False)

    # OHLC
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)

    # volumes
    base_volume = Column(Float, nullable=False)
    quote_volume = Column(Float, nullable=False)

    # derived internally from stream logic
    is_closed = Column(Boolean, nullable=False, default=False)

    created_at = Column(
    DateTime(timezone=True),
    nullable=True,
    server_default=func.now()   # ⭐ ADD THIS
)



class CDXCandle1M(CDXCandleBase, Base):
    __tablename__ = "cdx_candles_1m"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_cdx_1m_symbol_time"),
        Index("ix_cdx_1m_symbol_time", "symbol", "open_time"),
    )


class CDXCandle15M(CDXCandleBase, Base):
    __tablename__ = "cdx_candles_15m"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_cdx_15m_symbol_time"),
        Index("ix_cdx_15m_symbol_time", "symbol", "open_time"),
    )


class CDXCandle1H(CDXCandleBase, Base):
    __tablename__ = "cdx_candles_1h"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_cdx_1h_symbol_time"),
        Index("ix_cdx_1h_symbol_time", "symbol", "open_time"),
    )


class CDXCandle4H(CDXCandleBase, Base):
    __tablename__ = "cdx_candles_4h"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_cdx_4h_symbol_time"),
        Index("ix_cdx_4h_symbol_time", "symbol", "open_time"),
    )


class CDXCandle1D(CDXCandleBase, Base):
    __tablename__ = "cdx_candles_1d"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_cdx_1d_symbol_time"),
        Index("ix_cdx_1d_symbol_time", "symbol", "open_time"),
    )











# -------------------------------------------------
# Base Candle Model (NO PRIMARY KEY HERE)
# -------------------------------------------------
class CandleBase:
    # event info
    id = Column(
    BigInteger,
    Identity(always=True),   # ⭐ THIS enables auto increment
    unique=True,
    index=True
)

    lk_at = sa.Column(
    sa.DateTime(timezone=True),
    nullable=True
)
    event_time = Column(BigInteger, nullable=True)
    interval = Column(String(20), nullable=True)

    # symbol = Column(String(20), nullable=False)
    # open_time = Column(BigInteger, nullable=False)
    close_time = Column(BigInteger, nullable=False)

    # trade id range
    first_trade_id = Column(BigInteger, nullable=True)
    last_trade_id = Column(BigInteger, nullable=True)

    # OHLC
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)

    # volumes
    base_volume = Column(Float, nullable=False)
    quote_volume = Column(Float, nullable=False)

    taker_buy_base_volume = Column(Float, nullable=False)
    taker_buy_quote_volume = Column(Float, nullable=False)

    # stats
    trade_count = Column(Integer, nullable=False)
    is_closed = Column(Boolean, nullable=True)
    created_at = Column(
    DateTime(timezone=True),
    nullable=True,
    server_default=func.now()   # ⭐ ADD THIS
)


# -------------------------------------------------
# 1 Minute Candle
# -------------------------------------------------
class Candle1M(CandleBase, Base):
    __tablename__ = "candles_1m"

    # ⭐ Composite Primary Key
    symbol = Column(String(20), primary_key=True)
    open_time = Column(BigInteger, primary_key=True)


# -------------------------------------------------
# 15 Minute Candle
# -------------------------------------------------
class Candle15M(CandleBase, Base):
    __tablename__ = "candles_15m"

    symbol = Column(String(20), primary_key=True)
    open_time = Column(BigInteger, primary_key=True)


# -------------------------------------------------
# 1 Hour Candle
# -------------------------------------------------
class Candle1H(CandleBase, Base):
    __tablename__ = "candles_1h"

    symbol = Column(String(20), primary_key=True)
    open_time = Column(BigInteger, primary_key=True)


# -------------------------------------------------
# 4 Hour Candle
# -------------------------------------------------
class Candle4H(CandleBase, Base):
    __tablename__ = "candles_4h"

    symbol = Column(String(20), primary_key=True)
    open_time = Column(BigInteger, primary_key=True)


# -------------------------------------------------
# 1 Day Candle
# -------------------------------------------------
class Candle1D(CandleBase, Base):
    __tablename__ = "candles_1d"

    symbol = Column(String(20), primary_key=True)
    open_time = Column(BigInteger, primary_key=True)

