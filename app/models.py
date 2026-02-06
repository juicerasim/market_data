from sqlalchemy import (
    Column, BigInteger, Integer, String, Float, Boolean,
    Index, UniqueConstraint, DateTime
)
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















class CandleBase:
    id = Column(BigInteger, primary_key=True)
    event_time = Column(BigInteger, nullable=False)

    symbol = Column(String(20), nullable=False)

    open_time = Column(BigInteger, nullable=False)
    close_time = Column(BigInteger, nullable=False)

    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)

    base_volume = Column(Float, nullable=False)
    quote_volume = Column(Float, nullable=False)

    trade_count = Column(Integer, nullable=False)
    is_closed = Column(Boolean, nullable=False)


class Candle1M(CandleBase, Base):
    __tablename__ = "candles_1m"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_1m_symbol_time"),
        Index("ix_1m_symbol_time", "symbol", "open_time"),
    )


class Candle15M(CandleBase, Base):
    __tablename__ = "candles_15m"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_15m_symbol_time"),
        Index("ix_15m_symbol_time", "symbol", "open_time"),
    )


class Candle1H(CandleBase, Base):
    __tablename__ = "candles_1h"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_1h_symbol_time"),
        Index("ix_1h_symbol_time", "symbol", "open_time"),
    )


class Candle4H(CandleBase, Base):
    __tablename__ = "candles_4h"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_4h_symbol_time"),
        Index("ix_4h_symbol_time", "symbol", "open_time"),
    )


class Candle1D(CandleBase, Base):
    __tablename__ = "candles_1d"
    __table_args__ = (
        UniqueConstraint("symbol", "open_time", name="uq_1d_symbol_time"),
        Index("ix_1d_symbol_time", "symbol", "open_time"),
    )
