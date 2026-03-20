"""
SQLAlchemy ORM 모델 — coinmarket-data 서비스용

ck_candles: Coincheck 캔들 (기존 테이블, coincheck-trader에서 이관)
bf_candles: BitFlyer 캔들 (신규 테이블)
"""
from sqlalchemy import (
    Column, BigInteger, Integer, String, Boolean,
    DateTime, Numeric, Index, PrimaryKeyConstraint,
)
from sqlalchemy.sql import func
from app.database import Base


class CkCandle(Base):
    """
    Coincheck OHLCV 캔들 (1H / 4H)
    WS 틱 스트림 + REST 백필로 구성
    """
    __tablename__ = "ck_candles"

    pair = Column(String(20), nullable=False)
    timeframe = Column(String(5), nullable=False)    # "1h" | "4h"
    open_time = Column(DateTime(timezone=True), nullable=False)
    close_time = Column(DateTime(timezone=True), nullable=False)
    open = Column(Numeric(18, 8), nullable=False)
    high = Column(Numeric(18, 8), nullable=False)
    low = Column(Numeric(18, 8), nullable=False)
    close = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(18, 8), nullable=False, default=0)
    tick_count = Column(Integer, nullable=False, default=0)
    is_complete = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("pair", "timeframe", "open_time", name="ck_candles_pkey"),
        Index("idx_ck_candles_lookup", "pair", "timeframe", "open_time"),
        Index("idx_ck_candles_incomplete", "pair", "timeframe", "is_complete"),
    )

    def __repr__(self):
        return f"<CkCandle {self.pair} {self.timeframe} {self.open_time} complete={self.is_complete}>"


class BfCandle(Base):
    """
    BitFlyer OHLCV 캔들 (1H / 4H)
    WS 틱 스트림 + REST 백필로 구성
    """
    __tablename__ = "bf_candles"

    product_code = Column(String(20), nullable=False)   # BTC_JPY 등
    timeframe = Column(String(5), nullable=False)         # "1h" | "4h"
    open_time = Column(DateTime(timezone=True), nullable=False)
    close_time = Column(DateTime(timezone=True), nullable=False)
    open = Column(Numeric(18, 8), nullable=False)
    high = Column(Numeric(18, 8), nullable=False)
    low = Column(Numeric(18, 8), nullable=False)
    close = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(18, 8), nullable=False, default=0)
    tick_count = Column(Integer, nullable=False, default=0)
    is_complete = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("product_code", "timeframe", "open_time", name="bf_candles_pkey"),
        Index("idx_bf_candles_lookup", "product_code", "timeframe", "open_time"),
        Index("idx_bf_candles_incomplete", "product_code", "timeframe", "is_complete"),
    )

    def __repr__(self):
        return f"<BfCandle {self.product_code} {self.timeframe} {self.open_time} complete={self.is_complete}>"


class BfFundingRate(Base):
    """
    BitFlyer 펀딩레이트 수집 이력 (15분 폴링)
    Public API: GET /v1/getfundingrate
    """
    __tablename__ = "bf_funding_rates"

    id = Column(BigInteger, autoincrement=True, nullable=False)
    product_code = Column(String(20), nullable=False)
    current_funding_rate = Column(Numeric(12, 8), nullable=False)
    next_settlement_date = Column(DateTime(timezone=True), nullable=True)
    collected_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("id", name="bf_funding_rates_pkey"),
        Index("idx_bf_funding_rates_lookup", "product_code", "collected_at"),
    )

    def __repr__(self):
        return f"<BfFundingRate {self.product_code} rate={self.current_funding_rate} at={self.collected_at}>"


class GmoCandle(Base):
    """
    GMO FX OHLCV 캔들 (1H / 4H)
    KLine API 폴링으로 수집 (tick 집계 아님)
    Volume: GMO FX는 volume 미제공 → 0 고정
    """
    __tablename__ = "gmo_candles"

    pair = Column(String(20), nullable=False)             # usd_jpy 등
    timeframe = Column(String(5), nullable=False)         # "1h" | "4h"
    open_time = Column(DateTime(timezone=True), nullable=False)
    close_time = Column(DateTime(timezone=True), nullable=False)
    open = Column(Numeric(18, 8), nullable=False)
    high = Column(Numeric(18, 8), nullable=False)
    low = Column(Numeric(18, 8), nullable=False)
    close = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(18, 8), nullable=False, default=0)
    tick_count = Column(Integer, nullable=False, default=0)
    is_complete = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("pair", "timeframe", "open_time", name="gmo_candles_pkey"),
        Index("idx_gmo_candles_lookup", "pair", "timeframe", "open_time"),
        Index("idx_gmo_candles_incomplete", "pair", "timeframe", "is_complete"),
    )

    def __repr__(self):
        return f"<GmoCandle {self.pair} {self.timeframe} {self.open_time} complete={self.is_complete}>"
