"""
SQLAlchemy ORM 모델 — coinmarket-data 서비스용

ck_candles: Coincheck 캔들 (기존 테이블, coincheck-trader에서 이관)
bf_candles: BitFlyer 캔들 (신규 테이블)
economic_events: 경제 캘린더 (ForexFactory, F-01 알파 팩터)
intermarket_data: FRED 매크로 지표 (F-04 알파 팩터)
"""
from sqlalchemy import (
    Column, BigInteger, Integer, String, Boolean,
    DateTime, Numeric, Index, PrimaryKeyConstraint, UniqueConstraint,
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


class EconomicEvent(Base):
    """
    경제 캘린더 이벤트 (ForexFactory JSON API 기반, F-01 알파 팩터).

    수집 주기: 6시간마다 thisweek + nextweek 교대 수집.
    영향 통화: USD, JPY, GBP, EUR (FX 전략 연관 통화만 필터링).
    impact: High, Medium만 저장 (Low/Holiday 제외).
    """
    __tablename__ = "economic_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    country = Column(String(3), nullable=False)          # USD, JPY, GBP, EUR
    event_time = Column(DateTime(timezone=True), nullable=False)
    impact = Column(String(10), nullable=False)          # High, Medium
    forecast = Column(String(50), nullable=True)
    previous = Column(String(50), nullable=True)
    actual = Column(String(50), nullable=True)           # 발표 후 업데이트
    source = Column(String(20), nullable=False, default="forexfactory")
    fetched_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("title", "event_time", name="uq_economic_events_title_time"),
        Index("idx_economic_events_time", "event_time"),
        Index("idx_economic_events_country", "country"),
    )

    def __repr__(self):
        return f"<EconomicEvent {self.country} '{self.title}' at={self.event_time} impact={self.impact}>"


class IntermarketData(Base):
    """
    FRED 매크로 경제 지표 (F-04 알파 팩터).

    수집 주기: 매일 08:00 JST (FRED는 T+1 영업일 지연).
    series_id: DGS10 / DGS2 / T10Y2Y / DEXJPUS / DTWEXBGS / VIXCLS
    """
    __tablename__ = "intermarket_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(String(30), nullable=False)       # DGS10, VIXCLS 등
    obs_date = Column(DateTime(timezone=False), nullable=False)  # DATE (UTC midnight)
    value = Column(Numeric(12, 4), nullable=True)        # FRED는 결측치(".") 가능
    source = Column(String(20), nullable=False, default="fred")
    fetched_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("series_id", "obs_date", name="uq_intermarket_series_date"),
        Index("idx_intermarket_series_date", "series_id", "obs_date"),
    )

    def __repr__(self):
        return f"<IntermarketData {self.series_id} {self.obs_date} val={self.value}>"
