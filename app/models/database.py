"""
SQLAlchemy ORM 모델 — trading-data 서비스용

gmoc_candles: GMO 코인 OHLCV 캔들
economic_events: 경제 캘린더 (ForexFactory, F-01 알파 팩터)
intermarket_data: FRED 매크로 지표 (F-04 알파 팩터)
sentiment_scores: Fear & Greed Index (Alternative.me API)
"""
from sqlalchemy import (
    Column, BigInteger, Integer, String, Boolean,
    DateTime, Numeric, Index, PrimaryKeyConstraint, UniqueConstraint,
)
from sqlalchemy.sql import func
from app.database import Base


class SentimentScore(Base):
    """
    センチメント스코어 (Alternative.me Fear & Greed Index 수집).

    수집 소스: Alternative.me FNG API (무료, 레이트 리밋 없음)
    수집 주기: 1시간 (FNG 갱신 주기와 동일)
    score: 0~100 (0=Extreme Fear, 100=Extreme Greed)
    classification: Extreme Fear | Fear | Neutral | Greed | Extreme Greed
    raw_timestamp: FNG API의 Unix timestamp (갱신 시각)
    """
    __tablename__ = "sentiment_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False)          # "alternative_me_fng"
    score = Column(Integer, nullable=False)               # 0~100
    classification = Column(String(30), nullable=False)  # "Extreme Fear" | ... | "Extreme Greed"
    fetched_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    raw_timestamp = Column(BigInteger, nullable=True)    # FNG API timestamp

    __table_args__ = (
        Index("idx_sentiment_scores_source_fetched", "source", "fetched_at"),
    )

    def __repr__(self):
        return f"<SentimentScore source={self.source} score={self.score} [{self.classification}]>"


class NewsArticle(Base):
    """
    금융 뉴스 기사 (Marketaux API 수집).

    수집 소스: Marketaux (무료 100건/일, 실시간, sentiment 내장)
    수집 주기: 15분 (쿼리 라운드 로빈: crypto → forex → macro)
    sentiment_score: entities[].sentiment_score 평균 (-1.0 ~ 1.0)
    category: crypto | forex | macro (검색 쿼리 기반 분류)
    """
    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(50), nullable=False, unique=True)       # Marketaux UUID
    title = Column(String(500), nullable=False)
    snippet = Column(String(1000), nullable=True)                # 기사 요약
    source = Column(String(100), nullable=False)                 # 도메인 (reuters.com 등)
    url = Column(String(1000), nullable=False)
    published_at = Column(DateTime(timezone=True), nullable=False)
    language = Column(String(5), nullable=False, default="en")
    symbols = Column(String(200), nullable=True)                 # 쉼표 구분 (BTC,USDJPY 등)
    entity_type = Column(String(30), nullable=True)              # cryptocurrency | currency | equity
    sentiment_score = Column(Numeric(5, 4), nullable=True)       # -1.0 ~ 1.0 (entities 평균)
    category = Column(String(50), nullable=False, default="general")  # crypto | forex | macro
    collector_source = Column(String(20), nullable=False, default="marketaux")
    fetched_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_news_articles_published", "published_at"),
        Index("idx_news_articles_category", "category"),
        Index("idx_news_articles_symbols", "symbols"),
    )

    def __repr__(self):
        return f"<NewsArticle [{self.category}] '{self.title[:40]}' at={self.published_at}>"


class GmocCandle(Base):
    """
    GMO 코인 OHLCV 캔들 (1H / 4H)
    KLine API 폴링으로 수집 (tick 집계 아님)
    Volume: GMO 코인은 실제 거래량 제공 (GMO FX와 달리 0이 아님)
    """
    __tablename__ = "gmoc_candles"

    pair = Column(String(20), nullable=False)             # btc_jpy 등
    timeframe = Column(String(5), nullable=False)         # "1h" | "4h"
    open_time = Column(DateTime(timezone=True), nullable=False)
    close_time = Column(DateTime(timezone=True), nullable=False)
    open = Column(Numeric(18, 8), nullable=False)
    high = Column(Numeric(18, 8), nullable=False)
    low = Column(Numeric(18, 8), nullable=False)
    close = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(24, 8), nullable=False)       # 실제 거래량 (DEFAULT 없음)
    tick_count = Column(Integer, nullable=False, default=0)
    is_complete = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("pair", "timeframe", "open_time", name="gmoc_candles_pkey"),
        Index("idx_gmoc_candles_lookup", "pair", "timeframe", "open_time"),
        Index("idx_gmoc_candles_incomplete", "pair", "timeframe", "is_complete"),
    )

    def __repr__(self):
        return f"<GmocCandle {self.pair} {self.timeframe} {self.open_time} complete={self.is_complete}>"


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
