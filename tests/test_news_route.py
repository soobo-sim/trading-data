"""
뉴스 API 라우트 단위 테스트.

GET /api/news/latest — SQLite 인메모리 DB로 실제 쿼리 로직 검증.
GET /api/news/status — 수집기 상태 응답 형식 검증.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.database import NewsArticle, Base

# SQLite는 on_conflict_do_nothing을 지원하지 않으므로 라우트 조회 테스트만 진행.
# UPSERT 로직은 test_news_collector.py에서 PostgreSQL dialect mock으로 검증.


@pytest_asyncio.fixture
async def db_session():
    """SQLite 인메모리 DB + news_articles 테이블 생성."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        # NewsArticle 테이블만 생성
        await conn.run_sync(lambda c: NewsArticle.__table__.create(c))
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield factory, engine
    await engine.dispose()


def _make_news_row(
    uuid: str,
    title: str,
    category: str = "crypto",
    sentiment: float | None = 0.5,
    hours_ago: int = 1,
) -> dict:
    """DB 삽입용 기사 데이터."""
    published_at = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return {
        "uuid": uuid,
        "title": title,
        "snippet": "snippet",
        "source": "reuters.com",
        "url": "https://reuters.com/article",
        "published_at": published_at,
        "language": "en",
        "symbols": "BTC",
        "entity_type": "cryptocurrency",
        "sentiment_score": Decimal(str(sentiment)) if sentiment is not None else None,
        "category": category,
        "collector_source": "marketaux",
    }


@pytest_asyncio.fixture
async def app_with_db(db_session):
    """DB가 연결된 FastAPI 앱 픽스처."""
    from fastapi import FastAPI
    from app.routes.news import router
    from app.database import AsyncSessionLocal as _orig_session

    factory, engine = db_session

    # AsyncSessionLocal을 SQLite factory로 교체
    mock_session_local = factory

    test_app = FastAPI()
    test_app.include_router(router)

    return test_app, factory, engine


# ── GET /api/news/latest ─────────────────────────────────────

@pytest.mark.asyncio
async def test_latest_news_empty_db(app_with_db):
    """DB 비어있을 때 → 빈 articles, count=0, avg_sentiment=null."""
    test_app, factory, _ = app_with_db

    async with factory() as session:
        pass  # DB 비어있음

    with patch("app.routes.news.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/news/latest")

    assert resp.status_code == 200
    body = resp.json()
    assert body["articles"] == []
    assert body["count"] == 0
    assert body["avg_sentiment"] is None


@pytest.mark.asyncio
async def test_latest_news_returns_articles(app_with_db):
    """DB에 2건 → 2건 반환, avg_sentiment 계산."""
    test_app, factory, _ = app_with_db

    # 2건 삽입
    async with factory() as session:
        for row_data in [
            _make_news_row("n1", "BTC surges", sentiment=0.4),
            _make_news_row("n2", "ETH stable", sentiment=0.6),
        ]:
            session.add(NewsArticle(**row_data))
        await session.commit()

    with patch("app.routes.news.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/news/latest")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    # avg_sentiment = (0.4 + 0.6) / 2 = 0.5
    assert abs(body["avg_sentiment"] - 0.5) < 0.001


@pytest.mark.asyncio
async def test_latest_news_category_filter(app_with_db):
    """category=crypto 필터 → crypto 기사만 반환."""
    test_app, factory, _ = app_with_db

    async with factory() as session:
        session.add(NewsArticle(**_make_news_row("c1", "BTC breakdown", category="crypto")))
        session.add(NewsArticle(**_make_news_row("f1", "FOMC holds", category="forex")))
        await session.commit()

    with patch("app.routes.news.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/news/latest", params={"category": "crypto"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["articles"][0]["category"] == "crypto"


@pytest.mark.asyncio
async def test_latest_news_hours_filter(app_with_db):
    """hours=1 → 1시간 이내 기사만. 12시간 전 기사는 제외."""
    test_app, factory, _ = app_with_db

    async with factory() as session:
        session.add(NewsArticle(**_make_news_row("new1", "Recent news", hours_ago=0)))  # 방금
        session.add(NewsArticle(**_make_news_row("old1", "Old news", hours_ago=12)))   # 12시간 전
        await session.commit()

    with patch("app.routes.news.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/news/latest", params={"hours": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["articles"][0]["uuid"] == "new1"


@pytest.mark.asyncio
async def test_latest_news_avg_sentiment_null_excluded(app_with_db):
    """sentiment=None 기사 제외한 평균 계산."""
    test_app, factory, _ = app_with_db

    async with factory() as session:
        session.add(NewsArticle(**_make_news_row("s1", "Positive", sentiment=0.8)))
        session.add(NewsArticle(**_make_news_row("s2", "No sentiment", sentiment=None)))
        await session.commit()

    with patch("app.routes.news.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/news/latest")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    # avg_sentiment = 0.8 (null 제외)
    assert abs(body["avg_sentiment"] - 0.8) < 0.001


@pytest.mark.asyncio
async def test_latest_news_limit_param(app_with_db):
    """limit=1 → 최대 1건 반환."""
    test_app, factory, _ = app_with_db

    async with factory() as session:
        for i in range(3):
            session.add(NewsArticle(**_make_news_row(f"lim{i}", f"Article {i}")))
        await session.commit()

    with patch("app.routes.news.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/news/latest", params={"limit": 1})

    assert resp.status_code == 200
    assert resp.json()["count"] == 1


# ── GET /api/news/status ─────────────────────────────────────

@pytest.mark.asyncio
async def test_news_status_response_shape(app_with_db):
    """status 응답에 필수 필드 포함 확인."""
    test_app, _, _ = app_with_db

    mock_status = {
        "running": False,
        "poll_interval_sec": 900,
        "api_configured": True,
        "last_published_at": None,
        "query_index": 0,
    }

    mock_svc_instance = MagicMock()
    mock_svc_instance.get_status.return_value = mock_status

    with patch("app.services.news_collector.get_news_collector_service", return_value=mock_svc_instance):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/news/status")

    assert resp.status_code == 200
    body = resp.json()
    assert "running" in body
    assert "api_configured" in body
    assert body["poll_interval_sec"] == 900


# ── ORM 모델 임포트 검증 ──────────────────────────────────────

def test_news_article_model_importable():
    """NewsArticle ORM 모델 임포트 성공."""
    from app.models.database import NewsArticle as NA
    assert NA.__tablename__ == "news_articles"


def test_news_article_model_columns():
    """NewsArticle 필수 컬럼 존재 확인."""
    from app.models.database import NewsArticle as NA
    col_names = {c.name for c in NA.__table__.columns}
    required = {"id", "uuid", "title", "snippet", "source", "url",
                "published_at", "language", "symbols", "entity_type",
                "sentiment_score", "category", "collector_source", "fetched_at"}
    assert required.issubset(col_names)


def test_news_article_uuid_unique():
    """uuid 컬럼에 unique 제약 존재."""
    from app.models.database import NewsArticle as NA
    uuid_col = next(c for c in NA.__table__.columns if c.name == "uuid")
    assert uuid_col.unique
