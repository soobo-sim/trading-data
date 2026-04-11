"""
센티먼트 API 라우트 단위 테스트.

GET /api/sentiment/latest — SQLite 인메모리 DB로 실제 쿼리 로직 검증.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.database import SentimentScore, Base


@pytest_asyncio.fixture
async def db_session():
    """SQLite 인메모리 DB + sentiment_scores 테이블 생성."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: SentimentScore.__table__.create(c))
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield factory, engine
    await engine.dispose()


def _make_sentiment_row(score: int, classification: str, source: str = "alternative_me_fng") -> dict:
    return {
        "source": source,
        "score": score,
        "classification": classification,
        "fetched_at": datetime.now(timezone.utc),
    }


@pytest_asyncio.fixture
async def app_with_db(db_session):
    """DB가 연결된 FastAPI 앱 픽스처."""
    from fastapi import FastAPI
    from app.routes.sentiment import router

    factory, engine = db_session
    test_app = FastAPI()
    test_app.include_router(router)
    return test_app, factory, engine


# ── TC4: GET /api/sentiment/latest ───────────────────────────

@pytest.mark.asyncio
async def test_latest_sentiment_empty_db(app_with_db):
    """DB 비어있을 때 → 빈 scores, count=0."""
    test_app, factory, _ = app_with_db

    with patch("app.routes.sentiment.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/sentiment/latest")

    assert resp.status_code == 200
    body = resp.json()
    assert body["scores"] == []
    assert body["count"] == 0


@pytest.mark.asyncio
async def test_latest_sentiment_with_data(app_with_db):
    """DB에 데이터 있을 때 → scores 배열 반환."""
    test_app, factory, engine = app_with_db

    # 데이터 삽입
    async with factory() as session:
        from app.models.database import SentimentScore
        row = _make_sentiment_row(25, "Extreme Fear")
        session.add(SentimentScore(**row))
        await session.commit()

    with patch("app.routes.sentiment.AsyncSessionLocal", factory):
        async with AsyncClient(
            transport=ASGITransport(app=test_app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/sentiment/latest?limit=10")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert len(body["scores"]) == 1
    assert body["scores"][0]["score"] == 25
    assert body["scores"][0]["classification"] == "Extreme Fear"
    assert body["scores"][0]["source"] == "alternative_me_fng"
