"""
뉴스 기사 조회 API (trading-engine DataHub용).

GET /api/news/latest  — 최근 N시간 뉴스 기사 반환 (category 필터 가능)
GET /api/news/status  — 수집 서비스 상태
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Query
from sqlalchemy import select, and_

from app.core.error_handlers import handle_api_errors
from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/news", tags=["News"])


@router.get("/latest")
@handle_api_errors("뉴스 최신 기사 조회")
async def get_latest_news(
    category: Optional[str] = Query(None, description="crypto | forex | macro"),
    hours: int = Query(24, ge=1, le=168, description="최근 N시간"),
    limit: int = Query(20, ge=1, le=50, description="최대 반환 건수"),
):
    """
    최근 N시간 뉴스 기사 반환.

    trading-engine DataHub.get_news_summary() / get_sentiment()가 이 API를 호출.
    Response:
      - articles: 최신순 기사 목록
      - count: 반환 건수
      - avg_sentiment: 기사 sentiment_score 평균 (-1.0~1.0), 데이터 없으면 null
    """
    from app.models.database import NewsArticle

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)

    conditions = [
        NewsArticle.published_at >= cutoff,
    ]
    if category:
        conditions.append(NewsArticle.category == category)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(NewsArticle)
            .where(and_(*conditions))
            .order_by(NewsArticle.published_at.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()

    articles = [
        {
            "uuid": row.uuid,
            "title": row.title,
            "snippet": row.snippet,
            "source": row.source,
            "url": row.url,
            "published_at": row.published_at.isoformat(),
            "category": row.category,
            "symbols": row.symbols,
            "sentiment_score": float(row.sentiment_score) if row.sentiment_score is not None else None,
        }
        for row in rows
    ]

    # avg_sentiment 계산 (null 제외)
    scores = [a["sentiment_score"] for a in articles if a["sentiment_score"] is not None]
    avg_sentiment = round(sum(scores) / len(scores), 4) if scores else None

    return {
        "articles": articles,
        "count": len(articles),
        "avg_sentiment": avg_sentiment,
    }


@router.get("/status")
@handle_api_errors("뉴스 수집 상태 조회")
async def get_news_status():
    """뉴스 수집 서비스 상태."""
    from app.services.news_collector import get_news_collector_service
    return get_news_collector_service().get_status()
