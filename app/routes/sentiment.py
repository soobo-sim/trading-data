"""
센티먼트 스코어 조회 API (trading-engine DataHub용).

GET /api/sentiment/latest  — 최근 N건 센티먼트 스코어 반환 (source 필터 가능)
GET /api/sentiment/status  — 수집 서비스 상태
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Query
from sqlalchemy import select

from app.core.error_handlers import handle_api_errors
from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sentiment", tags=["Sentiment"])


@router.get("/latest")
@handle_api_errors("센티먼트 최신 스코어 조회")
async def get_latest_sentiment(
    source: Optional[str] = Query(None, description="수집 소스 필터 (예: alternative_me_fng)"),
    limit: int = Query(1, ge=1, le=100, description="최대 반환 건수"),
):
    """
    최근 N건 센티먼트 스코어 반환.

    trading-engine DataHub.get_sentiment()가 이 API를 호출.
    Response:
      - scores: 최신순 스코어 목록
      - count: 반환 건수
    """
    from app.models.database import SentimentScore

    conditions = []
    if source:
        conditions.append(SentimentScore.source == source)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(SentimentScore)
            .where(*conditions)
            .order_by(SentimentScore.fetched_at.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()

    scores = [
        {
            "source": row.source,
            "score": row.score,
            "classification": row.classification,
            "fetched_at": row.fetched_at.isoformat(),
        }
        for row in rows
    ]

    return {
        "scores": scores,
        "count": len(scores),
    }


@router.get("/status")
@handle_api_errors("센티먼트 수집 상태 조회")
async def get_sentiment_status():
    """센티먼트 수집 서비스 상태."""
    from app.services.sentiment_collector import get_sentiment_collector_service
    return get_sentiment_collector_service().get_status()
