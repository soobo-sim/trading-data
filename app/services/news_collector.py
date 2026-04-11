"""
NewsCollectorService — Marketaux API 뉴스 수집 서비스.

수집 대상: 금융 뉴스 (crypto + forex + macro)
API: https://api.marketaux.com/v1/news/all
무료 플랜: 100건/일, 실시간, sentiment 내장
폴링 주기: 15분 (MARKETAUX_POLL_INTERVAL 설정 가능)

쿼리 전략:
  - 라운드 로빈으로 crypto / forex / macro 카테고리를 순환
  - published_after 파라미터로 마지막 수집 이후 기사만 요청 (중복 방지)
  - limit=3 → 15분 × 32회(crypto/forex/macro 각 약 32회) ≈ 96건/일
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.marketaux.com/v1/news/all"

# 카테고리별 검색 쿼리 (라운드 로빈)
_SEARCH_QUERIES: list[dict] = [
    {
        "search": "bitcoin OR btc OR crypto",
        "entity_types": "cryptocurrency",
        "category": "crypto",
    },
    {
        "search": "USD JPY OR forex OR \"central bank\" OR \"interest rate\" OR FOMC",
        "entity_types": "currency",
        "category": "forex",
    },
    {
        "search": "inflation OR GDP OR employment OR CPI OR \"trade war\" OR tariff",
        "category": "macro",
    },
]


class NewsCollectorService:
    """Marketaux 뉴스 수집기 (싱글턴)."""

    def __init__(self) -> None:
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_published_at: Optional[datetime] = None  # 마지막 수집 기사 시각
        self._query_index = 0                                 # 라운드 로빈 인덱스

    # ── Public API ────────────────────────────────────────────

    async def start(self) -> None:
        """백그라운드 수집 태스크 시작."""
        if self._task and not self._task.done():
            logger.warning("[NewsCollector] 이미 실행 중")
            return
        self._running = True
        self._task = asyncio.create_task(
            self._poll_loop(), name="news_collector_poller"
        )
        logger.info("[NewsCollector] 수집 시작 (15분 주기, Marketaux)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[NewsCollector] 수집 종료")

    def get_status(self) -> dict:
        task_alive = self._task is not None and not self._task.done()
        settings = get_settings()
        return {
            "running": self._running and task_alive,
            "poll_interval_sec": settings.MARKETAUX_POLL_INTERVAL,
            "api_configured": bool(settings.MARKETAUX_API_TOKEN),
            "last_published_at": self._last_published_at.isoformat() if self._last_published_at else None,
            "query_index": self._query_index,
        }

    # ── 내부 ──────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """15분 주기 폴링 (기동 직후 즉시 1회 실행)."""
        settings = get_settings()
        while self._running:
            try:
                query = _SEARCH_QUERIES[self._query_index % len(_SEARCH_QUERIES)]
                count = await self._fetch_and_store(query)
                self._query_index += 1
                logger.info(f"[NewsCollector] category={query['category']} {count}건 수집")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[NewsCollector] 수집 오류 (무시): {e}")
            try:
                await asyncio.sleep(settings.MARKETAUX_POLL_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _fetch_and_store(self, query_config: dict) -> int:
        """Marketaux API 호출 → DB UPSERT. 저장 건수 반환."""
        settings = get_settings()
        if not settings.MARKETAUX_API_TOKEN:
            return 0

        params: dict = {
            "api_token": settings.MARKETAUX_API_TOKEN,
            "language": "en",
            "limit": 3,
            "must_have_entities": "true",
        }
        if "search" in query_config:
            params["search"] = query_config["search"]
        if "entity_types" in query_config:
            params["entity_types"] = query_config["entity_types"]
        if self._last_published_at is not None:
            params["published_after"] = self._last_published_at.strftime("%Y-%m-%dT%H:%M")

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(_BASE_URL, params=params)
            resp.raise_for_status()
            data: list[dict] = resp.json().get("data", [])

        if not data:
            return 0

        stored = 0
        category = query_config.get("category", "general")
        for article in data:
            stored += await self._upsert_article(article, category)

        # 마지막 기사 시각 갱신 (중복 방지용 커서)
        latest_str = max(
            (a.get("published_at", "") for a in data),
            default=None,
        )
        if latest_str:
            try:
                self._last_published_at = datetime.fromisoformat(
                    latest_str.replace("Z", "+00:00")
                )
            except ValueError:
                pass

        return stored

    async def _upsert_article(self, article: dict, category: str) -> int:
        """단일 기사 UPSERT. 저장 시 1 반환, 중복/실패 시 0."""
        from app.models.database import NewsArticle

        entities: list[dict] = article.get("entities") or []
        symbols_list = [e.get("symbol", "") for e in entities if e.get("symbol")]
        symbols = ",".join(symbols_list)[:200] if symbols_list else None
        entity_type = (entities[0].get("type", "") or "")[:30] if entities else None
        sentiments = [
            e["sentiment_score"] for e in entities
            if e.get("sentiment_score") is not None
        ]
        avg_sentiment = (
            round(sum(sentiments) / len(sentiments), 4)
            if sentiments else None
        )

        uuid = article.get("uuid", "")
        if not uuid:
            return 0

        published_at_raw = article.get("published_at")
        if isinstance(published_at_raw, str):
            try:
                published_at = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
            except ValueError:
                published_at = datetime.now(timezone.utc)
        elif isinstance(published_at_raw, datetime):
            published_at = published_at_raw
        else:
            published_at = datetime.now(timezone.utc)

        stmt = pg_insert(NewsArticle).values(
            uuid=uuid,
            title=(article.get("title") or "")[:500],
            snippet=(article.get("snippet") or "")[:1000],
            source=(article.get("source") or ""),
            url=(article.get("url") or "")[:1000],
            published_at=published_at,
            language=(article.get("language") or "en"),
            symbols=symbols,
            entity_type=entity_type if entity_type else None,
            sentiment_score=avg_sentiment,
            category=category,
            collector_source="marketaux",
        ).on_conflict_do_nothing(index_elements=["uuid"])

        async with AsyncSessionLocal() as session:
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount


# ── 싱글턴 ────────────────────────────────────────────────────

_service: Optional[NewsCollectorService] = None


def get_news_collector_service() -> NewsCollectorService:
    global _service
    if _service is None:
        _service = NewsCollectorService()
    return _service
