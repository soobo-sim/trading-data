"""
SentimentCollectorService — Alternative.me Fear & Greed Index 수집 서비스.

수집 대상: Crypto Fear & Greed Index (0~100)
API: https://api.alternative.me/fng/?limit=1
무료, 레이트 리밋 없음, 갱신 주기 1시간
폴링 주기: 1시간 (SENTIMENT_POLL_INTERVAL 설정 가능, 기본 3600초)

중복 방지: 직전 score와 동일한 경우 INSERT 스킵 (_last_score 캐시)
에러 처리: API 실패 시 WARNING 로그 + 다음 폴링 대기 (크래시 없음)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx
from sqlalchemy import insert

from app.database import AsyncSessionLocal
from app.models.database import SentimentScore

logger = logging.getLogger(__name__)

_FNG_URL = "https://api.alternative.me/fng/?limit=1"
_DEFAULT_POLL_INTERVAL = 3600  # 1시간 (FNG 갱신 주기)


class SentimentCollectorService:
    """Alternative.me Fear & Greed Index 수집기 (싱글턴)."""

    def __init__(self, poll_interval: int = _DEFAULT_POLL_INTERVAL) -> None:
        self._poll_interval = poll_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_score: Optional[int] = None  # 직전 수집 score (중복 INSERT 방지)

    # ── Public API ────────────────────────────────────────────

    async def start(self) -> None:
        """백그라운드 수집 태스크 시작."""
        if self._task and not self._task.done():
            logger.warning("[SentimentCollector] 이미 실행 중")
            return
        self._running = True
        self._task = asyncio.create_task(
            self._poll_loop(), name="sentiment_collector_poller"
        )
        logger.info(f"[SentimentCollector] 수집 시작 ({self._poll_interval}초 주기, Alternative.me FNG)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[SentimentCollector] 수집 종료")

    def get_status(self) -> dict:
        return {
            "running": self._running,
            "last_score": self._last_score,
            "poll_interval": self._poll_interval,
        }

    # ── 내부 루프 ─────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while self._running:
            await self._poll_once()
            await asyncio.sleep(self._poll_interval)

    async def _poll_once(self) -> None:
        """FNG API 1회 폴링 → score 변경 시만 INSERT."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(_FNG_URL)
                resp.raise_for_status()
                body = resp.json()

            entry = body["data"][0]
            score = int(entry["value"])
            classification = entry["value_classification"]
            raw_timestamp = int(entry.get("timestamp", 0)) or None

        except Exception as e:
            logger.warning(f"[SentimentCollector] FNG API 실패 (다음 폴링에서 재시도): {e}")
            return

        # score 변화 없으면 INSERT 스킵
        if self._last_score is not None and score == self._last_score:
            logger.debug(f"[SentimentCollector] score 변화 없음 ({score}), INSERT 스킵")
            return

        async with AsyncSessionLocal() as session:
            await session.execute(
                insert(SentimentScore).values(
                    source="alternative_me_fng",
                    score=score,
                    classification=classification,
                    raw_timestamp=raw_timestamp,
                )
            )
            await session.commit()

        logger.info(
            f"[SentimentCollector] score 수집: {score} ({classification}), "
            f"이전={self._last_score}"
        )
        self._last_score = score


# ── 싱글턴 ───────────────────────────────────────────────────

_service: Optional[SentimentCollectorService] = None


def get_sentiment_collector_service() -> SentimentCollectorService:
    global _service
    if _service is None:
        _service = SentimentCollectorService()
    return _service
