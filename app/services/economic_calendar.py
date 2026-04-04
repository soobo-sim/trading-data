"""
EconomicCalendarService — ForexFactory 경제 캘린더 수집 서비스 (F-01 알파 팩터).

ForexFactory 무료 JSON API를 polling하여 economic_events 테이블에 저장.
trading-engine이 REST API로 조회하여 이벤트 전 진입 차단에 활용.

수집 대상:
  - 통화: USD, JPY, GBP, EUR (FX 전략 연관 통화)
  - impact: High, Medium (Low/Holiday 제외)
  - 기간: thisweek + nextweek (최대 2주 선행)

폴링 주기: 6시간 (매일 4회)
FF API:
  https://nfs.faireconomy.media/ff_calendar_thisweek.json
  https://nfs.faireconomy.media/ff_calendar_nextweek.json
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 6 * 3600  # 6시간 (초)

_FF_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
]

# FX 전략과 관련된 통화만 수집
_RELEVANT_CURRENCIES = {"USD", "JPY", "GBP", "EUR"}

# High, Medium만 수집 (Low/Holiday 제외)
_RELEVANT_IMPACTS = {"High", "Medium"}


class EconomicCalendarService:
    """ForexFactory 경제 캘린더 수집 서비스 (싱글턴)."""

    def __init__(self) -> None:
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Public API ────────────────────────────────────────────

    async def start(self) -> None:
        """백그라운드 수집 태스크 시작."""
        if self._task and not self._task.done():
            logger.warning("[EconomicCalendar] 이미 실행 중")
            return
        self._running = True
        self._task = asyncio.create_task(
            self._poll_loop(), name="economic_calendar_poller"
        )
        logger.info("[EconomicCalendar] 수집 시작 (6시간 주기)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[EconomicCalendar] 수집 종료")

    def get_status(self) -> dict:
        task_alive = self._task is not None and not self._task.done()
        return {
            "running": self._running and task_alive,
            "poll_interval_sec": _POLL_INTERVAL,
            "sources": _FF_URLS,
        }

    async def get_upcoming_events(
        self,
        countries: list[str],
        hours_ahead: int = 24,
    ) -> list[dict]:
        """
        향후 N시간 이내 이벤트 조회 (trading-engine API용).

        Args:
            countries: 필터할 통화 목록 (예: ["USD", "JPY"])
            hours_ahead: 조회 기간 (현재 시각 기준)

        Returns:
            이벤트 딕셔너리 목록 (title, country, event_time, impact)
        """
        from app.models.database import EconomicEvent
        from sqlalchemy import select, and_
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=hours_ahead)

        async with AsyncSessionLocal() as session:
            stmt = (
                select(EconomicEvent)
                .where(
                    and_(
                        EconomicEvent.event_time >= now,
                        EconomicEvent.event_time <= cutoff,
                        EconomicEvent.country.in_(countries),
                    )
                )
                .order_by(EconomicEvent.event_time)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

        return [
            {
                "id": row.id,
                "title": row.title,
                "country": row.country,
                "event_time": row.event_time.isoformat(),
                "impact": row.impact,
                "forecast": row.forecast,
                "previous": row.previous,
                "actual": row.actual,
            }
            for row in rows
        ]

    # ── 내부 ──────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """6시간 주기 폴링 (기동 직후 즉시 1회 실행)."""
        while self._running:
            try:
                count = await self._fetch_and_store_all()
                logger.info(f"[EconomicCalendar] {count}건 저장/갱신")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[EconomicCalendar] 수집 오류 (무시): {e}")
            try:
                await asyncio.sleep(_POLL_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _fetch_and_store_all(self) -> int:
        """thisweek + nextweek JSON 수집 → DB UPSERT. 저장 건수 반환."""
        total = 0
        for url in _FF_URLS:
            try:
                events = await self._fetch_calendar(url)
                filtered = self._filter_relevant_events(events)
                stored = await self._store_events(filtered)
                total += stored
            except Exception as e:
                logger.warning(f"[EconomicCalendar] URL {url} 오류: {e}")
        return total

    async def _fetch_calendar(self, url: str) -> list[dict]:
        """ForexFactory JSON 조회. 성공 시 이벤트 리스트 반환."""
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()

    def _filter_relevant_events(self, events: list[dict]) -> list[dict]:
        """관련 통화 + High/Medium impact 이벤트만 필터링."""
        filtered = []
        for ev in events:
            country = ev.get("country", "").upper()
            impact = ev.get("impact", "")
            if country in _RELEVANT_CURRENCIES and impact in _RELEVANT_IMPACTS:
                filtered.append(ev)
        return filtered

    async def _store_events(self, events: list[dict]) -> int:
        """경제 이벤트 DB UPSERT. 저장 건수 반환."""
        if not events:
            return 0

        from app.models.database import EconomicEvent

        stored = 0
        fetched_at = datetime.now(timezone.utc)

        async with AsyncSessionLocal() as session:
            for ev in events:
                try:
                    event_time = self._parse_event_time(ev.get("date", ""))
                    if event_time is None:
                        continue

                    stmt = pg_insert(EconomicEvent).values(
                        title=ev.get("title", ""),
                        country=ev.get("country", "").upper(),
                        event_time=event_time,
                        impact=ev.get("impact", ""),
                        forecast=ev.get("forecast") or None,
                        previous=ev.get("previous") or None,
                        actual=ev.get("actual") or None,
                        source="forexfactory",
                        fetched_at=fetched_at,
                    ).on_conflict_do_update(
                        constraint="uq_economic_events_title_time",
                        set_={
                            "actual": ev.get("actual") or None,
                            "fetched_at": fetched_at,
                        },
                    )
                    await session.execute(stmt)
                    stored += 1
                except Exception as e:
                    logger.debug(f"[EconomicCalendar] 이벤트 저장 오류: {e}")

            await session.commit()

        return stored

    @staticmethod
    def _parse_event_time(date_str: str) -> Optional[datetime]:
        """
        ISO8601 문자열 파싱 → UTC aware datetime.
        ForexFactory: "2026-03-30T10:30:00-04:00" 형식.
        """
        if not date_str:
            return None
        try:
            # Python 3.7+: fromisoformat은 오프셋 있는 형식 지원
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        except ValueError:
            logger.debug(f"[EconomicCalendar] 날짜 파싱 실패: {date_str!r}")
            return None


# 싱글턴
_service: Optional[EconomicCalendarService] = None


def get_economic_calendar_service() -> EconomicCalendarService:
    global _service
    if _service is None:
        _service = EconomicCalendarService()
    return _service
