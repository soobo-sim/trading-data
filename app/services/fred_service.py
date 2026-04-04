"""
FredService — FRED(Federal Reserve Economic Data) 매크로 지표 수집 서비스 (F-04 알파 팩터).

수집 지표:
  DGS10    — US 10년물 국채 금리 (일간)
  DGS2     — US 2년물 국채 금리 (일간)
  T10Y2Y   — 10Y-2Y 수익률 스프레드 (일간)
  DEXJPUS  — USD/JPY 공식 환율 (일간)
  DTWEXBGS — 달러 인덱스 DXY 대안 (주간)
  VIXCLS   — VIX 변동성 지수 (일간)

수집 주기: 매일 08:00 JST (FRED T+1 영업일 지연 → 전날 데이터 확보 가능)
히스토리: 초기 기동 시 최근 2년 일괄 백필 (UNIQUE 충돌 무시)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.database import AsyncSessionLocal
from app.models.database import IntermarketData

logger = logging.getLogger(__name__)

_JST = ZoneInfo("Asia/Tokyo")

# 수집할 FRED series 목록
FRED_SERIES = [
    "DGS10",    # US 10Y 국채 금리
    "DGS2",     # US 2Y 국채 금리
    "T10Y2Y",   # 10Y-2Y 스프레드
    "DEXJPUS",  # USD/JPY 공식 환율
    "DTWEXBGS", # 달러 인덱스
    "VIXCLS",   # VIX
]

# 최신값 조회 캐시 (TTL 1시간)
_cache: dict[str, tuple[float, datetime]] = {}  # series_id → (value, cached_at)
_CACHE_TTL = 3600  # 초


class FredService:
    """FRED 지표 수집 + 캐시 서비스 (싱글턴)."""

    def __init__(self) -> None:
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Public API ────────────────────────────────────────────

    async def start(self) -> None:
        """백그라운드 수집 태스크 시작. 최초 기동 시 히스토리 백필 수행."""
        if self._task and not self._task.done():
            logger.warning("[FRED] 이미 실행 중")
            return
        settings = get_settings()
        if not settings.FRED_API_KEY:
            logger.warning("[FRED] FRED_API_KEY 미설정 — 서비스 비활성")
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="fred_poller")
        logger.info("[FRED] 수집 시작 (매일 08:00 JST)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[FRED] 수집 종료")

    def get_status(self) -> dict:
        task_alive = self._task is not None and not self._task.done()
        return {
            "running": self._running and task_alive,
            "series": FRED_SERIES,
        }

    async def get_latest(self, series_id: str) -> Optional[float]:
        """
        최신 관측값 반환 (캐시 TTL=1시간).
        DB에서 가장 최근 non-null 값 조회.
        """
        now = datetime.now(tz=timezone.utc)
        if series_id in _cache:
            val, cached_at = _cache[series_id]
            if (now - cached_at).total_seconds() < _CACHE_TTL:
                return val

        async with AsyncSessionLocal() as session:
            row = await session.execute(
                select(IntermarketData.value)
                .where(
                    IntermarketData.series_id == series_id,
                    IntermarketData.value.is_not(None),
                )
                .order_by(IntermarketData.obs_date.desc())
                .limit(1)
            )
            result = row.scalar_one_or_none()
        if result is None:
            return None
        val = float(result)
        _cache[series_id] = (val, now)
        return val

    async def get_recent(self, series_id: str, days: int = 10) -> list[tuple[date, float]]:
        """
        최근 N일 관측값 반환 (시계열 분석용).
        Returns: [(obs_date, value), ...] 오래된 것 → 최신 순서.
        """
        async with AsyncSessionLocal() as session:
            rows = await session.execute(
                select(IntermarketData.obs_date, IntermarketData.value)
                .where(
                    IntermarketData.series_id == series_id,
                    IntermarketData.value.is_not(None),
                )
                .order_by(IntermarketData.obs_date.desc())
                .limit(days)
            )
            results = [(r.obs_date, float(r.value)) for r in rows]
        return list(reversed(results))

    # ── Internal ─────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """매일 JST 08:00에 전일자 데이터 수집. 초기 기동 시 2년 백필."""
        # 초기 히스토리 백필
        try:
            await self._backfill_history(days=730)
        except Exception as e:
            logger.warning(f"[FRED] 히스토리 백필 실패: {e}")

        while self._running:
            try:
                await self._wait_until_next_fetch()
                await self._fetch_and_store_recent(days=7)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[FRED] 수집 오류: {e}")
                await asyncio.sleep(300)  # 5분 대기 후 재시도

    async def _wait_until_next_fetch(self) -> None:
        """다음 JST 08:00까지 대기."""
        settings = get_settings()
        now_jst = datetime.now(tz=_JST)
        target_hour = settings.FRED_FETCH_HOUR_JST
        next_run = now_jst.replace(hour=target_hour, minute=0, second=0, microsecond=0)
        if now_jst >= next_run:
            next_run += timedelta(days=1)
        wait_sec = (next_run - now_jst).total_seconds()
        logger.info(f"[FRED] 다음 수집: {next_run.strftime('%Y-%m-%d %H:%M JST')} ({wait_sec/3600:.1f}시간 후)")
        await asyncio.sleep(wait_sec)

    async def _backfill_history(self, days: int = 730) -> None:
        """초기 기동 시 히스토리 데이터 일괄 수집."""
        since = date.today() - timedelta(days=days)
        logger.info(f"[FRED] 히스토리 백필 시작: {since} ~ today ({days}일)")
        total = 0
        for series_id in FRED_SERIES:
            count = await self._fetch_series(series_id, observation_start=since.isoformat())
            total += count
            logger.info(f"[FRED] {series_id}: {count}건 저장")
            await asyncio.sleep(0.5)  # API 레이트 리밋 배려
        logger.info(f"[FRED] 히스토리 백필 완료: 총 {total}건")

    async def _fetch_and_store_recent(self, days: int = 7) -> None:
        """최근 N일 데이터 수집 (일상 폴링용)."""
        since = date.today() - timedelta(days=days)
        total = 0
        for series_id in FRED_SERIES:
            count = await self._fetch_series(series_id, observation_start=since.isoformat())
            total += count
            await asyncio.sleep(0.3)
        logger.info(f"[FRED] 일상 수집 완료: 총 {total}건 upsert")
        # 캐시 무효화
        _cache.clear()

    async def _fetch_series(self, series_id: str, observation_start: str) -> int:
        """FRED API에서 series 데이터 조회 후 DB upsert. 저장 건수 반환."""
        settings = get_settings()
        url = f"{settings.FRED_BASE_URL}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": settings.FRED_API_KEY,
            "file_type": "json",
            "observation_start": observation_start,
            "sort_order": "asc",
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        observations = data.get("observations", [])
        if not observations:
            return 0

        rows = []
        for obs in observations:
            raw_val = obs.get("value", ".")
            value = None if raw_val == "." else float(raw_val)
            obs_date = datetime.strptime(obs["date"], "%Y-%m-%d")
            rows.append(
                {
                    "series_id": series_id,
                    "obs_date": obs_date,
                    "value": value,
                    "source": "fred",
                }
            )

        if not rows:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = pg_insert(IntermarketData).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["series_id", "obs_date"],
                set_={"value": stmt.excluded.value, "fetched_at": text("NOW()")},
            )
            await session.execute(stmt)
            await session.commit()

        return len(rows)


# ── Singleton ─────────────────────────────────────────────────

_instance: Optional[FredService] = None


def get_fred_service() -> FredService:
    global _instance
    if _instance is None:
        _instance = FredService()
    return _instance
