"""
BfFundingRateService — BitFlyer 펀딩레이트 수집 및 조회

GET /v1/getfundingrate (Public API, 인증 불필요)
15분 주기로 폴링하여 bf_funding_rates 테이블에 저장.

사용:
    from app.services.bf_funding_rate_service import get_bf_funding_rate_service
    svc = get_bf_funding_rate_service()
    latest = await svc.get_latest()
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy import select, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 900   # 15분 (초)
_PRODUCT_CODE = "FX_BTC_JPY"


class BfFundingRateService:
    """BitFlyer 펀딩레이트 수집 서비스"""

    def __init__(self):
        settings = get_settings()
        self.base_url = settings.BITFLYER_BASE_URL.rstrip("/")
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Public API ────────────────────────────────────────────

    async def start(self) -> None:
        """백그라운드 폴링 태스크 시작"""
        if self._task and not self._task.done():
            logger.warning("[BfFundingRateService] 이미 실행 중")
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="bf_funding_rate_poller")
        logger.info("[BfFundingRateService] 폴링 시작 (15분 주기)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[BfFundingRateService] 폴링 종료")

    async def get_latest(self, product_code: str = _PRODUCT_CODE) -> Optional[dict]:
        """최신 펀딩레이트 1건 반환"""
        from app.models.database import BfFundingRate
        async with AsyncSessionLocal() as session:
            stmt = (
                select(BfFundingRate)
                .where(BfFundingRate.product_code == product_code)
                .order_by(desc(BfFundingRate.collected_at))
                .limit(1)
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return self._to_dict(row)

    async def get_history(self, product_code: str = _PRODUCT_CODE, limit: int = 96) -> list[dict]:
        """펀딩레이트 이력 반환 (최신순, 기본 96건 = 24시간)"""
        from app.models.database import BfFundingRate
        async with AsyncSessionLocal() as session:
            stmt = (
                select(BfFundingRate)
                .where(BfFundingRate.product_code == product_code)
                .order_by(desc(BfFundingRate.collected_at))
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            return [self._to_dict(r) for r in rows]

    def get_status(self) -> dict:
        """폴러 실행 상태 반환 (헬스체크용)"""
        task_alive = (
            self._task is not None
            and not self._task.done()
        )
        return {
            "running": self._running and task_alive,
            "poll_interval_sec": _POLL_INTERVAL,
            "product_code": _PRODUCT_CODE,
        }

    # ── 내부 ──────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """15분 주기 폴링 루프 (기동 직후 즉시 1회 실행)"""
        while self._running:
            try:
                await self._fetch_and_store()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[BfFundingRateService] 폴링 오류: {e}", exc_info=True)
            try:
                await asyncio.sleep(_POLL_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _fetch_and_store(self) -> None:
        url = f"{self.base_url}/v1/getfundingrate"
        params = {"product_code": _PRODUCT_CODE}
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        current_rate = data.get("current_funding_rate")
        next_date_str = data.get("next_funding_rate_settledate")

        if current_rate is None:
            logger.warning(f"[BfFundingRateService] 응답에 current_funding_rate 없음: {data}")
            return

        next_settlement = None
        if next_date_str:
            try:
                next_settlement = datetime.fromisoformat(next_date_str.replace("Z", "+00:00"))
            except ValueError:
                pass

        collected_at = datetime.now(timezone.utc)

        from app.models.database import BfFundingRate
        async with AsyncSessionLocal() as session:
            stmt = pg_insert(BfFundingRate).values(
                product_code=_PRODUCT_CODE,
                current_funding_rate=current_rate,
                next_settlement_date=next_settlement,
                collected_at=collected_at,
            ).on_conflict_do_nothing(
                index_elements=["product_code", "collected_at"]
            )
            await session.execute(stmt)
            await session.commit()

        logger.debug(f"[BfFundingRateService] 저장 완료: rate={current_rate}, collected_at={collected_at}")

    @staticmethod
    def _to_dict(row) -> dict:
        return {
            "product_code": row.product_code,
            "current_funding_rate": float(row.current_funding_rate),
            "next_settlement_date": row.next_settlement_date.isoformat() if row.next_settlement_date else None,
            "collected_at": row.collected_at.isoformat(),
        }


# ── 싱글턴 ────────────────────────────────────────────────────────────────
_service: Optional[BfFundingRateService] = None


def get_bf_funding_rate_service() -> BfFundingRateService:
    global _service
    if _service is None:
        _service = BfFundingRateService()
    return _service
