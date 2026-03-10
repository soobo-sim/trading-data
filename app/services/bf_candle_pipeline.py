"""
BfCandlePipeline — BitFlyer 캔들 집계 파이프라인 오케스트레이터

CoincheckCandlePipeline과 동일한 구조.
BF_WS_PRODUCTS에 설정된 각 product마다 3개의 asyncio 태스크를 관리:

  Task 1 — BackfillJob:
    활성화 직후 1회 실행.
    GET /v1/getexecutions 로 과거 틱 소급 → BfCandleService.backfill()

  Task 2 — CandleAggregator:
    BitFlyerWSClient의 {PRODUCT}-executions 채널 Queue 구독
    → BfCandleService.process_tick()
    → 캔들 완성 이벤트 로깅

  Task 3 — FlushWorker:
    30초마다 진행 중인 캔들을 DB에 저장 (재시작 대비).
"""
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, Optional

from app.services.bf_candle_service import get_bf_candle_service
from app.services.bf_ws_client import get_bitflyer_ws_client

logger = logging.getLogger(__name__)

_FLUSH_INTERVAL = 30       # 초
_DEFAULT_BACKFILL_DAYS = 7


class BfCandlePipeline:
    """product별로 캔들 집계 태스크를 관리하는 싱글턴"""

    def __init__(self):
        self._tasks: Dict[str, list[asyncio.Task]] = {}
        self._params: Dict[str, Dict[str, Any]] = {}

    # ── 공개 API ────────────────────────────────────────────────

    async def start(self, product_code: str, params: Optional[Dict[str, Any]] = None) -> None:
        """product에 대한 캔들 파이프라인 태스크 시작. 이미 실행 중이면 재시작."""
        pc = product_code.upper()
        if pc in self._tasks:
            logger.info(f"[BfCandlePipeline] {pc}: 이미 실행 중 → 재시작")
            await self.stop(pc)

        self._params[pc] = params or {}
        tasks = [
            asyncio.create_task(self._backfill_job(pc),      name=f"bf_candle_backfill:{pc}"),
            asyncio.create_task(self._candle_aggregator(pc), name=f"bf_candle_agg:{pc}"),
            asyncio.create_task(self._flush_worker(pc),      name=f"bf_candle_flush:{pc}"),
        ]
        self._tasks[pc] = tasks
        logger.info(f"[BfCandlePipeline] {pc}: 캔들 파이프라인 3개 태스크 시작")

    async def stop(self, product_code: str) -> None:
        pc = product_code.upper()
        tasks = self._tasks.pop(pc, [])
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._params.pop(pc, None)
        logger.info(f"[BfCandlePipeline] {pc}: 파이프라인 종료")

    async def stop_all(self) -> None:
        for pc in list(self._tasks.keys()):
            await self.stop(pc)
        logger.info("[BfCandlePipeline] 전체 파이프라인 종료")

    def is_running(self, product_code: str) -> bool:
        pc = product_code.upper()
        return pc in self._tasks and any(not t.done() for t in self._tasks[pc])

    def running_products(self) -> list[str]:
        return [pc for pc in self._tasks if self.is_running(pc)]

    # ── Task 1: 백필 (1회) ──────────────────────────────────────

    async def _backfill_job(self, product_code: str) -> None:
        try:
            params = self._params.get(product_code, {})
            days = int(params.get("backfill_days", _DEFAULT_BACKFILL_DAYS))
            logger.info(f"[BfCandlePipeline] {product_code}: 백필 시작 (days={days})")
            count = await get_bf_candle_service().backfill(product_code, days=days)
            logger.info(f"[BfCandlePipeline] {product_code}: 백필 완료 ({count}건 upsert)")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[BfCandlePipeline] {product_code}: 백필 오류 — {e}", exc_info=True)

    # ── Task 2: 캔들 집계기 ─────────────────────────────────────

    async def _candle_aggregator(self, product_code: str) -> None:
        """
        BitFlyerWSClient의 {product_code}-executions 채널 Queue에서 틱 수신
        → BfCandleService.process_tick()

        BitFlyer 체결 필드:
          exec_date: ISO8601 문자열
          price: float
          size: float
        """
        ws_client = get_bitflyer_ws_client()
        candle_svc = get_bf_candle_service()

        trade_queue = ws_client.subscribe_executions(product_code)

        try:
            while True:
                try:
                    tick = await asyncio.wait_for(trade_queue.get(), timeout=60.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    exec_date = tick.get("exec_date", "")
                    ts = datetime.fromisoformat(exec_date.replace("Z", "+00:00"))
                    price = Decimal(str(tick["price"]))
                    amount = Decimal(str(tick["size"]))
                except (KeyError, ValueError) as e:
                    logger.debug(f"[BfCandlePipeline] {product_code}: 틱 파싱 오류 {e}")
                    continue

                completed_tfs = await candle_svc.process_tick(product_code, price, amount, ts)
                if completed_tfs:
                    logger.info(
                        f"[BfCandlePipeline] {product_code}: 캔들 완성 {completed_tfs}"
                    )

        except asyncio.CancelledError:
            ws_client.unsubscribe_executions(product_code, trade_queue)
        except Exception as e:
            logger.error(
                f"[BfCandlePipeline] {product_code}: 캔들 집계 오류 — {e}", exc_info=True
            )
            ws_client.unsubscribe_executions(product_code, trade_queue)

    # ── Task 3: 진행 중 캔들 주기 저장 ─────────────────────────

    async def _flush_worker(self, product_code: str) -> None:
        try:
            while True:
                await asyncio.sleep(_FLUSH_INTERVAL)
                try:
                    await get_bf_candle_service().flush_current_candles(product_code)
                except Exception as e:
                    logger.warning(f"[BfCandlePipeline] {product_code}: flush 오류 — {e}")
        except asyncio.CancelledError:
            try:
                await get_bf_candle_service().flush_current_candles(product_code)
                logger.info(f"[BfCandlePipeline] {product_code}: 종료 전 마지막 flush 완료")
            except Exception:
                pass


# ── 싱글턴 ──────────────────────────────────────────────────────

_bf_candle_pipeline: Optional[BfCandlePipeline] = None


def get_bf_candle_pipeline() -> BfCandlePipeline:
    global _bf_candle_pipeline
    if _bf_candle_pipeline is None:
        _bf_candle_pipeline = BfCandlePipeline()
    return _bf_candle_pipeline
