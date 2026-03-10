"""
CandlePipeline — Coincheck 캔들 집계 파이프라인 오케스트레이터

coincheck-trader의 BoxMeanReversionManager에서 캔들 관련 태스크(1, 2, 6)를 분리.

3개의 asyncio 태스크를 pair별로 관리:

  Task 1 — CandleAggregator:
    CoincheckWSClient의 [pair]-trades 틱 큐 구독
    → CandleService.process_tick()
    → 캔들 완성 이벤트 로깅

  Task 2 — BackfillJob:
    활성화 직후 1회 실행.
    GET /api/trades로 과거 틱 소급 → CandleService.backfill()

  Task 3 — FlushWorker:
    30초마다 진행 중인 캔들을 DB에 저장 (재시작 대비).

각 pair는 서버 시작 시 자동 구동, 추가/제거 가능.
"""
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, Optional

from app.services.candle_service import get_candle_service
from app.services.ck_ws_client import get_coincheck_ws_client

logger = logging.getLogger(__name__)

_FLUSH_INTERVAL = 30   # 초
_DEFAULT_BACKFILL_DAYS = 7


class CandlePipeline:
    """pair별로 캔들 집계 태스크를 관리하는 싱글턴"""

    def __init__(self):
        self._tasks: Dict[str, list[asyncio.Task]] = {}
        self._params: Dict[str, Dict[str, Any]] = {}

    # ── 공개 API ────────────────────────────────────────────────

    async def start(self, pair: str, params: Optional[Dict[str, Any]] = None) -> None:
        """pair에 대한 캔들 파이프라인 태스크 시작. 이미 실행 중이면 재시작."""
        if pair in self._tasks:
            logger.info(f"[CandlePipeline] {pair}: 이미 실행 중 → 재시작")
            await self.stop(pair)

        self._params[pair] = params or {}
        tasks = [
            asyncio.create_task(self._backfill_job(pair),    name=f"candle_backfill:{pair}"),
            asyncio.create_task(self._candle_aggregator(pair), name=f"candle_agg:{pair}"),
            asyncio.create_task(self._flush_worker(pair),    name=f"candle_flush:{pair}"),
        ]
        self._tasks[pair] = tasks
        logger.info(f"[CandlePipeline] {pair}: 캔들 파이프라인 3개 태스크 시작")

    async def stop(self, pair: str) -> None:
        """pair에 대한 모든 태스크 취소."""
        tasks = self._tasks.pop(pair, [])
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._params.pop(pair, None)
        logger.info(f"[CandlePipeline] {pair}: 파이프라인 종료")

    async def stop_all(self) -> None:
        """모든 pair 태스크 종료 (lifespan shutdown용)."""
        for pair in list(self._tasks.keys()):
            await self.stop(pair)
        logger.info("[CandlePipeline] 전체 파이프라인 종료")

    def is_running(self, pair: str) -> bool:
        return pair in self._tasks and any(not t.done() for t in self._tasks[pair])

    def running_pairs(self) -> list[str]:
        return [p for p in self._tasks if self.is_running(p)]

    # ── Task 1: 백필 (1회) ──────────────────────────────────────

    async def _backfill_job(self, pair: str) -> None:
        """활성화 직후 1회 실행. 과거 틱 소급 → 캔들 재구성."""
        try:
            params = self._params.get(pair, {})
            days = int(params.get("backfill_days", _DEFAULT_BACKFILL_DAYS))
            logger.info(f"[CandlePipeline] {pair}: 백필 시작 (days={days})")
            count = await get_candle_service().backfill(pair, days=days)
            logger.info(f"[CandlePipeline] {pair}: 백필 완료 ({count}건 upsert)")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[CandlePipeline] {pair}: 백필 오류 — {e}", exc_info=True)

    # ── Task 2: 캔들 집계기 ─────────────────────────────────────

    async def _candle_aggregator(self, pair: str) -> None:
        """
        WS 클라이언트 Subscribe Queue에서 실시간 틱 수신 → CandleService.process_tick().
        틱 형식: {"timestamp": float, "pair": str, "rate": str, "amount": str, ...}
        """
        ws_client = get_coincheck_ws_client()
        candle_svc = get_candle_service()

        # WS pair 구독 확인
        await ws_client.add_pair(pair)

        channel = f"{pair}-trades"
        trade_queue = ws_client.subscribe(channel)

        try:
            while True:
                try:
                    tick = await asyncio.wait_for(trade_queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    ts = datetime.fromtimestamp(float(tick["timestamp"]), tz=timezone.utc)
                    price = Decimal(str(tick["rate"]))
                    amount = Decimal(str(tick["amount"]))
                except (KeyError, ValueError) as e:
                    logger.debug(f"[CandlePipeline] {pair}: 틱 파싱 오류 {e}")
                    continue

                completed_tfs = await candle_svc.process_tick(pair, price, amount, ts)
                if completed_tfs:
                    logger.info(f"[CandlePipeline] {pair}: 캔들 완성 {completed_tfs}")

        except asyncio.CancelledError:
            ws_client.unsubscribe(channel, trade_queue)
        except Exception as e:
            logger.error(f"[CandlePipeline] {pair}: 캔들 집계 오류 — {e}", exc_info=True)
            ws_client.unsubscribe(channel, trade_queue)

    # ── Task 3: 진행 중 캔들 주기 저장 ─────────────────────────

    async def _flush_worker(self, pair: str) -> None:
        """30초마다 진행 중인 캔들을 DB에 저장."""
        try:
            while True:
                await asyncio.sleep(_FLUSH_INTERVAL)
                try:
                    await get_candle_service().flush_current_candles(pair)
                except Exception as e:
                    logger.warning(f"[CandlePipeline] {pair}: flush 오류 — {e}")
        except asyncio.CancelledError:
            try:
                await get_candle_service().flush_current_candles(pair)
                logger.info(f"[CandlePipeline] {pair}: 종료 전 마지막 flush 완료")
            except Exception:
                pass


# ── 싱글턴 ──────────────────────────────────────────────────────

_candle_pipeline: Optional[CandlePipeline] = None


def get_candle_pipeline() -> CandlePipeline:
    global _candle_pipeline
    if _candle_pipeline is None:
        _candle_pipeline = CandlePipeline()
    return _candle_pipeline
