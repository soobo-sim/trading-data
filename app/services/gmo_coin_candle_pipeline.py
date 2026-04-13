"""
GmoCoinCandlePipeline — GMO 코인 캔들 수집 파이프라인 오케스트레이터.

GMO FX 캔들 파이프라인과 동일한 구조.
GMO FX 대신 GMO 코인 KLine API 폴링.

태스크 구성 (pair당 2개):
  Task 1 — BackfillJob: 기동 시 1회. 과거 캔들 백필 (4H: 올해+작년, 1H: 최근 180일)
  Task 2 — PollWorker: 5분마다 최신 캔들 폴링 → DB UPSERT
"""
import asyncio
import logging
from typing import Dict, Optional

from app.services.gmo_coin_candle_service import get_gmo_coin_candle_service

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 300  # 5분


class GmoCoinCandlePipeline:
    """pair별로 캔들 수집 태스크를 관리하는 싱글턴."""

    def __init__(self):
        self._tasks: Dict[str, list[asyncio.Task]] = {}

    async def start(self, pair: str) -> None:
        """pair에 대한 캔들 파이프라인 시작."""
        key = pair.lower()
        if key in self._tasks:
            logger.info(f"[GmoCoinCandlePipeline] {key}: 이미 실행 중 → 재시작")
            await self.stop(key)

        tasks = [
            asyncio.create_task(self._backfill_job(key), name=f"gmoc_candle_backfill:{key}"),
            asyncio.create_task(self._poll_worker(key), name=f"gmoc_candle_poll:{key}"),
        ]
        self._tasks[key] = tasks
        logger.info(f"[GmoCoinCandlePipeline] {key}: 캔들 파이프라인 2개 태스크 시작")

    async def stop(self, pair: str) -> None:
        key = pair.lower()
        tasks = self._tasks.pop(key, [])
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"[GmoCoinCandlePipeline] {key}: 파이프라인 종료")

    async def stop_all(self) -> None:
        for key in list(self._tasks.keys()):
            await self.stop(key)
        logger.info("[GmoCoinCandlePipeline] 전체 파이프라인 종료")

    def is_running(self, pair: str) -> bool:
        key = pair.lower()
        return key in self._tasks and any(not t.done() for t in self._tasks[key])

    def running_pairs(self) -> list[str]:
        return [p for p in self._tasks if self.is_running(p)]

    # ── Task 1: 백필 (1회) ──────────────────────────────────────

    _1H_BACKFILL_DAYS = 180  # 타임프레임 최적화 백테스트 요건

    async def _backfill_job(self, pair: str) -> None:
        try:
            svc = get_gmo_coin_candle_service()
            logger.info(f"[GmoCoinCandlePipeline] {pair}: 4H 백필 시작")
            count_4h = await svc.backfill(pair, timeframe="4h")
            logger.info(f"[GmoCoinCandlePipeline] {pair}: 4H 백필 완료 ({count_4h}건)")

            logger.info(f"[GmoCoinCandlePipeline] {pair}: 1H 백필 시작 ({self._1H_BACKFILL_DAYS}일)")
            count_1h = await svc.backfill(pair, timeframe="1h", days=self._1H_BACKFILL_DAYS)
            logger.info(f"[GmoCoinCandlePipeline] {pair}: 1H 백필 완료 ({count_1h}건)")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(
                f"[GmoCoinCandlePipeline] {pair}: 백필 오류 — {e}", exc_info=True
            )

    # ── Task 2: 주기적 폴링 ────────────────────────────────────

    async def _poll_worker(self, pair: str) -> None:
        """5분마다 KLine API 폴링 → 최신 캔들 DB UPSERT."""
        svc = get_gmo_coin_candle_service()
        poll_count = 0
        heartbeat_every = 12  # 12회 × 5분 = 1시간마다 heartbeat
        try:
            # 백필 완료 대기 (10초)
            await asyncio.sleep(10)
            logger.info(f"[GmoCoinCandlePipeline] {pair}: 폴링 워커 시작 (주기 {_POLL_INTERVAL}초)")
            while True:
                try:
                    count_4h = await svc.poll_and_upsert(pair, "4h")
                    count_1h = await svc.poll_and_upsert(pair, "1h")
                    poll_count += 1
                    if count_4h or count_1h:
                        logger.debug(
                            f"[GmoCoinCandlePipeline] {pair}: 폴링 upsert 4H={count_4h} 1H={count_1h}"
                        )
                    if poll_count % heartbeat_every == 0:
                        logger.info(
                            f"[GmoCoinCandlePipeline] {pair}: 폴링 heartbeat — "
                            f"총 {poll_count}회 실행, 최근 4H={count_4h} 1H={count_1h}"
                        )
                except Exception as e:
                    logger.error(
                        f"[GmoCoinCandlePipeline] {pair}: 폴링 오류 — {e}", exc_info=True
                    )
                await asyncio.sleep(_POLL_INTERVAL)
        except asyncio.CancelledError:
            pass


_instance: Optional[GmoCoinCandlePipeline] = None


def get_gmo_coin_candle_pipeline() -> GmoCoinCandlePipeline:
    global _instance
    if _instance is None:
        _instance = GmoCoinCandlePipeline()
    return _instance
