"""
BfCandleService — BitFlyer OHLCV 캔들 집계 및 관리

두 가지 소스로 캔들 구성:
1. REST 백필: GET /v1/getexecutions 로 과거 체결 소급
2. WS 틱 스트림: BitFlyerWSClient의 {PRODUCT}-executions 채널 실시간 집계

캔들 시간 경계 (UTC 기준):
  1H: 매 정각
  4H: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Dict, Tuple

import httpx
from sqlalchemy import select, and_, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.database import AsyncSessionLocal
from app.models.database import BfCandle

logger = logging.getLogger(__name__)

TIMEFRAME_SECONDS: Dict[str, int] = {"1h": 3600, "4h": 14400}


def _get_candle_open_time(dt: datetime, timeframe: str) -> datetime:
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    if timeframe == "1h":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif timeframe == "4h":
        hour = (dt.hour // 4) * 4
        return dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _get_candle_close_time(open_time: datetime, timeframe: str) -> datetime:
    return open_time + timedelta(seconds=TIMEFRAME_SECONDS[timeframe]) - timedelta(microseconds=1)


class BfCandleService:
    """
    BitFlyer 캔들 집계 서비스
    - 인메모리 버퍼로 진행 중인 캔들 관리
    - 캔들 완성 시 DB에 is_complete=True로 UPSERT
    - 진행 중 캔들도 주기적으로 UPSERT (재시작 시 복구용)
    """

    def __init__(self):
        # {product_code: {timeframe: BfCandle}}
        self._current: Dict[str, Dict[str, BfCandle]] = {}
        self._lock = asyncio.Lock()

    # ── 틱 처리 ────────────────────────────────────────────────

    async def process_tick(
        self, product_code: str, price: Decimal, amount: Decimal, ts: datetime
    ) -> list[str]:
        """실시간 틱 1건 처리. 캔들이 완성되면 완성된 timeframe 목록 반환."""
        completed = []
        async with self._lock:
            for tf in ("1h", "4h"):
                open_time = _get_candle_open_time(ts, tf)
                curr = self._current.get(product_code, {}).get(tf)

                if curr is None:
                    curr = await self._load_or_create_candle(
                        product_code, tf, open_time, price, amount, ts
                    )
                elif curr.open_time != open_time:
                    curr.is_complete = True
                    await self._upsert_candle(curr)
                    logger.info(f"[BfCandleService] 캔들 완성: {product_code} {tf} {curr.open_time}")
                    completed.append(tf)
                    curr = self._make_candle(product_code, tf, open_time, price, amount, ts)

                p = float(price)
                a = float(amount)
                if p > float(curr.high):
                    curr.high = Decimal(str(p))
                if p < float(curr.low):
                    curr.low = Decimal(str(p))
                curr.close = Decimal(str(p))
                curr.volume = Decimal(str(float(curr.volume) + a))
                curr.tick_count += 1
                curr.updated_at = ts

                if product_code not in self._current:
                    self._current[product_code] = {}
                self._current[product_code][tf] = curr
        return completed

    async def flush_current_candles(self, product_code: str) -> None:
        """진행 중인 캔들을 DB에 저장 (재시작 대비 주기 저장용)."""
        async with self._lock:
            for tf, candle in (self._current.get(product_code) or {}).items():
                candle.updated_at = datetime.now(timezone.utc)
                await self._upsert_candle(candle)

    # ── 조회 ────────────────────────────────────────────────────

    async def get_completed_candles(
        self, product_code: str, timeframe: str, limit: int = 60
    ) -> list[BfCandle]:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(BfCandle)
                .where(and_(
                    BfCandle.product_code == product_code,
                    BfCandle.timeframe == timeframe,
                    BfCandle.is_complete == True,
                ))
                .order_by(desc(BfCandle.open_time))
                .limit(limit)
            )
            candles = list(result.scalars().all())
        return list(reversed(candles))

    async def get_current_candle(
        self, product_code: str, timeframe: str
    ) -> Optional[BfCandle]:
        async with self._lock:
            return (self._current.get(product_code) or {}).get(timeframe)

    async def get_latest_complete_open_time(
        self, product_code: str, timeframe: str
    ) -> Optional[datetime]:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(BfCandle.open_time)
                .where(and_(
                    BfCandle.product_code == product_code,
                    BfCandle.timeframe == timeframe,
                    BfCandle.is_complete == True,
                ))
                .order_by(desc(BfCandle.open_time))
                .limit(1)
            )
            return result.scalar_one_or_none()

    async def get_rsi(
        self, product_code: str, timeframe: str, period: int = 14
    ) -> Optional[float]:
        candles = await self.get_completed_candles(product_code, timeframe, limit=period + 1)
        if len(candles) < period + 1:
            return None
        closes = [float(c.close) for c in candles]
        gains, losses = [], []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i - 1]
            gains.append(max(change, 0))
            losses.append(max(-change, 0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    # ── 백필 (REST) ─────────────────────────────────────────────

    async def backfill(self, product_code: str, days: int = 7) -> int:
        """
        BitFlyer GET /v1/getexecutions で歴史的な約定を遡及してキャンドル再構成.
        count=500 で1回呼び出し (APIレート制限考慮).
        """
        since = await self._backfill_start_time(product_code, days)
        logger.info(f"[BfCandleService] 백필 시작: {product_code} since={since.isoformat()}")
        ticks = await self._fetch_ticks_since(product_code, since)
        if not ticks:
            logger.info(f"[BfCandleService] 백필 틱 없음: {product_code}")
            return 0

        candle_map: Dict[Tuple[str, str, datetime], dict] = {}
        for tick in ticks:
            ts, p, a = tick["ts"], tick["price"], tick["amount"]
            for tf in ("1h", "4h"):
                ot = _get_candle_open_time(ts, tf)
                ct = _get_candle_close_time(ot, tf)
                key = (product_code, tf, ot)
                if key not in candle_map:
                    candle_map[key] = {
                        "product_code": product_code, "timeframe": tf,
                        "open_time": ot, "close_time": ct,
                        "open": p, "high": p, "low": p, "close": p,
                        "volume": Decimal("0"), "tick_count": 0, "is_complete": False,
                    }
                c = candle_map[key]
                if p > c["high"]:
                    c["high"] = p
                if p < c["low"]:
                    c["low"] = p
                c["close"] = p
                c["volume"] += a
                c["tick_count"] += 1

        now = datetime.now(timezone.utc)
        for c in candle_map.values():
            if c["close_time"] < now:
                c["is_complete"] = True

        count = await self._bulk_upsert_candles(list(candle_map.values()))
        logger.info(f"[BfCandleService] 백필 완료: {product_code} {count}건 upsert")
        return count

    async def _backfill_start_time(self, product_code: str, days: int) -> datetime:
        latest = await self.get_latest_complete_open_time(product_code, "1h")
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        if latest and latest > cutoff:
            return latest - timedelta(hours=1)
        return cutoff

    async def _fetch_ticks_since(self, product_code: str, since: datetime) -> list[dict]:
        settings = get_settings()
        base_url = settings.BITFLYER_BASE_URL
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                resp = await client.get(
                    f"{base_url}/v1/getexecutions",
                    params={"product_code": product_code, "count": 500},
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"[BfCandleService] 백필 API 오류: {e}")
                return []

        ticks = []
        for item in (data if isinstance(data, list) else []):
            exec_date = item.get("exec_date", "")
            try:
                ts = datetime.fromisoformat(exec_date.replace("Z", "+00:00"))
                # timezone-naive인 경우 UTC로 처리
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if ts < since:
                continue
            try:
                ticks.append({
                    "ts": ts,
                    "price": Decimal(str(item["price"])),
                    "amount": Decimal(str(item["size"])),
                })
            except (KeyError, Exception):
                continue
        logger.info(f"[BfCandleService] 백필 틱 수집: {product_code} {len(ticks)}건")
        return ticks

    # ── 내부 DB 헬퍼 ────────────────────────────────────────────

    async def _load_or_create_candle(
        self, product_code: str, tf: str, open_time: datetime,
        price: Decimal, amount: Decimal, ts: datetime,
    ) -> BfCandle:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(BfCandle).where(and_(
                    BfCandle.product_code == product_code,
                    BfCandle.timeframe == tf,
                    BfCandle.open_time == open_time,
                ))
            )
            existing = result.scalar_one_or_none()
        if existing:
            return existing
        return self._make_candle(product_code, tf, open_time, price, amount, ts)

    @staticmethod
    def _make_candle(
        product_code: str, tf: str, open_time: datetime,
        price: Decimal, amount: Decimal, ts: datetime,
    ) -> BfCandle:
        candle = BfCandle()
        candle.product_code = product_code
        candle.timeframe = tf
        candle.open_time = open_time
        candle.close_time = _get_candle_close_time(open_time, tf)
        candle.open = price
        candle.high = price
        candle.low = price
        candle.close = price
        candle.volume = amount
        candle.tick_count = 1
        candle.is_complete = False
        candle.created_at = ts
        candle.updated_at = ts
        return candle

    async def _upsert_candle(self, candle: BfCandle) -> None:
        async with AsyncSessionLocal() as db:
            stmt = pg_insert(BfCandle).values(
                product_code=candle.product_code, timeframe=candle.timeframe,
                open_time=candle.open_time, close_time=candle.close_time,
                open=candle.open, high=candle.high, low=candle.low, close=candle.close,
                volume=candle.volume, tick_count=candle.tick_count, is_complete=candle.is_complete,
                created_at=candle.created_at or datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            ).on_conflict_do_update(
                constraint="uq_bf_candles_pc_tf_open",
                set_={
                    "high": candle.high, "low": candle.low, "close": candle.close,
                    "volume": candle.volume, "tick_count": candle.tick_count,
                    "is_complete": candle.is_complete,
                    "updated_at": datetime.now(timezone.utc),
                },
            )
            await db.execute(stmt)
            await db.commit()

    async def _bulk_upsert_candles(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            for row in rows:
                stmt = pg_insert(BfCandle).values(
                    product_code=row["product_code"], timeframe=row["timeframe"],
                    open_time=row["open_time"], close_time=row["close_time"],
                    open=row["open"], high=row["high"], low=row["low"], close=row["close"],
                    volume=row["volume"], tick_count=row["tick_count"], is_complete=row["is_complete"],
                    created_at=now, updated_at=now,
                ).on_conflict_do_update(
                    constraint="uq_bf_candles_pc_tf_open",
                    set_={
                        "high": row["high"], "low": row["low"], "close": row["close"],
                        "volume": row["volume"], "tick_count": row["tick_count"],
                        "is_complete": row["is_complete"], "updated_at": now,
                    },
                )
                await db.execute(stmt)
            await db.commit()
        return len(rows)


_bf_candle_service: Optional[BfCandleService] = None


def get_bf_candle_service() -> BfCandleService:
    global _bf_candle_service
    if _bf_candle_service is None:
        _bf_candle_service = BfCandleService()
    return _bf_candle_service
