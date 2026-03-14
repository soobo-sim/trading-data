"""
CandleService — Coincheck OHLCV 캔들 집계 및 관리
coinmarket-data 서비스용 — coincheck-trader에서 이관

두 가지 소스로 캔들 구성:
1. REST 백필: GET /api/trades 단일 호출로 최근 100틱 소급
2. WS 틱 스트림: CoincheckWSClient의 [pair]-trades 채널 실시간 집계

캔들 시간 경계(UTC 기준 고정):
  1H: 매 정각
  4H: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Dict, Tuple

import httpx
from sqlalchemy import select, and_, desc, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.database import AsyncSessionLocal
from app.models.database import CkCandle

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


class CandleService:
    """
    Coincheck 캔들 집계 서비스
    - 인메모리 버퍼로 진행 중인 캔들 관리
    - 캔들 완성 시 DB에 is_complete=True로 UPSERT
    - 진행 중 캔들도 주기적으로 UPSERT (재시작 시 복구용)
    """

    def __init__(self):
        self._current: Dict[str, Dict[str, CkCandle]] = {}
        self._lock = asyncio.Lock()

    # ── 틱 처리 ────────────────────────────────────────────────

    async def process_tick(self, pair: str, price: Decimal, amount: Decimal, ts: datetime) -> list[str]:
        """실시간 틱 1건 처리. 캔들이 완성되면 완성된 timeframe 목록 반환."""
        completed = []
        async with self._lock:
            for tf in ("1h", "4h"):
                open_time = _get_candle_open_time(ts, tf)
                curr = self._current.get(pair, {}).get(tf)

                if curr is None:
                    curr = await self._load_or_create_candle(pair, tf, open_time, price, amount, ts)
                elif curr.open_time != open_time:
                    curr.is_complete = True
                    await self._upsert_candle(curr)
                    logger.info(f"[CandleService] 캔들 완성: {pair} {tf} {curr.open_time}")
                    completed.append(tf)
                    curr = self._make_candle(pair, tf, open_time, price, amount, ts)

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

                if pair not in self._current:
                    self._current[pair] = {}
                self._current[pair][tf] = curr
        return completed

    async def recover_stale_candles(self) -> int:
        """
        기동 시 복구 로직 — close_time이 경과했으나 is_complete=False인 캔들을 일괄 완결 처리.
        장애/재시작으로 인해 메모리에서 flush되지 못한 캔들 대비.
        """
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(CkCandle)
                .where(and_(CkCandle.is_complete == False, CkCandle.close_time < now))
                .values(is_complete=True, updated_at=now)
                .returning(CkCandle.pair, CkCandle.timeframe, CkCandle.open_time)
            )
            rows = result.fetchall()
            await db.commit()
        if rows:
            logger.warning(
                "[CandleService] 장애 복구: stale ck_candles %d건 is_complete=True 처리",
                len(rows),
            )
            for r in rows:
                logger.info("  → ck_candles: %s %s open=%s", r.pair, r.timeframe, r.open_time)
        return len(rows)

    async def flush_current_candles(self, pair: str) -> None:
        """진행 중인 캔들을 DB에 저장 (재시작 대비 주기 저장용)."""
        async with self._lock:
            now = datetime.now(timezone.utc)
            for tf, candle in (self._current.get(pair) or {}).items():
                if not candle.is_complete and candle.close_time < now:
                    candle.is_complete = True
                    logger.info(
                        "[CandleService] %s %s: close_time 경과로 캔들 자동 완결 open=%s",
                        pair, tf, candle.open_time,
                    )
                candle.updated_at = now
                await self._upsert_candle(candle)

    # ── 조회 ────────────────────────────────────────────────────

    async def get_completed_candles(self, pair: str, timeframe: str, limit: int = 60) -> list[CkCandle]:
        """완성된 캔들 최신순으로 N개 반환."""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(CkCandle)
                .where(and_(CkCandle.pair == pair, CkCandle.timeframe == timeframe, CkCandle.is_complete == True))
                .order_by(desc(CkCandle.open_time))
                .limit(limit)
            )
            candles = list(result.scalars().all())
        return list(reversed(candles))

    async def get_current_candle(self, pair: str, timeframe: str) -> Optional[CkCandle]:
        """진행 중인 캔들 반환 (메모리 우선)."""
        async with self._lock:
            return (self._current.get(pair) or {}).get(timeframe)

    async def get_latest_complete_open_time(self, pair: str, timeframe: str) -> Optional[datetime]:
        """DB에 있는 가장 최신 완성 캔들의 open_time."""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(CkCandle.open_time)
                .where(and_(CkCandle.pair == pair, CkCandle.timeframe == timeframe, CkCandle.is_complete == True))
                .order_by(desc(CkCandle.open_time))
                .limit(1)
            )
            return result.scalar_one_or_none()

    async def get_rsi(self, pair: str, timeframe: str, period: int = 14) -> Optional[float]:
        """완성된 캔들 종가로 RSI(period) 계산."""
        candles = await self.get_completed_candles(pair, timeframe, limit=period + 1)
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

    async def get_ema(self, pair: str, timeframe: str, period: int = 20) -> Optional[dict]:
        """
        완성된 캔들 종가로 EMA(period) 계산.
        반환: {ema, slope_pct, candles_used}
          - ema: 최신 EMA 값
          - slope_pct: 직전 EMA 대비 변화율 (%) — 양수=상승 기울기
          - candles_used: 실제 사용된 캔들 수
        period+1 개 미만이면 None 반환.
        """
        # EMA 계산에는 최소 period*2 개가 있으면 정밀도 향상, 최소 period+1 개 필요
        limit = max(period * 2, period + 1)
        candles = await self.get_completed_candles(pair, timeframe, limit=limit)
        if len(candles) < period + 1:
            return None

        closes = [float(c.close) for c in candles]
        # 지수이동평균: k = 2 / (period + 1)
        k = 2.0 / (period + 1)
        ema = sum(closes[:period]) / period  # SMA로 초기값
        for price in closes[period:]:
            ema = price * k + ema * (1 - k)

        # slope: 직전 캔들까지의 EMA 재계산
        ema_prev = sum(closes[:period]) / period
        for price in closes[period:-1]:
            ema_prev = price * k + ema_prev * (1 - k)

        slope_pct = round((ema - ema_prev) / ema_prev * 100, 4) if ema_prev > 0 else 0.0

        return {
            "ema": round(ema, 6),
            "slope_pct": slope_pct,
            "candles_used": len(closes),
        }

    async def get_atr(self, pair: str, timeframe: str, period: int = 14) -> Optional[dict]:
        """
        완성된 캔들로 ATR(period) 계산.
        반환: {atr, atr_pct, candles_used}
          - atr: ATR 절대값 (JPY)
          - atr_pct: ATR / 현재가 × 100 (%)
          - candles_used: 실제 사용된 캔들 수
        period+1 개 미만이면 None 반환.
        """
        candles = await self.get_completed_candles(pair, timeframe, limit=period + 1)
        if len(candles) < period + 1:
            return None

        trs = []
        for i in range(1, len(candles)):
            h = float(candles[i].high)
            l = float(candles[i].low)
            prev_c = float(candles[i - 1].close)
            trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))

        atr_window = trs[-period:] if len(trs) >= period else trs
        atr = sum(atr_window) / len(atr_window)
        last_close = float(candles[-1].close)
        atr_pct = round(atr / last_close * 100, 4) if last_close > 0 else 0.0

        return {
            "atr": round(atr, 6),
            "atr_pct": atr_pct,
            "candles_used": len(candles),
        }

    # ── 백필 (REST) ─────────────────────────────────────────────

    async def backfill(self, pair: str, days: int = 7) -> int:
        """
        GET /api/trades 단일 호출로 최근 틱을 소급해 캔들 재구성.
        NOTE: 2026년 기준 Coincheck API는 limit=100 단일 호출만 가능.
        """
        since = await self._backfill_start_time(pair, days)
        logger.info(f"[CandleService] 백필 시작: {pair} since={since.isoformat()}")
        ticks = await self._fetch_ticks_since(pair, since)
        if not ticks:
            return 0

        candle_map: Dict[Tuple[str, str, datetime], dict] = {}
        for tick in ticks:
            ts, p, a = tick["ts"], tick["price"], tick["amount"]
            for tf in ("1h", "4h"):
                ot = _get_candle_open_time(ts, tf)
                ct = _get_candle_close_time(ot, tf)
                key = (pair, tf, ot)
                if key not in candle_map:
                    candle_map[key] = {
                        "pair": pair, "timeframe": tf,
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
        logger.info(f"[CandleService] 백필 완료: {pair} {count}건 upsert")
        return count

    async def _backfill_start_time(self, pair: str, days: int) -> datetime:
        latest = await self.get_latest_complete_open_time(pair, "1h")
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        if latest and latest > cutoff:
            return latest - timedelta(hours=1)
        return cutoff

    async def _fetch_ticks_since(self, pair: str, since: datetime) -> list[dict]:
        settings = get_settings()
        base_url = settings.COINCHECK_BASE_URL
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                resp = await client.get(
                    f"{base_url}/api/trades",
                    params={"pair": pair, "limit": 100, "order": "desc"},
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"[CandleService] 백필 API 오류: {e}")
                return []

        all_ticks = []
        for t in data.get("data", []):
            ts_str = t.get("created_at", "")
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                continue
            if ts < since:
                continue
            all_ticks.append({
                "ts": ts,
                "price": Decimal(str(t["rate"])),
                "amount": Decimal(str(t["amount"])),
            })
        logger.info(f"[CandleService] 백필 틱 수집: {len(all_ticks)}건 (단일 호출 100틱 한정)")
        return all_ticks

    # ── 내부 DB 헬퍼 ────────────────────────────────────────────

    async def _load_or_create_candle(
        self, pair: str, tf: str, open_time: datetime,
        price: Decimal, amount: Decimal, ts: datetime,
    ) -> CkCandle:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(CkCandle).where(and_(
                    CkCandle.pair == pair,
                    CkCandle.timeframe == tf,
                    CkCandle.open_time == open_time,
                ))
            )
            existing = result.scalar_one_or_none()
        if existing:
            return existing
        return self._make_candle(pair, tf, open_time, price, amount, ts)

    @staticmethod
    def _make_candle(
        pair: str, tf: str, open_time: datetime,
        price: Decimal, amount: Decimal, ts: datetime,
    ) -> CkCandle:
        candle = CkCandle()
        candle.pair = pair
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

    async def _upsert_candle(self, candle: CkCandle) -> None:
        async with AsyncSessionLocal() as db:
            stmt = pg_insert(CkCandle).values(
                pair=candle.pair, timeframe=candle.timeframe,
                open_time=candle.open_time, close_time=candle.close_time,
                open=candle.open, high=candle.high, low=candle.low, close=candle.close,
                volume=candle.volume, tick_count=candle.tick_count, is_complete=candle.is_complete,
                created_at=candle.created_at or datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            ).on_conflict_do_update(
                index_elements=["pair", "timeframe", "open_time"],
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
                stmt = pg_insert(CkCandle).values(
                    pair=row["pair"], timeframe=row["timeframe"],
                    open_time=row["open_time"], close_time=row["close_time"],
                    open=row["open"], high=row["high"], low=row["low"], close=row["close"],
                    volume=row["volume"], tick_count=row["tick_count"], is_complete=row["is_complete"],
                    created_at=now, updated_at=now,
                ).on_conflict_do_update(
                    index_elements=["pair", "timeframe", "open_time"],
                    set_={
                        "high": row["high"], "low": row["low"], "close": row["close"],
                        "volume": row["volume"], "tick_count": row["tick_count"],
                        "is_complete": row["is_complete"], "updated_at": now,
                    },
                )
                await db.execute(stmt)
            await db.commit()
        return len(rows)


_candle_service: Optional[CandleService] = None


def get_candle_service() -> CandleService:
    global _candle_service
    if _candle_service is None:
        _candle_service = CandleService()
    return _candle_service
