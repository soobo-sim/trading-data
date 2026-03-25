"""
BaseCandleService — CK/BF 거래소 공통 OHLCV 캔들 집계 베이스 클래스

서브클래스에서 오버라이드해야 하는 속성/메서드:
  - model_class: SQLAlchemy ORM 모델 (CkCandle, BfCandle)
  - pair_column: 모델의 페어 필드명 ("pair" or "product_code")
  - index_elements: UPSERT conflict key (["pair","timeframe","open_time"] etc.)
  - service_name: 로그 프리픽스 ("[CandleService]" etc.)
  - backfill(symbol, days): 거래소별 REST API가 달라 서브클래스 구현
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Dict

from sqlalchemy import select, and_, desc, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

TIMEFRAME_SECONDS: Dict[str, int] = {"1h": 3600, "4h": 14400}


def get_candle_open_time(dt: datetime, timeframe: str) -> datetime:
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    if timeframe == "1h":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif timeframe == "4h":
        hour = (dt.hour // 4) * 4
        return dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def get_candle_close_time(open_time: datetime, timeframe: str) -> datetime:
    return open_time + timedelta(seconds=TIMEFRAME_SECONDS[timeframe]) - timedelta(microseconds=1)


class BaseCandleService:
    model_class = None          # CkCandle | BfCandle
    pair_column: str = "pair"   # "pair" | "product_code"
    index_elements: list[str] = ["pair", "timeframe", "open_time"]
    service_name: str = "[BaseCandleService]"

    def __init__(self):
        self._current: Dict[str, Dict[str, object]] = {}
        self._lock = asyncio.Lock()

    def _get_pair_attr(self, candle):
        return getattr(candle, self.pair_column)

    def _set_pair_attr(self, candle, value):
        setattr(candle, self.pair_column, value)

    def _pair_filter(self, symbol: str):
        return getattr(self.model_class, self.pair_column) == symbol

    # ── 틱 처리 ────────────────────────────────────────────────

    async def process_tick(self, symbol: str, price: Decimal, amount: Decimal, ts: datetime) -> list[str]:
        completed = []
        async with self._lock:
            for tf in ("1h", "4h"):
                open_time = get_candle_open_time(ts, tf)
                curr = self._current.get(symbol, {}).get(tf)

                if curr is None:
                    curr = await self._load_or_create_candle(symbol, tf, open_time, price, amount, ts)
                elif curr.open_time != open_time:
                    curr.is_complete = True
                    await self._upsert_candle(curr)
                    logger.info(f"{self.service_name} 캔들 완성: {symbol} {tf} {curr.open_time}")
                    completed.append(tf)
                    curr = self._make_candle(symbol, tf, open_time, price, amount, ts)

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

                if symbol not in self._current:
                    self._current[symbol] = {}
                self._current[symbol][tf] = curr
        return completed

    async def recover_stale_candles(self) -> int:
        now = datetime.now(timezone.utc)
        M = self.model_class
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(M)
                .where(and_(M.is_complete == False, M.close_time < now))
                .values(is_complete=True, updated_at=now)
                .returning(getattr(M, self.pair_column), M.timeframe, M.open_time)
            )
            rows = result.fetchall()
            await db.commit()
        if rows:
            logger.warning(
                "%s 장애 복구: stale candles %d건 is_complete=True 처리",
                self.service_name, len(rows),
            )
            for r in rows:
                logger.info("  → %s %s open=%s", r[0], r[1], r[2])
        return len(rows)

    async def flush_current_candles(self, symbol: str) -> None:
        async with self._lock:
            now = datetime.now(timezone.utc)
            for tf, candle in (self._current.get(symbol) or {}).items():
                if not candle.is_complete and candle.close_time < now:
                    candle.is_complete = True
                    logger.info(
                        "%s %s %s: close_time 경과로 캔들 자동 완결 open=%s",
                        self.service_name, symbol, tf, candle.open_time,
                    )
                candle.updated_at = now
                await self._upsert_candle(candle)

    # ── 조회 ────────────────────────────────────────────────────

    async def get_completed_candles(self, symbol: str, timeframe: str, limit: int = 60) -> list:
        M = self.model_class
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(M)
                .where(and_(self._pair_filter(symbol), M.timeframe == timeframe, M.is_complete == True))
                .order_by(desc(M.open_time))
                .limit(limit)
            )
            candles = list(result.scalars().all())
        return list(reversed(candles))

    async def get_current_candle(self, symbol: str, timeframe: str):
        async with self._lock:
            return (self._current.get(symbol) or {}).get(timeframe)

    async def get_latest_complete_open_time(self, symbol: str, timeframe: str) -> Optional[datetime]:
        M = self.model_class
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(M.open_time)
                .where(and_(self._pair_filter(symbol), M.timeframe == timeframe, M.is_complete == True))
                .order_by(desc(M.open_time))
                .limit(1)
            )
            return result.scalar_one_or_none()

    # ── 기술 지표 ──────────────────────────────────────────────

    async def get_rsi(self, symbol: str, timeframe: str, period: int = 14) -> Optional[float]:
        candles = await self.get_completed_candles(symbol, timeframe, limit=period + 1)
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

    async def get_ema(self, symbol: str, timeframe: str, period: int = 20) -> Optional[dict]:
        limit = max(period * 2, period + 1)
        candles = await self.get_completed_candles(symbol, timeframe, limit=limit)
        if len(candles) < period + 1:
            return None
        closes = [float(c.close) for c in candles]
        k = 2.0 / (period + 1)
        ema = sum(closes[:period]) / period
        for price in closes[period:]:
            ema = price * k + ema * (1 - k)
        ema_prev = sum(closes[:period]) / period
        for price in closes[period:-1]:
            ema_prev = price * k + ema_prev * (1 - k)
        slope_pct = round((ema - ema_prev) / ema_prev * 100, 4) if ema_prev > 0 else 0.0
        return {"ema": round(ema, 6), "slope_pct": slope_pct, "candles_used": len(closes)}

    async def get_atr(self, symbol: str, timeframe: str, period: int = 14) -> Optional[dict]:
        candles = await self.get_completed_candles(symbol, timeframe, limit=period + 1)
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
        return {"atr": round(atr, 6), "atr_pct": atr_pct, "candles_used": len(candles)}

    # ── 시계열 (차트용) ─────────────────────────────────────────

    async def get_ema_series(self, symbol: str, timeframe: str, period: int = 20, limit: int = 200) -> list[dict]:
        fetch_limit = limit + period
        candles = await self.get_completed_candles(symbol, timeframe, limit=fetch_limit)
        if len(candles) < period + 1:
            return []
        closes = [float(c.close) for c in candles]
        times = [c.open_time for c in candles]
        k = 2.0 / (period + 1)
        ema = sum(closes[:period]) / period
        series = []
        for i in range(period, len(closes)):
            ema = closes[i] * k + ema * (1 - k)
            series.append({"time": times[i], "value": round(ema, 6)})
        return series[-limit:]

    async def get_rsi_series(self, symbol: str, timeframe: str, period: int = 14, limit: int = 200) -> list[dict]:
        fetch_limit = limit + period + 1
        candles = await self.get_completed_candles(symbol, timeframe, limit=fetch_limit)
        if len(candles) < period + 1:
            return []
        closes = [float(c.close) for c in candles]
        times = [c.open_time for c in candles]
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        avg_gain = sum(max(c, 0) for c in changes[:period]) / period
        avg_loss = sum(max(-c, 0) for c in changes[:period]) / period
        series = []
        if avg_loss == 0:
            series.append({"time": times[period], "value": 100.0})
        else:
            rs = avg_gain / avg_loss
            series.append({"time": times[period], "value": round(100 - (100 / (1 + rs)), 2)})
        for i in range(period, len(changes)):
            avg_gain = (avg_gain * (period - 1) + max(changes[i], 0)) / period
            avg_loss = (avg_loss * (period - 1) + max(-changes[i], 0)) / period
            if avg_loss == 0:
                rsi_val = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi_val = round(100 - (100 / (1 + rs)), 2)
            series.append({"time": times[i + 1], "value": rsi_val})
        return series[-limit:]

    async def get_atr_series(self, symbol: str, timeframe: str, period: int = 14, limit: int = 200) -> list[dict]:
        fetch_limit = limit + period + 1
        candles = await self.get_completed_candles(symbol, timeframe, limit=fetch_limit)
        if len(candles) < period + 1:
            return []
        times = [c.open_time for c in candles]
        trs = []
        for i in range(1, len(candles)):
            h = float(candles[i].high)
            l = float(candles[i].low)
            prev_c = float(candles[i - 1].close)
            trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        atr = sum(trs[:period]) / period
        series = [{"time": times[period], "value": round(atr, 6)}]
        for i in range(period, len(trs)):
            atr = (atr * (period - 1) + trs[i]) / period
            series.append({"time": times[i + 1], "value": round(atr, 6)})
        return series[-limit:]

    # ── 내부 DB 헬퍼 ────────────────────────────────────────────

    async def _load_or_create_candle(self, symbol, tf, open_time, price, amount, ts):
        M = self.model_class
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(M).where(and_(self._pair_filter(symbol), M.timeframe == tf, M.open_time == open_time))
            )
            existing = result.scalar_one_or_none()
        if existing:
            return existing
        return self._make_candle(symbol, tf, open_time, price, amount, ts)

    def _make_candle(self, symbol, tf, open_time, price, amount, ts):
        candle = self.model_class()
        self._set_pair_attr(candle, symbol)
        candle.timeframe = tf
        candle.open_time = open_time
        candle.close_time = get_candle_close_time(open_time, tf)
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

    async def _upsert_candle(self, candle) -> None:
        M = self.model_class
        values = {
            self.pair_column: self._get_pair_attr(candle),
            "timeframe": candle.timeframe,
            "open_time": candle.open_time, "close_time": candle.close_time,
            "open": candle.open, "high": candle.high, "low": candle.low, "close": candle.close,
            "volume": candle.volume, "tick_count": candle.tick_count, "is_complete": candle.is_complete,
            "created_at": candle.created_at or datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        async with AsyncSessionLocal() as db:
            stmt = pg_insert(M).values(**values).on_conflict_do_update(
                index_elements=self.index_elements,
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
        M = self.model_class
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            for row in rows:
                vals = {
                    self.pair_column: row[self.pair_column],
                    "timeframe": row["timeframe"],
                    "open_time": row["open_time"], "close_time": row["close_time"],
                    "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"],
                    "volume": row["volume"], "tick_count": row["tick_count"], "is_complete": row["is_complete"],
                    "created_at": now, "updated_at": now,
                }
                stmt = pg_insert(M).values(**vals).on_conflict_do_update(
                    index_elements=self.index_elements,
                    set_={
                        "high": row["high"], "low": row["low"], "close": row["close"],
                        "volume": row["volume"], "tick_count": row["tick_count"],
                        "is_complete": row["is_complete"], "updated_at": now,
                    },
                )
                await db.execute(stmt)
            await db.commit()
        return len(rows)
