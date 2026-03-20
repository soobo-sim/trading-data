"""
GmoCandleService — GMO FX OHLCV 캔들 관리.

CK/BF와 달리 tick 집계가 아닌 KLine API 폴링 방식.
GMO FX KLine API는 완성된 캔들을 직접 제공하므로 집계 불필요.

수집 전략:
- 4H: GET /public/v1/klines?interval=4hour&date={YYYY} → 연간 전체
- 1H: GET /public/v1/klines?interval=1hour&date={YYYYMMDD} → 일별 전체
- 폴링 주기: 5분마다 최신 캔들 갱신
- 신규 캔들 → DB UPSERT (is_complete 판정)

캔들 시간 경계 (UTC 기준, JST-9):
  1H: 매 정각 — open_time = 해당 정각, close_time = +59분59초
  4H: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00

Volume: GMO FX KLine에 volume 없음 → 0 고정.
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional

from sqlalchemy import and_, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import AsyncSessionLocal
from app.models.database import GmoCandle
from app.services.gmo_public_client import get_gmo_public_client

logger = logging.getLogger(__name__)

TIMEFRAME_SECONDS: Dict[str, int] = {"1h": 3600, "4h": 14400}

# KLine API interval 매핑
_INTERVAL_MAP = {"1h": "1hour", "4h": "4hour"}

# 4hour → date=YYYY, 1hour → date=YYYYMMDD
_DATE_FORMAT_MAP = {"1h": "%Y%m%d", "4h": "%Y"}


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


class GmoCandleService:
    """
    GMO FX 캔들 서비스.

    KLine API를 폴링하여 DB에 UPSERT.
    API가 완성된 캔들을 제공하므로 tick 집계 불필요.
    """

    def __init__(self):
        self._lock = asyncio.Lock()

    async def poll_and_upsert(self, pair: str, timeframe: str = "4h") -> int:
        """
        KLine API 폴링 → 신규/갱신 캔들 DB UPSERT.

        Returns:
            upsert된 캔들 수.
        """
        client = get_gmo_public_client()
        symbol = pair.upper()
        interval = _INTERVAL_MAP[timeframe]
        now = datetime.now(timezone.utc)
        date_str = now.strftime(_DATE_FORMAT_MAP[timeframe])

        klines = await client.get_klines(
            symbol=symbol,
            interval=interval,
            date=date_str,
            price_type="BID",
        )

        if not klines:
            return 0

        count = 0
        async with self._lock:
            async with AsyncSessionLocal() as session:
                for kl in klines:
                    try:
                        open_time_ms = int(kl["openTime"])
                        open_time = datetime.fromtimestamp(
                            open_time_ms / 1000, tz=timezone.utc
                        )
                        close_time = _get_candle_close_time(open_time, timeframe)

                        # 현재 시각 기준 완성 여부 판정
                        is_complete = now > close_time

                        stmt = pg_insert(GmoCandle).values(
                            pair=pair.lower(),
                            timeframe=timeframe,
                            open_time=open_time,
                            close_time=close_time,
                            open=Decimal(kl["open"]),
                            high=Decimal(kl["high"]),
                            low=Decimal(kl["low"]),
                            close=Decimal(kl["close"]),
                            volume=Decimal("0"),
                            tick_count=0,
                            is_complete=is_complete,
                        )
                        stmt = stmt.on_conflict_do_update(
                            constraint="gmo_candles_pkey",
                            set_={
                                "high": stmt.excluded.high,
                                "low": stmt.excluded.low,
                                "close": stmt.excluded.close,
                                "is_complete": stmt.excluded.is_complete,
                                "updated_at": now,
                            },
                        )
                        await session.execute(stmt)
                        count += 1
                    except (KeyError, ValueError) as e:
                        logger.debug(f"[GmoCandleService] KLine 파싱 오류: {e}")
                        continue

                await session.commit()

        return count

    async def backfill(self, pair: str, timeframe: str = "4h", year: Optional[int] = None) -> int:
        """
        과거 캔들 백필.

        4H: date=YYYY → 연간 전체
        1H: date=YYYYMMDD → 최근 N일
        """
        client = get_gmo_public_client()
        symbol = pair.upper()
        interval = _INTERVAL_MAP[timeframe]
        now = datetime.now(timezone.utc)
        total = 0

        if timeframe == "4h":
            # 4H: 올해 + 작년 백필
            years = [year] if year else [now.year, now.year - 1]
            for y in years:
                klines = await client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    date=str(y),
                    price_type="BID",
                )
                if klines:
                    count = await self._upsert_klines(pair, timeframe, klines, now)
                    total += count
                    logger.info(
                        f"[GmoCandleService] 백필 {symbol} {timeframe} {y}: {count}건"
                    )

        elif timeframe == "1h":
            # 1H: 최근 7일
            for i in range(7):
                day = now - timedelta(days=i)
                date_str = day.strftime("%Y%m%d")
                klines = await client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    date=date_str,
                    price_type="BID",
                )
                if klines:
                    count = await self._upsert_klines(pair, timeframe, klines, now)
                    total += count
                # 레이트 리밋 배려
                await asyncio.sleep(0.2)

        return total

    async def _upsert_klines(
        self, pair: str, timeframe: str, klines: list[dict], now: datetime
    ) -> int:
        """KLine 배열 → DB UPSERT."""
        count = 0
        async with AsyncSessionLocal() as session:
            for kl in klines:
                try:
                    open_time_ms = int(kl["openTime"])
                    open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
                    close_time = _get_candle_close_time(open_time, timeframe)
                    is_complete = now > close_time

                    stmt = pg_insert(GmoCandle).values(
                        pair=pair.lower(),
                        timeframe=timeframe,
                        open_time=open_time,
                        close_time=close_time,
                        open=Decimal(kl["open"]),
                        high=Decimal(kl["high"]),
                        low=Decimal(kl["low"]),
                        close=Decimal(kl["close"]),
                        volume=Decimal("0"),
                        tick_count=0,
                        is_complete=is_complete,
                    )
                    stmt = stmt.on_conflict_do_update(
                        constraint="gmo_candles_pkey",
                        set_={
                            "high": stmt.excluded.high,
                            "low": stmt.excluded.low,
                            "close": stmt.excluded.close,
                            "is_complete": stmt.excluded.is_complete,
                            "updated_at": now,
                        },
                    )
                    await session.execute(stmt)
                    count += 1
                except (KeyError, ValueError) as e:
                    logger.debug(f"[GmoCandleService] 파싱 오류: {e}")
                    continue

            await session.commit()
        return count

    async def recover_stale_candles(self) -> int:
        """
        기동 시 복구 — close_time이 경과했으나 is_complete=False인 캔들 일괄 완결.
        """
        from sqlalchemy import update
        now = datetime.now(timezone.utc)

        async with AsyncSessionLocal() as session:
            stmt = (
                update(GmoCandle)
                .where(
                    and_(
                        GmoCandle.is_complete.is_(False),
                        GmoCandle.close_time < now,
                    )
                )
                .values(is_complete=True, updated_at=now)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount


_instance: Optional[GmoCandleService] = None


def get_gmo_candle_service() -> GmoCandleService:
    global _instance
    if _instance is None:
        _instance = GmoCandleService()
    return _instance
