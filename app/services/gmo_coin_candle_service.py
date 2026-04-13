"""
GmoCoinCandleService — GMO 코인 OHLCV 캔들 관리.

GMO FX 캔들 서비스와 동일한 구조 (KLine API 폴링).
주요 차이점:
- 모델: GmocCandle (gmoc_candles 테이블)
- KLine 심볼: PAIR→현물심볼 변환 필요 (btc_jpy→BTC 등)
- priceType 없음 (GMO 코인은 단일 가격)
- Volume: 실제 거래량 저장 (GMO FX는 0 고정)

수집 전략:
- 4H: GET /public/v1/klines?interval=4hour&date={YYYY} → 연간 전체
- 1H: GET /public/v1/klines?interval=1hour&date={YYYYMMDD} → 일별 전체
- 폴링 주기: 5분마다 최신 캔들 갱신
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import AsyncSessionLocal
from app.models.database import GmocCandle
from app.services.gmo_coin_public_client import get_gmo_coin_public_client
from app.services.base_candle_service import (
    get_candle_close_time as _get_candle_close_time,
)

logger = logging.getLogger(__name__)

# KLine API interval 매핑
_INTERVAL_MAP = {"1h": "1hour", "4h": "4hour"}

# 4hour → date=YYYY, 1hour → date=YYYYMMDD
_DATE_FORMAT_MAP = {"1h": "%Y%m%d", "4h": "%Y"}

# 페어 → KLine 현물 심볼 (GMO 코인 KLine API는 현물 심볼 사용)
_PAIR_TO_KLINE_SYMBOL: dict[str, str] = {
    "btc_jpy": "BTC",
    "eth_jpy": "ETH",
    "bch_jpy": "BCH",
    "ltc_jpy": "LTC",
    "xrp_jpy": "XRP",
    "xem_jpy": "XEM",
    "xlm_jpy": "XLM",
    "bat_jpy": "BAT",
    "omg_jpy": "OMG",
    "qtum_jpy": "QTUM",
    "ens_jpy": "ENS",
    "mkr_jpy": "MKR",
    "dot_jpy": "DOT",
    "atom_jpy": "ATOM",
    "ada_jpy": "ADA",
    "link_jpy": "LINK",
    "doge_jpy": "DOGE",
    "sol_jpy": "SOL",
    "astar_jpy": "ASTR",
    "matic_jpy": "MATIC",
    "avax_jpy": "AVAX",
    "aptos_jpy": "APT",
    "flr_jpy": "FLR",
    "sand_jpy": "SAND",
    "mana_jpy": "MANA",
    "gala_jpy": "GALA",
    "axs_jpy": "AXS",
    "ape_jpy": "APE",
    "chz_jpy": "CHZ",
    "matic_jpy2": "MATIC",
    "sui_jpy": "SUI",
}


class GmoCoinCandleService:
    """
    GMO 코인 캔들 서비스.

    KLine API를 폴링하여 DB에 UPSERT.
    API가 완성된 캔들을 제공하므로 tick 집계 불필요.
    """

    def __init__(self):
        self._lock = asyncio.Lock()

    def _to_kline_symbol(self, pair: str) -> str:
        """페어 → KLine 심볼 변환. 매핑 없으면 pair 앞부분 대문자 사용."""
        key = pair.lower()
        if key in _PAIR_TO_KLINE_SYMBOL:
            return _PAIR_TO_KLINE_SYMBOL[key]
        # fallback: btc_jpy → BTC
        return key.split("_")[0].upper()

    async def poll_and_upsert(self, pair: str, timeframe: str = "4h") -> int:
        """
        KLine API 폴링 → 신규/갱신 캔들 DB UPSERT.

        Returns:
            upsert된 캔들 수.
        """
        client = get_gmo_coin_public_client()
        symbol = self._to_kline_symbol(pair)
        interval = _INTERVAL_MAP[timeframe]
        now = datetime.now(timezone.utc)
        date_str = now.strftime(_DATE_FORMAT_MAP[timeframe])

        # GMO 코인: priceType 파라미터 없음 (단일 가격)
        klines = await client.get_klines(
            symbol=symbol,
            interval=interval,
            date=date_str,
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

                        is_complete = now > close_time

                        stmt = pg_insert(GmocCandle).values(
                            pair=pair.lower(),
                            timeframe=timeframe,
                            open_time=open_time,
                            close_time=close_time,
                            open=Decimal(kl["open"]),
                            high=Decimal(kl["high"]),
                            low=Decimal(kl["low"]),
                            close=Decimal(kl["close"]),
                            volume=Decimal(kl["volume"]),
                            tick_count=0,
                            is_complete=is_complete,
                        )
                        stmt = stmt.on_conflict_do_update(
                            constraint="gmoc_candles_pkey",
                            set_={
                                "high": stmt.excluded.high,
                                "low": stmt.excluded.low,
                                "close": stmt.excluded.close,
                                "volume": stmt.excluded.volume,
                                "is_complete": stmt.excluded.is_complete,
                                "updated_at": now,
                            },
                        )
                        await session.execute(stmt)
                        count += 1
                    except (KeyError, ValueError) as e:
                        logger.debug(f"[GmoCoinCandleService] KLine 파싱 오류: {e}")
                        continue

                await session.commit()

        return count

    async def backfill(
        self,
        pair: str,
        timeframe: str = "4h",
        year: Optional[int] = None,
        days: int = 180,
    ) -> int:
        """
        과거 캔들 백필.

        4H: date=YYYY → 연간 전체
        1H: date=YYYYMMDD → 최근 N일 (기본 180일)
        """
        client = get_gmo_coin_public_client()
        symbol = self._to_kline_symbol(pair)
        interval = _INTERVAL_MAP[timeframe]
        now = datetime.now(timezone.utc)
        total = 0

        if timeframe == "4h":
            years = [year] if year else [now.year, now.year - 1]
            for y in years:
                klines = await client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    date=str(y),
                )
                if klines:
                    count = await self._upsert_klines(pair, timeframe, klines, now)
                    total += count
                    logger.info(
                        f"[GmoCoinCandleService] 백필 {symbol} {timeframe} {y}: {count}건"
                    )

        elif timeframe == "1h":
            for i in range(days):
                day = now - timedelta(days=i)
                date_str = day.strftime("%Y%m%d")
                klines = await client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    date=date_str,
                )
                if klines:
                    count = await self._upsert_klines(pair, timeframe, klines, now)
                    total += count
                # GMO 코인 레이트 리밋 배려 (Public GET 제한)
                await asyncio.sleep(0.2)
            logger.info(
                f"[GmoCoinCandleService] 1H 백필 {symbol}: {days}일, 총 {total}건"
            )

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

                    stmt = pg_insert(GmocCandle).values(
                        pair=pair.lower(),
                        timeframe=timeframe,
                        open_time=open_time,
                        close_time=close_time,
                        open=Decimal(kl["open"]),
                        high=Decimal(kl["high"]),
                        low=Decimal(kl["low"]),
                        close=Decimal(kl["close"]),
                        volume=Decimal(kl["volume"]),
                        tick_count=0,
                        is_complete=is_complete,
                    )
                    stmt = stmt.on_conflict_do_update(
                        constraint="gmoc_candles_pkey",
                        set_={
                            "high": stmt.excluded.high,
                            "low": stmt.excluded.low,
                            "close": stmt.excluded.close,
                            "volume": stmt.excluded.volume,
                            "is_complete": stmt.excluded.is_complete,
                            "updated_at": now,
                        },
                    )
                    await session.execute(stmt)
                    count += 1
                except (KeyError, ValueError) as e:
                    logger.debug(f"[GmoCoinCandleService] 파싱 오류: {e}")
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
                update(GmocCandle)
                .where(
                    and_(
                        GmocCandle.is_complete.is_(False),
                        GmocCandle.close_time < now,
                    )
                )
                .values(is_complete=True, updated_at=now)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount


_instance: Optional[GmoCoinCandleService] = None


def get_gmo_coin_candle_service() -> GmoCoinCandleService:
    global _instance
    if _instance is None:
        _instance = GmoCoinCandleService()
    return _instance
