"""
BfCandleService — BitFlyer OHLCV 캔들 집계 (BaseCandleService 특화)

BF 고유: REST 백필 API (GET /v1/getexecutions, product_code, price/size 필드)
"""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Dict, Tuple

import httpx

from app.core.config import get_settings
from app.models.database import BfCandle
from app.services.base_candle_service import BaseCandleService, get_candle_open_time, get_candle_close_time

logger = logging.getLogger(__name__)


class BfCandleService(BaseCandleService):
    model_class = BfCandle
    pair_column = "product_code"
    index_elements = ["product_code", "timeframe", "open_time"]
    service_name = "[BfCandleService]"

    # ── 백필 (BF REST 특화) ─────────────────────────────────────

    async def backfill(self, product_code: str, days: int = 7) -> int:
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
                ot = get_candle_open_time(ts, tf)
                ct = get_candle_close_time(ot, tf)
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


_bf_candle_service: Optional[BfCandleService] = None


def get_bf_candle_service() -> BfCandleService:
    global _bf_candle_service
    if _bf_candle_service is None:
        _bf_candle_service = BfCandleService()
    return _bf_candle_service
