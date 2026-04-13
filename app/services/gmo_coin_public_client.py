"""
GmoCoinPublicClient — GMO 코인 Public REST API 클라이언트.

KLine(캔들), Ticker 등 인증 불필요한 공개 API.

GMO FX 클라이언트(gmo_public_client.py)와의 차이:
- Base URL: api.coin.z.com (FX: forex-api.coin.z.com)
- KLine symbol: 현물 심볼 "BTC" 사용 (FX: "USD_JPY")
- KLine에 priceType 파라미터 없음 (FX: BID/ASK 지정)
- Volume: 실제 거래량 제공 (FX: 미제공)
"""
import logging
from typing import Optional

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_instance: Optional["GmoCoinPublicClient"] = None


class GmoCoinPublicClient:
    """GMO 코인 공개 REST 클라이언트 (싱글턴)."""

    def __init__(self) -> None:
        settings = get_settings()
        self._base_url = settings.GMO_COIN_BASE_URL.rstrip("/")
        self._public_url = f"{self._base_url}/public"
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        date: str,
    ) -> list[dict]:
        """
        KLine(캔들) 데이터 조회.

        GET /public/v1/klines?symbol=BTC&interval=4hour&date=2026

        GMO FX와 달리 priceType 파라미터 없음 (단일 가격).

        Args:
            symbol:   BTC, ETH 등 현물 심볼 (대문자, _JPY 없음)
            interval: 1min, 5min, 10min, 15min, 30min, 1hour, 4hour, 8hour, 12hour, 1day, 1week, 1month
            date:     YYYYMMDD (1hour 이하) or YYYY (4hour 이상)

        Returns:
            [{"openTime": "1618588800000", "open": "6418255", "high": ..., "volume": "0.0001"}, ...]
        """
        client = await self._get_client()
        params = {
            "symbol": symbol,
            "interval": interval,
            "date": date,
        }
        url = f"{self._public_url}/v1/klines"

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"[GmoCoinPublicClient] KLine 요청 실패 {symbol}: {e}")
            return []

        if data.get("status") != 0:
            logger.warning(f"[GmoCoinPublicClient] KLine 비즈니스 에러 {symbol}: {data}")
            return []

        return data.get("data", [])

    async def get_ticker(self, symbol: str) -> dict:
        """
        Ticker 조회.

        GET /public/v1/ticker?symbol=BTC
        Response: data는 list → 해당 심볼 항목 반환
        """
        client = await self._get_client()
        url = f"{self._public_url}/v1/ticker"

        try:
            response = await client.get(url, params={"symbol": symbol})
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"[GmoCoinPublicClient] Ticker 요청 실패 {symbol}: {e}")
            return {}

        if data.get("status") != 0:
            return {}

        items = data.get("data", [])
        if not items:
            return {}
        if isinstance(items, list):
            return items[0]
        return items


def get_gmo_coin_public_client() -> GmoCoinPublicClient:
    global _instance
    if _instance is None:
        _instance = GmoCoinPublicClient()
    return _instance


async def close_gmo_coin_public_client() -> None:
    global _instance
    if _instance is not None:
        await _instance.close()
        _instance = None
