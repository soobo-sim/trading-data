"""
GmoPublicClient — GMO FX Public REST API 클라이언트.

KLine(캔들), Ticker, Symbols 등 인증 불필요한 공개 API.
"""
import logging
from typing import Optional

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_instance: Optional["GmoPublicClient"] = None


class GmoPublicClient:
    """GMO FX 공개 REST 클라이언트 (싱글턴)."""

    def __init__(self) -> None:
        settings = get_settings()
        self._base_url = settings.GMOFX_BASE_URL.rstrip("/")
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
        price_type: str = "BID",
    ) -> list[dict]:
        """
        KLine(캔들) 데이터 조회.

        GET /public/v1/klines?symbol=USD_JPY&priceType=BID&interval=4hour&date=2026

        Args:
            symbol:     USD_JPY 등 (대문자)
            interval:   1hour, 4hour 등
            date:       YYYYMMDD (1hour 이하) or YYYY (4hour 이상)
            price_type: ASK or BID

        Returns:
            [{"openTime": "1679011200000", "open": "130.5", "high": "131.0", "low": "130.2", "close": "130.8"}, ...]
        """
        client = await self._get_client()
        params = {
            "symbol": symbol,
            "priceType": price_type,
            "interval": interval,
            "date": date,
        }
        url = f"{self._public_url}/v1/klines"

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"[GmoPublicClient] KLine 요청 실패: {e}")
            return []

        if data.get("status") != 0:
            logger.warning(f"[GmoPublicClient] KLine 비즈니스 에러: {data}")
            return []

        return data.get("data", [])

    async def get_ticker(self, symbol: str) -> dict:
        """
        Ticker 조회.

        GET /public/v1/ticker?symbol=USD_JPY
        """
        client = await self._get_client()
        url = f"{self._public_url}/v1/ticker"

        try:
            response = await client.get(url, params={"symbol": symbol})
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"[GmoPublicClient] Ticker 요청 실패: {e}")
            return {}

        if data.get("status") != 0:
            return {}

        items = data.get("data", [])
        return items[0] if isinstance(items, list) and items else items

    async def get_status(self) -> dict:
        """서비스 상태 조회."""
        client = await self._get_client()
        url = f"{self._public_url}/v1/status"

        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get("data", {})
        except httpx.HTTPError as e:
            logger.error(f"[GmoPublicClient] Status 요청 실패: {e}")
            return {}


def get_gmo_public_client() -> GmoPublicClient:
    global _instance
    if _instance is None:
        _instance = GmoPublicClient()
    return _instance


async def close_gmo_public_client() -> None:
    global _instance
    if _instance is not None:
        await _instance.close()
        _instance = None
