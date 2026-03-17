"""
BitFlyer Public REST API 클라이언트 (인증 불필요)
coinmarket-data 서비스용 — bitflyer-trader에서 이관
"""
import logging
from typing import Optional
import httpx

from app.core.config import get_settings
from app.core.exceptions import MarketDataAPIError

logger = logging.getLogger(__name__)

_public_client: Optional["BitFlyerPublicClient"] = None


class BitFlyerPublicClient:
    """BitFlyer 공개 API 클라이언트 (API 키 불필요)"""

    def __init__(self):
        settings = get_settings()
        self.base_url = settings.BITFLYER_BASE_URL.rstrip("/")
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=15.0)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _get(self, path: str, params: Optional[dict] = None):
        client = await self._get_client()
        try:
            response = await client.get(self.base_url + path, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise MarketDataAPIError(
                detail=f"HTTP {e.response.status_code}: {e.response.text}",
                operation=f"GET {path}",
            )
        except httpx.RequestError as e:
            raise MarketDataAPIError(detail=str(e), operation=f"GET {path}")

    async def get_ticker(self, product_code: Optional[str] = None) -> dict:
        """GET /v1/getticker"""
        settings = get_settings()
        pc = (product_code or settings.bf_ws_products_list[0]).upper()
        return await self._get("/v1/getticker", params={"product_code": pc})

    async def get_board(self, product_code: Optional[str] = None) -> dict:
        """GET /v1/getboard — 호가창 (mid_price + bids + asks)"""
        settings = get_settings()
        pc = (product_code or settings.bf_ws_products_list[0]).upper()
        return await self._get("/v1/getboard", params={"product_code": pc})

    async def get_executions(self, product_code: Optional[str] = None, count: int = 100) -> list:
        """GET /v1/getexecutions — 공개 체결 이력"""
        settings = get_settings()
        pc = (product_code or settings.bf_ws_products_list[0]).upper()
        return await self._get("/v1/getexecutions", params={"product_code": pc, "count": count})

    async def get_exchange_status(self) -> dict:
        """GET /v1/gethealth — 거래소 상태"""
        settings = get_settings()
        pc = settings.bf_ws_products_list[0].upper()
        return await self._get("/v1/gethealth", params={"product_code": pc})

    async def get_markets(self) -> list:
        """GET /v1/getmarkets"""
        return await self._get("/v1/getmarkets")


def get_bitflyer_public_client() -> BitFlyerPublicClient:
    global _public_client
    if _public_client is None:
        _public_client = BitFlyerPublicClient()
    return _public_client


async def close_bitflyer_public_client():
    global _public_client
    if _public_client is not None:
        await _public_client.close()
        _public_client = None
