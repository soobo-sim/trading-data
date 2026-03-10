"""
Coincheck Public API 클라이언트 (인증 불필요)
coinmarket-data 서비스용 — coincheck-trader에서 이관
"""
import httpx
from typing import Optional
import logging
from app import __version__
from app.core.config import get_settings

logger = logging.getLogger(__name__)

_public_client: Optional["CoincheckPublicClient"] = None


class CoincheckPublicClient:
    """Coincheck Public API 클라이언트"""

    def __init__(self):
        self.base_url = get_settings().COINCHECK_BASE_URL
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={"User-Agent": f"CoinMarketData/{__version__}"},
        )

    async def close(self):
        await self.client.aclose()

    async def get_ticker(self, pair: str = "btc_jpy") -> dict:
        """GET /api/ticker — 최신 시세"""
        url = f"{self.base_url}/api/ticker"
        response = await self.client.get(url, params={"pair": pair})
        response.raise_for_status()
        data = response.json()
        return {
            "success": True,
            "last": data.get("last"),
            "bid": data.get("bid"),
            "ask": data.get("ask"),
            "high": data.get("high"),
            "low": data.get("low"),
            "volume": str(data.get("volume")),
            "timestamp": data.get("timestamp"),
        }

    async def get_order_books(self, pair: str = "btc_jpy") -> dict:
        """GET /api/order_books — 호가창"""
        response = await self.client.get(
            f"{self.base_url}/api/order_books", params={"pair": pair}
        )
        response.raise_for_status()
        data = response.json()
        return {"success": True, "asks": data.get("asks", []), "bids": data.get("bids", [])}

    async def get_trades(self, pair: str = "btc_jpy", limit: int = 20) -> dict:
        """GET /api/trades — 최근 체결 내역"""
        if not pair or pair.strip() == "":
            pair = "btc_jpy"
        response = await self.client.get(
            f"{self.base_url}/api/trades", params={"pair": pair, "limit": 100}
        )
        response.raise_for_status()
        data = response.json()
        if "data" in data and isinstance(data["data"], list):
            data["data"] = data["data"][:limit]
        return data

    async def get_rate(self, pair: str = "btc_jpy") -> dict:
        """GET /api/rate/{pair} — 기준 환율"""
        response = await self.client.get(f"{self.base_url}/api/rate/{pair}")
        response.raise_for_status()
        data = response.json()
        return {"success": True, "rate": data.get("rate")}

    async def get_exchange_status(self) -> dict:
        """GET /api/exchange_status — 거래소 상태"""
        response = await self.client.get(f"{self.base_url}/api/exchange_status")
        response.raise_for_status()
        data = response.json()
        statuses = data.get("exchange_status", data) if isinstance(data, dict) else data
        return {"success": True, "statuses": statuses}


def get_coincheck_public_client() -> CoincheckPublicClient:
    global _public_client
    if _public_client is None:
        _public_client = CoincheckPublicClient()
    return _public_client


async def close_coincheck_public_client():
    global _public_client
    if _public_client is not None:
        await _public_client.close()
        _public_client = None
