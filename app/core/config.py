"""Application configuration for CoinMarket Data Service"""
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional
from app import __version__


class Settings(BaseSettings):
    """Application settings"""

    # API
    API_VERSION: str = __version__
    API_TITLE: str = "CoinMarket Data API"
    API_DESCRIPTION: str = "Coincheck & BitFlyer 마켓 데이터 수집 및 제공 서비스"
    DEBUG: bool = False

    # Database (coincheck-trader / bitflyer-trader 와 동일 인스턴스 공유)
    DATABASE_URL: str = "postgresql+asyncpg://trader:trader_password_123@localhost:5432/trader_db"

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_RETENTION_DAYS: int = 14

    # ── Coincheck ─────────────────────────────────────────────────
    COINCHECK_BASE_URL: str = "https://coincheck.com"
    COINCHECK_WS_URL: str = "wss://ws-api.coincheck.com"

    # WebSocket 설정 (Coincheck Public)
    CK_WS_PAIRS: str = "btc_jpy"          # 쉼표 구분 (예: "btc_jpy,eth_jpy")
    WS_WINDOW_SEC: int = 30               # market-pulse 분석 윈도우 (초)
    WS_MAX_TRADES: int = 1000             # 메모리에 보관할 최대 체결 건수
    WS_RECONNECT_MAX_DELAY: int = 30      # 재접속 최대 대기 시간 (초)

    # ── BitFlyer ──────────────────────────────────────────────────
    BITFLYER_BASE_URL: str = "https://api.bitflyer.com"
    BITFLYER_WS_URL: str = "wss://ws.lightstream.bitflyer.com/json-rpc"
    BF_WS_PRODUCTS: str = "BTC_JPY"               # 쉼표 구분 (예: "BTC_JPY,ETH_JPY,XRP_JPY")

    # ── GMO FX ────────────────────────────────────────────────
    GMOFX_BASE_URL: str = "https://forex-api.coin.z.com"
    GMO_FX_PAIRS: str = ""                         # 쉼표 구분 (예: "USD_JPY,EUR_JPY"), 빈 문자열이면 비활성

    @property
    def ck_ws_pairs_list(self) -> list[str]:
        return [p.strip() for p in self.CK_WS_PAIRS.split(",") if p.strip()]

    @property
    def bf_ws_products_list(self) -> list[str]:
        return [p.strip().upper() for p in self.BF_WS_PRODUCTS.split(",") if p.strip()]

    @property
    def gmo_fx_pairs_list(self) -> list[str]:
        return [p.strip().upper() for p in self.GMO_FX_PAIRS.split(",") if p.strip()]

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()
