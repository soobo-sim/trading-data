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
    API_DESCRIPTION: str = "GMO Coin 마켓 데이터 수집 및 제공 서비스"
    DEBUG: bool = False

    # Database (coincheck-trader / bitflyer-trader 와 동일 인스턴스 공유)
    DATABASE_URL: str = "postgresql+asyncpg://trader:trader_password_123@localhost:5432/trader_db"

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_RETENTION_DAYS: int = 14

    # ── GMO Coin ──────────────────────────────────────────────────
    GMO_COIN_BASE_URL: str = "https://api.coin.z.com"
    GMO_COIN_PAIRS: str = ""                       # 쉼표 구분 (예: "BTC_JPY"), 빈 문자열이면 비활성

    # ── FRED API (F-04 매크로 팩터) ───────────────────────────
    FRED_API_KEY: str = ""                         # https://fred.stlouisfed.org/docs/api/api_key.html
    FRED_BASE_URL: str = "https://api.stlouisfed.org/fred"
    FRED_FETCH_HOUR_JST: int = 8                   # 매일 08:00 JST 수집

    # ── Marketaux API (뉴스 수집) ─────────────────────────────
    MARKETAUX_API_TOKEN: str = ""                  # https://www.marketaux.com/register
    MARKETAUX_POLL_INTERVAL: int = 900             # 15분 (초)

    @property
    def gmo_coin_pairs_list(self) -> list[str]:
        return [p.strip().upper() for p in self.GMO_COIN_PAIRS.split(",") if p.strip()]

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # .env에 정의되지 않은 키(TZ 등) 무시


@lru_cache()
def get_settings() -> Settings:
    return Settings()
