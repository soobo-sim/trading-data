"""
Settings 설정 파싱 테스트.

BUG-010: BF_WS_PRODUCTS에서 FX_BTC_JPY 누락으로 수집 중단된 사례 방지.
"""
import pytest
from app.core.config import Settings


def _settings(**kwargs) -> Settings:
    """테스트용 Settings — .env 파일 로딩 비활성화."""
    return Settings(_env_file=None, **kwargs)


class TestBfWsProductsList:

    def test_single_product(self):
        s = _settings(BF_WS_PRODUCTS="BTC_JPY")
        assert s.bf_ws_products_list == ["BTC_JPY"]

    def test_multiple_products(self):
        s = _settings(BF_WS_PRODUCTS="BTC_JPY,ETH_JPY,XRP_JPY")
        assert s.bf_ws_products_list == ["BTC_JPY", "ETH_JPY", "XRP_JPY"]

    def test_fx_btc_jpy_included(self):
        """BUG-010: FX_BTC_JPY가 포함된 설정이 올바르게 파싱되는지 확인."""
        s = _settings(BF_WS_PRODUCTS="BTC_JPY,ETH_JPY,XRP_JPY,FX_BTC_JPY")
        assert "FX_BTC_JPY" in s.bf_ws_products_list
        assert len(s.bf_ws_products_list) == 4

    def test_whitespace_trimmed(self):
        s = _settings(BF_WS_PRODUCTS=" BTC_JPY , ETH_JPY ")
        assert s.bf_ws_products_list == ["BTC_JPY", "ETH_JPY"]

    def test_lowercase_normalized_to_upper(self):
        s = _settings(BF_WS_PRODUCTS="btc_jpy,fx_btc_jpy")
        assert s.bf_ws_products_list == ["BTC_JPY", "FX_BTC_JPY"]

    def test_empty_entries_filtered(self):
        s = _settings(BF_WS_PRODUCTS="BTC_JPY,,ETH_JPY,")
        assert s.bf_ws_products_list == ["BTC_JPY", "ETH_JPY"]

    def test_empty_string(self):
        s = _settings(BF_WS_PRODUCTS="")
        assert s.bf_ws_products_list == []


class TestMarketauxSettings:
    """MARKETAUX_API_TOKEN + MARKETAUX_POLL_INTERVAL 설정 파싱 테스트."""

    def test_token_default_empty(self):
        """토큰 명시적 빈 문자열 설정 시 비활성."""
        s = _settings(MARKETAUX_API_TOKEN="")
        assert s.MARKETAUX_API_TOKEN == ""
        assert not bool(s.MARKETAUX_API_TOKEN)

    def test_token_from_env(self):
        """환경변수로 토큰 설정 가능."""
        s = _settings(MARKETAUX_API_TOKEN="my-test-token")
        assert s.MARKETAUX_API_TOKEN == "my-test-token"

    def test_poll_interval_default(self):
        """폴링 주기 기본값 900초 (15분)."""
        s = _settings()
        assert s.MARKETAUX_POLL_INTERVAL == 900

    def test_poll_interval_override(self):
        """폴링 주기 오버라이드."""
        s = _settings(MARKETAUX_POLL_INTERVAL=300)
        assert s.MARKETAUX_POLL_INTERVAL == 300

    def test_token_configured_detection(self):
        """토큰 설정 여부로 수집기 활성 판단."""
        assert not bool(_settings(MARKETAUX_API_TOKEN="").MARKETAUX_API_TOKEN)
        assert bool(_settings(MARKETAUX_API_TOKEN="abc").MARKETAUX_API_TOKEN)
