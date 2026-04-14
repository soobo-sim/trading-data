"""
Settings 설정 파싱 테스트.
"""
import pytest
from app.core.config import Settings


def _settings(**kwargs) -> Settings:
    """테스트용 Settings — .env 파일 로딩 비활성화."""
    return Settings(_env_file=None, **kwargs)


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

    def test_poll_interval_default(self, monkeypatch):
        """폴링 주기 코드 기본값 900초 (15분) — 환경변수 없을 때."""
        # database.py 등이 load_dotenv()로 OS env를 설정할 수 있으므로 명시적 격리
        monkeypatch.delenv("MARKETAUX_POLL_INTERVAL", raising=False)
        s = _settings()
        assert s.MARKETAUX_POLL_INTERVAL == 900

    def test_poll_interval_override(self):
        """폴링 주기 오버라이드."""
        s = _settings(MARKETAUX_POLL_INTERVAL=300)
        assert s.MARKETAUX_POLL_INTERVAL == 300

    def test_poll_interval_1800(self, monkeypatch):
        """폴링 주기 1800초 (30분) 설정 — 운영 .env 설정값."""
        monkeypatch.setenv("MARKETAUX_POLL_INTERVAL", "1800")
        s = _settings()
        assert s.MARKETAUX_POLL_INTERVAL == 1800

    def test_token_configured_detection(self):
        """토큰 설정 여부로 수집기 활성 판단."""
        assert not bool(_settings(MARKETAUX_API_TOKEN="").MARKETAUX_API_TOKEN)
        assert bool(_settings(MARKETAUX_API_TOKEN="abc").MARKETAUX_API_TOKEN)
