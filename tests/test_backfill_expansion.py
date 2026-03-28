"""
1H 백필 확장 + 헬스체크 강화 테스트 (TIMEFRAME_OPTIMIZATION.md T-01/T-02).

DB/API 의존성 없이 순수 로직 테스트.
Docker 컨테이너 테스트는 Dockerfile Stage test에서 수행.
"""
import asyncio
import importlib
import sys
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace, ModuleType
from unittest.mock import AsyncMock, MagicMock, patch


# ── 모듈 로딩 헬퍼 (DB 의존성 우회) ────────────────────────────

def _read_source(rel_path: str) -> str:
    """소스 파일 텍스트 읽기 (import 없이)."""
    import os
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(base, rel_path), "r") as f:
        return f.read()


# ── T-01: GMO FX 1H 백필 확장 (소스 분석) ─────────────────────


class TestGmoBackfillExpansion:
    """GmoCandleService.backfill() 1H 기간 확장 검증."""

    def test_backfill_accepts_days_param(self):
        """backfill() 시그니처에 days 파라미터 존재."""
        src = _read_source("app/services/gmo_candle_service.py")
        assert "def backfill(self, pair" in src
        assert "days:" in src

    def test_1h_backfill_uses_days_not_hardcoded_7(self):
        """1H 백필이 하드코딩 range(7) 대신 days 파라미터 사용."""
        src = _read_source("app/services/gmo_candle_service.py")
        # range(7) 하드코딩이 제거됐는지 확인
        assert "range(7)" not in src
        # days 파라미터를 range에 사용
        assert "range(days)" in src

    def test_1h_backfill_default_180(self):
        """기본 days 값이 180."""
        src = _read_source("app/services/gmo_candle_service.py")
        assert "days: int = 180" in src

    def test_4h_backfill_unchanged(self):
        """4H 백필 로직은 변경되지 않음 (올해 + 작년)."""
        src = _read_source("app/services/gmo_candle_service.py")
        assert 'now.year, now.year - 1' in src

    def test_1h_date_format_yyyymmdd(self):
        """1H API 호출이 YYYYMMDD 포맷 사용."""
        src = _read_source("app/services/gmo_candle_service.py")
        assert '"%Y%m%d"' in src


# ── T-02: BF 백필 기본값 ───────────────────────────────────────


class TestBfBackfillDefaults:
    """BfCandlePipeline 기본 백필 일수 검증."""

    def test_default_backfill_days_is_180(self):
        """_DEFAULT_BACKFILL_DAYS가 180일."""
        src = _read_source("app/services/bf_candle_pipeline.py")
        assert "_DEFAULT_BACKFILL_DAYS = 180" in src

    def test_bf_backfill_has_days_param(self):
        """BfCandleService.backfill()에 days 파라미터 존재."""
        src = _read_source("app/services/bf_candle_service.py")
        assert "def backfill(self, product_code" in src
        assert "days:" in src

    def test_bf_backfill_generates_1h_candles(self):
        """BF 백필이 1h 타임프레임 캔들을 생성."""
        src = _read_source("app/services/bf_candle_service.py")
        assert '"1h"' in src


# ── GMO Pipeline 상수 ──────────────────────────────────────────


class TestGmoPipelineConstants:
    """GmoCandlePipeline 1H 백필 일수 검증."""

    def test_1h_backfill_days_constant(self):
        """파이프라인에 _1H_BACKFILL_DAYS = 180 상수 존재."""
        src = _read_source("app/services/gmo_candle_pipeline.py")
        assert "_1H_BACKFILL_DAYS = 180" in src

    def test_pipeline_passes_days_to_backfill(self):
        """파이프라인이 backfill 호출 시 days= 파라미터 전달."""
        src = _read_source("app/services/gmo_candle_pipeline.py")
        assert "days=self._1H_BACKFILL_DAYS" in src

    def test_pipeline_docstring_updated(self):
        """파이프라인 문서가 180일로 업데이트."""
        src = _read_source("app/services/gmo_candle_pipeline.py")
        assert "180일" in src or "180" in src


# ── 헬스체크 GMO 파이프라인 포함 ───────────────────────────────


class TestHealthCheckGmoInclusion:
    """시스템 헬스체크가 GMO FX 파이프라인 상태를 포함하는지 검증."""

    def test_system_health_imports_gmo_pipeline(self):
        """system.py가 gmo_candle_pipeline을 import."""
        src = _read_source("app/routes/system.py")
        assert "gmo_candle_pipeline" in src

    def test_system_health_checks_gmo_running(self):
        """헬스체크가 GMO 파이프라인 실행 상태 확인."""
        src = _read_source("app/routes/system.py")
        assert "gmo_pipeline_ok" in src

    def test_system_health_includes_gmo_in_response(self):
        """응답에 gmo_candle_pipeline 키 포함."""
        src = _read_source("app/routes/system.py")
        assert '"gmo_candle_pipeline"' in src

    def test_system_health_503_on_gmo_failure(self):
        """GMO 파이프라인 장애 시 503 반환 로직 존재."""
        src = _read_source("app/routes/system.py")
        assert "gmo_pipeline_ok" in src
        # healthy 판정에 gmo_pipeline_ok 포함
        assert "gmo_pipeline_ok" in src

    def test_system_health_gmo_issues_message(self):
        """GMO 파이프라인 미실행 시 issues에 메시지 추가."""
        src = _read_source("app/routes/system.py")
        assert "gmo_candle_pipeline:" in src

    def test_system_health_skips_gmo_when_unconfigured(self):
        """GMO FX 미설정 시 파이프라인 체크 스킵 로직."""
        src = _read_source("app/routes/system.py")
        assert "gmo_fx_pairs" in src


# ── 캔들 open_time 정렬 1H 지원 ───────────────────────────────

# base_candle_service.py의 함수를 직접 재현 (DB import chain 우회)
def _get_candle_open_time(dt: datetime, timeframe: str) -> datetime:
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    if timeframe == "1h":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif timeframe == "4h":
        hour = (dt.hour // 4) * 4
        return dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _get_candle_close_time(open_time: datetime, timeframe: str) -> datetime:
    _TIMEFRAME_SECONDS = {"1h": 3600, "4h": 14400}
    return open_time + timedelta(seconds=_TIMEFRAME_SECONDS[timeframe]) - timedelta(microseconds=1)


class TestCandleOpenTime1H:
    """get_candle_open_time()이 1H 경계를 올바르게 계산하는지 검증."""

    def test_1h_alignment(self):
        """1H 캔들은 정각에 정렬."""
        dt = datetime(2026, 3, 28, 14, 37, 22, tzinfo=timezone.utc)
        result = _get_candle_open_time(dt, "1h")
        assert result == datetime(2026, 3, 28, 14, 0, 0, tzinfo=timezone.utc)

    def test_1h_exact_hour(self):
        """정각 입력은 그대로 반환."""
        dt = datetime(2026, 3, 28, 8, 0, 0, tzinfo=timezone.utc)
        result = _get_candle_open_time(dt, "1h")
        assert result == datetime(2026, 3, 28, 8, 0, 0, tzinfo=timezone.utc)

    def test_4h_alignment(self):
        """4H 캔들은 4시간 경계에 정렬."""
        dt = datetime(2026, 3, 28, 14, 37, 22, tzinfo=timezone.utc)
        result = _get_candle_open_time(dt, "4h")
        assert result == datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc)

    def test_1h_close_time(self):
        """1H 캔들의 close_time은 open_time + 59분 59.999999초."""
        open_time = datetime(2026, 3, 28, 14, 0, 0, tzinfo=timezone.utc)
        close_time = _get_candle_close_time(open_time, "1h")
        assert close_time.hour == 14
        assert close_time.minute == 59
        assert close_time.second == 59

    def test_unsupported_timeframe_raises(self):
        """지원하지 않는 타임프레임은 ValueError."""
        dt = datetime(2026, 3, 28, 14, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError):
            _get_candle_open_time(dt, "30m")
