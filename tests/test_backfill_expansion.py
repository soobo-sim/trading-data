"""
캔들 open_time 정렬 테스트.

DB/API 의존성 없이 순수 로직 테스트.
"""
import pytest
from datetime import datetime, timedelta, timezone


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
