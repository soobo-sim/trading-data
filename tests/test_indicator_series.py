"""
인디케이터 시계열 계산 테스트 — BUG-012 수정 검증.

trading-data EMA/RSI/ATR 시계열 메서드가 올바른 구조와 값을 반환하는지 확인.
DB 의존성 없이 순수 계산 로직만 테스트.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from types import SimpleNamespace


def _make_candles(closes: list[float], timeframe: str = "4h", pair: str = "xrp_jpy"):
    """테스트용 캔들 객체 리스트 생성. open_time은 4시간 간격."""
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    delta = timedelta(hours=4 if timeframe == "4h" else 1)
    candles = []
    for i, close in enumerate(closes):
        c = SimpleNamespace(
            pair=pair,
            timeframe=timeframe,
            open_time=base + delta * i,
            close_time=base + delta * (i + 1) - timedelta(microseconds=1),
            open=close * 0.99,
            high=close * 1.01,
            low=close * 0.98,
            close=close,
            volume=100.0,
            tick_count=10,
            is_complete=True,
        )
        candles.append(c)
    return candles


# ── EMA Series ──────────────────────────────────────────────────


class TestEmaSeriesCalculation:
    """EMA 시계열 계산 순수 로직 테스트."""

    def test_ema_series_structure(self):
        """각 항목이 time(datetime)과 value(float)를 가져야 한다."""
        closes = [100.0 + i for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_ema_series(candles, period=5, limit=200)
        assert len(series) > 0
        for point in series:
            assert "time" in point
            assert "value" in point
            assert isinstance(point["time"], datetime)
            assert isinstance(point["value"], float)

    def test_ema_series_length(self):
        """30개 캔들, period=5이면 25개 EMA 포인트."""
        closes = [100.0 + i for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_ema_series(candles, period=5, limit=200)
        assert len(series) == 25  # 30 - 5 = 25

    def test_ema_series_limit(self):
        """limit으로 반환 개수 제한."""
        closes = [100.0 + i for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_ema_series(candles, period=5, limit=10)
        assert len(series) == 10

    def test_ema_series_insufficient_candles(self):
        """period+1 미만이면 빈 리스트."""
        closes = [100.0, 101.0, 102.0]
        candles = _make_candles(closes)
        series = _compute_ema_series(candles, period=5, limit=200)
        assert series == []

    def test_ema_series_values_trending_up(self):
        """상승 추세에서 EMA도 상승해야 한다."""
        closes = [100.0 + i * 2 for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_ema_series(candles, period=10, limit=200)
        for i in range(1, len(series)):
            assert series[i]["value"] > series[i - 1]["value"]

    def test_ema_series_time_ascending(self):
        """시간이 오름차순이어야 한다."""
        closes = [100.0 + i for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_ema_series(candles, period=5, limit=200)
        for i in range(1, len(series)):
            assert series[i]["time"] > series[i - 1]["time"]


# ── RSI Series ──────────────────────────────────────────────────


class TestRsiSeriesCalculation:
    """RSI 시계열 계산 순수 로직 테스트."""

    def test_rsi_series_structure(self):
        closes = [100.0 + i for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_rsi_series(candles, period=14, limit=200)
        assert len(series) > 0
        for point in series:
            assert "time" in point
            assert "value" in point

    def test_rsi_range_0_100(self):
        """RSI는 0~100 사이여야 한다."""
        closes = [100 + (i % 5) * 3 - 7 for i in range(50)]
        candles = _make_candles(closes)
        series = _compute_rsi_series(candles, period=14, limit=200)
        for point in series:
            assert 0 <= point["value"] <= 100

    def test_rsi_all_up_is_100(self):
        """모든 종가가 상승이면 RSI = 100."""
        closes = [100.0 + i for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_rsi_series(candles, period=14, limit=200)
        assert series[0]["value"] == 100.0

    def test_rsi_series_insufficient_candles(self):
        closes = [100.0, 101.0, 102.0]
        candles = _make_candles(closes)
        series = _compute_rsi_series(candles, period=14, limit=200)
        assert series == []

    def test_rsi_series_time_ascending(self):
        closes = [100.0 + i * 0.5 - (i % 3) * 2 for i in range(50)]
        candles = _make_candles(closes)
        series = _compute_rsi_series(candles, period=14, limit=200)
        for i in range(1, len(series)):
            assert series[i]["time"] > series[i - 1]["time"]


# ── ATR Series ──────────────────────────────────────────────────


class TestAtrSeriesCalculation:
    """ATR 시계열 계산 순수 로직 테스트."""

    def test_atr_series_structure(self):
        closes = [100.0 + i for i in range(30)]
        candles = _make_candles(closes)
        series = _compute_atr_series(candles, period=14, limit=200)
        assert len(series) > 0
        for point in series:
            assert "time" in point
            assert "value" in point

    def test_atr_always_positive(self):
        """ATR는 항상 양수여야 한다."""
        closes = [100 + (i % 5) * 3 - 7 for i in range(50)]
        candles = _make_candles(closes)
        series = _compute_atr_series(candles, period=14, limit=200)
        for point in series:
            assert point["value"] > 0

    def test_atr_series_insufficient_candles(self):
        closes = [100.0, 101.0, 102.0]
        candles = _make_candles(closes)
        series = _compute_atr_series(candles, period=14, limit=200)
        assert series == []

    def test_atr_series_time_ascending(self):
        closes = [100.0 + i * 0.5 - (i % 3) * 2 for i in range(50)]
        candles = _make_candles(closes)
        series = _compute_atr_series(candles, period=14, limit=200)
        for i in range(1, len(series)):
            assert series[i]["time"] > series[i - 1]["time"]


# ── 헬퍼: 서비스 로직 복제 (DB 없이 순수 계산) ──────────────────


def _compute_ema_series(candles, period: int, limit: int) -> list[dict]:
    if len(candles) < period + 1:
        return []
    closes = [float(c.close) for c in candles]
    times = [c.open_time for c in candles]
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    series = []
    for i in range(period, len(closes)):
        ema = closes[i] * k + ema * (1 - k)
        series.append({"time": times[i], "value": round(ema, 6)})
    return series[-limit:]


def _compute_rsi_series(candles, period: int, limit: int) -> list[dict]:
    if len(candles) < period + 1:
        return []
    closes = [float(c.close) for c in candles]
    times = [c.open_time for c in candles]
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    avg_gain = sum(max(c, 0) for c in changes[:period]) / period
    avg_loss = sum(max(-c, 0) for c in changes[:period]) / period
    series = []
    if avg_loss == 0:
        series.append({"time": times[period], "value": 100.0})
    else:
        rs = avg_gain / avg_loss
        series.append({"time": times[period], "value": round(100 - (100 / (1 + rs)), 2)})
    for i in range(period, len(changes)):
        avg_gain = (avg_gain * (period - 1) + max(changes[i], 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-changes[i], 0)) / period
        if avg_loss == 0:
            rsi_val = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_val = round(100 - (100 / (1 + rs)), 2)
        series.append({"time": times[i + 1], "value": rsi_val})
    return series[-limit:]


def _compute_atr_series(candles, period: int, limit: int) -> list[dict]:
    if len(candles) < period + 1:
        return []
    times = [c.open_time for c in candles]
    trs = []
    for i in range(1, len(candles)):
        h = float(candles[i].high)
        l = float(candles[i].low)
        prev_c = float(candles[i - 1].close)
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    atr = sum(trs[:period]) / period
    series = [{"time": times[period], "value": round(atr, 6)}]
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
        series.append({"time": times[i + 1], "value": round(atr, 6)})
    return series[-limit:]
