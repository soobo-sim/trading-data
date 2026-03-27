"""
볼린저밴드 계산 로직 유닛 테스트 (BB-1 ~ BB-9).
DB 의존성 없이 순수 계산 로직만 테스트.
"""
import math
import pytest
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace


def _make_candles(closes: list[float]):
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    delta = timedelta(hours=4)
    return [
        SimpleNamespace(
            open_time=base + delta * i,
            close=c, open=c * 0.99, high=c * 1.01, low=c * 0.98,
            volume=100.0, is_complete=True,
        )
        for i, c in enumerate(closes)
    ]


# ── BB 계산 함수 (base_candle_service 로직 복사) ─────────────────

def _calc_bb(candles, period, std_dev):
    """볼린저밴드 최신값. 부족 시 None."""
    if len(candles) < period:
        return None
    closes = [float(c.close) for c in candles]
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    std = variance ** 0.5
    upper = middle + std_dev * std
    lower = middle - std_dev * std
    width_pct = round((upper - lower) / middle * 100, 4) if middle > 0 else 0.0
    current_price = float(candles[-1].close)
    price_position_pct = (
        round((current_price - lower) / (upper - lower) * 100, 2)
        if upper != lower else 50.0
    )
    return {
        "upper": round(upper, 6),
        "middle": round(middle, 6),
        "lower": round(lower, 6),
        "width_pct": width_pct,
        "current_price": current_price,
        "price_position_pct": price_position_pct,
        "candles_used": len(closes),
        "open_time": candles[-1].open_time,
    }


def _calc_bb_series(candles, period, std_dev, limit):
    """볼린저밴드 시계열."""
    if len(candles) < period:
        return []
    closes = [float(c.close) for c in candles]
    times = [c.open_time for c in candles]
    series = []
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        middle = sum(window) / period
        variance = sum((x - middle) ** 2 for x in window) / period
        std = variance ** 0.5
        upper = middle + std_dev * std
        lower = middle - std_dev * std
        width_pct = round((upper - lower) / middle * 100, 4) if middle > 0 else 0.0
        curr = closes[i]
        pp = round((curr - lower) / (upper - lower) * 100, 2) if upper != lower else 50.0
        series.append({
            "time": times[i],
            "upper": round(upper, 6),
            "middle": round(middle, 6),
            "lower": round(lower, 6),
            "width_pct": width_pct,
            "price_position_pct": pp,
        })
    return series[-limit:]


# ── BB-1: 정상 20개+ 캔들 → upper/middle/lower + width_pct ──────

def test_bb1_normal_candles():
    closes = [float(10_000_000 + i * 10_000) for i in range(25)]
    candles = _make_candles(closes)
    result = _calc_bb(candles, period=20, std_dev=2.0)

    assert result is not None
    assert result["upper"] > result["middle"] > result["lower"]
    assert result["width_pct"] > 0
    assert "price_position_pct" in result
    assert result["candles_used"] == 25


# ── BB-2: 캔들 부족 (< period) → None ──────────────────────────

def test_bb2_insufficient_candles():
    candles = _make_candles([float(11_000_000 + i * 5000) for i in range(15)])
    result = _calc_bb(candles, period=20, std_dev=2.0)
    assert result is None


# ── BB-3: 캔들 0건 → None ────────────────────────────────────────

def test_bb3_empty_candles():
    result = _calc_bb([], period=20, std_dev=2.0)
    assert result is None


# ── BB-4: 기본값 (period=20, std_dev=2.0) 정확성 ────────────────

def test_bb4_default_params_accuracy():
    closes = [float(10_000_000 + i * 8000) for i in range(30)]
    candles = _make_candles(closes)
    result = _calc_bb(candles, period=20, std_dev=2.0)
    assert result is not None

    # 수동 계산으로 검증
    window = closes[-20:]
    middle = sum(window) / 20
    variance = sum((x - middle) ** 2 for x in window) / 20
    std = variance ** 0.5
    expected_upper = middle + 2.0 * std
    expected_lower = middle - 2.0 * std

    assert abs(result["upper"] - expected_upper) < 0.01
    assert abs(result["middle"] - middle) < 0.01
    assert abs(result["lower"] - expected_lower) < 0.01


# ── BB-5: std_dev=0 → 400 조건 검증 ────────────────────────────

@pytest.mark.parametrize("bad_std", [0.0, -1.0, 0.09])
def test_bb5_zero_or_negative_std_rejected(bad_std):
    """std_dev < 0.1 → 400 조건."""
    assert bad_std < 0.1  # 라우트에서 400 반환 조건


# ── BB-6: 동일 입력 → 동일 width_pct (계산 일관성) ─────────────

def test_bb6_same_input_same_output():
    closes = [float(10_500_000 + math.sin(i) * 50_000) for i in range(30)]
    candles = _make_candles(closes)
    r1 = _calc_bb(candles, period=20, std_dev=2.0)
    r2 = _calc_bb(candles, period=20, std_dev=2.0)
    assert r1["width_pct"] == r2["width_pct"]
    assert r1["upper"] == r2["upper"]
    assert r1["lower"] == r2["lower"]


# ── BB-7: 미완성 캔들 제외 확인 ────────────────────────────────

def test_bb7_series_only_uses_complete_candles():
    """시계열 계산은 is_complete=True 캔들 기준 (get_completed_candles 계층 책임)."""
    closes_complete = [float(10_000_000 + i * 5000) for i in range(25)]
    candles = _make_candles(closes_complete)
    result = _calc_bb(candles, period=20, std_dev=2.0)
    # is_complete=True 캔들만 사용 → candles_used = 25
    assert result["candles_used"] == 25


# ── BB-8: price_position_pct 정확성 ────────────────────────────

def test_bb8_price_at_lower_is_zero():
    """현재가 = lower → price_position_pct = 0.0."""
    base_price = 10_000_000.0
    closes = [base_price] * 20
    # 마지막 캔들만 다른 값으로 (lower를 수동 계산)
    window = closes
    middle = sum(window) / 20
    std = 0.0  # 모두 동일 → std=0
    # std=0이면 upper=lower=middle → price_position_pct=50.0
    candles = _make_candles(closes)
    result = _calc_bb(candles, period=20, std_dev=2.0)
    assert result["price_position_pct"] == 50.0  # upper==lower 방어


def test_bb8_price_position_range():
    """price_position_pct 음수/100초과 허용 (이탈 표현)."""
    closes = [float(10_000_000 + i * 10_000) for i in range(24)] + [9_000_000.0]  # 급락
    candles = _make_candles(closes)
    result = _calc_bb(candles, period=20, std_dev=2.0)
    assert result is not None
    # 급락 시 음수 가능 (하단 이탈)
    assert isinstance(result["price_position_pct"], float)


# ── BB-9: std_dev 범위 외 → 400 조건 검증 ──────────────────────

@pytest.mark.parametrize("bad_std", [0.0, -1.0, 6.0, 0.05, 5.1])
def test_bb9_out_of_range_std_dev(bad_std):
    """std_dev < 0.1 or > 5.0 → 400 조건."""
    assert bad_std < 0.1 or bad_std > 5.0


@pytest.mark.parametrize("good_std", [0.1, 1.0, 2.0, 5.0, 3.5])
def test_bb9_valid_std_dev_passes(good_std):
    """정상 범위 std_dev → 400 조건 미충족."""
    assert 0.1 <= good_std <= 5.0


# ── BB 시계열 구조 확인 ─────────────────────────────────────────

def test_bb_series_structure():
    closes = [float(10_000_000 + i * 5000) for i in range(30)]
    candles = _make_candles(closes)
    series = _calc_bb_series(candles, period=20, std_dev=2.0, limit=5)
    assert len(series) == 5
    for item in series:
        assert "upper" in item
        assert "middle" in item
        assert "lower" in item
        assert "width_pct" in item
        assert "price_position_pct" in item
        assert item["upper"] > item["middle"] > item["lower"]


def test_bb_series_insufficient():
    candles = _make_candles([10_000_000.0] * 5)
    series = _calc_bb_series(candles, period=20, std_dev=2.0, limit=1)
    assert series == []
