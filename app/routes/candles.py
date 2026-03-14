"""
Candles API 라우트 — Coincheck OHLCV 캔들 데이터

GET /api/ck/candles/{pair}/status              — 파이프라인 가동 상태 + 최신 캔들 시각
GET /api/ck/candles/{pair}/{timeframe}          — 완성된 캔들 목록 (limit 1–200)
GET /api/ck/candles/{pair}/{timeframe}/current  — 진행 중 캔들
GET /api/ck/candles/{pair}/{timeframe}/rsi      — RSI 계산
GET /api/ck/candles/{pair}/{timeframe}/ema      — EMA 값 + 기울기 (추세추종 진입 판단)
GET /api/ck/candles/{pair}/{timeframe}/atr      — ATR 값 (손절폭/변동성 측정)
"""
import logging
from typing import Optional
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.error_handlers import handle_api_errors
from app.services.candle_service import get_candle_service
from app.services.candle_pipeline import get_candle_pipeline

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ck/candles", tags=["Candles"])

_JST = timezone(timedelta(hours=9))


def _to_jst(dt: datetime) -> datetime:
    """naive UTC datetime → JST(+09:00) aware datetime"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_JST)

# NOTE: /{pair}/status 는 /{pair}/{timeframe}보다 반드시 먼저 등록

_VALID_PAIRS = {
    "btc_jpy", "eth_jpy", "xrp_jpy", "etc_jpy", "lsk_jpy", "xem_jpy",
    "bch_jpy", "mona_jpy", "iost_jpy", "enj_jpy", "chz_jpy", "imx_jpy",
    "shib_jpy", "avax_jpy", "fnct_jpy", "dai_jpy", "wbtc_jpy", "doge_jpy",
    "sol_jpy",
}
_VALID_TIMEFRAMES = {"1h", "4h"}


class CandleResponse(BaseModel):
    pair: str
    timeframe: str
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    tick_count: int
    is_complete: bool


def _validate_pair_tf(pair: str, timeframe: str):
    if pair not in _VALID_PAIRS:
        raise HTTPException(400, {"blocked_code": "INVALID_PAIR", "message": f"지원하지 않는 페어: {pair}"})
    if timeframe not in _VALID_TIMEFRAMES:
        raise HTTPException(400, {"blocked_code": "INVALID_TIMEFRAME", "message": f"지원하지 않는 타임프레임: {timeframe} (1h | 4h)"})


@router.get("/{pair}/status", summary="캔들 파이프라인 가동 상태")
@handle_api_errors("캔들 파이프라인 상태 조회")
async def get_pipeline_status(pair: str):
    """캔들 집계 파이프라인 가동 여부 + 최신 완성 캔들 시각 + RSI"""
    if pair not in _VALID_PAIRS:
        raise HTTPException(400, {"blocked_code": "INVALID_PAIR"})
    pipeline = get_candle_pipeline()
    candle_svc = get_candle_service()
    latest_4h = await candle_svc.get_latest_complete_open_time(pair, "4h")
    latest_1h = await candle_svc.get_latest_complete_open_time(pair, "1h")
    return {
        "success": True,
        "pair": pair,
        "is_running": pipeline.is_running(pair),
        "running_pairs": pipeline.running_pairs(),
        "latest_4h_candle": _to_jst(latest_4h) if latest_4h else None,
        "latest_1h_candle": _to_jst(latest_1h) if latest_1h else None,
        "rsi_1h": await candle_svc.get_rsi(pair, "1h"),
        "rsi_4h": await candle_svc.get_rsi(pair, "4h"),
    }


@router.get("/{pair}/{timeframe}", summary="완성된 캔들 목록")
@handle_api_errors("캔들 조회")
async def get_candles(
    pair: str,
    timeframe: str,
    limit: int = Query(default=30, ge=1, le=200),
):
    _validate_pair_tf(pair, timeframe)
    candles = await get_candle_service().get_completed_candles(pair, timeframe, limit=limit)
    return {
        "success": True,
        "pair": pair,
        "timeframe": timeframe,
        "count": len(candles),
        "candles": [
            CandleResponse(
                pair=c.pair, timeframe=c.timeframe,
                open_time=_to_jst(c.open_time), close_time=_to_jst(c.close_time),
                open=float(c.open), high=float(c.high), low=float(c.low), close=float(c.close),
                volume=float(c.volume), tick_count=c.tick_count, is_complete=c.is_complete,
            )
            for c in candles
        ],
    }


@router.get("/{pair}/{timeframe}/current", summary="진행 중인 캔들")
@handle_api_errors("현재 캔들 조회")
async def get_current_candle(pair: str, timeframe: str):
    _validate_pair_tf(pair, timeframe)
    candle = await get_candle_service().get_current_candle(pair, timeframe)
    if not candle:
        return {"success": True, "pair": pair, "timeframe": timeframe, "current_candle": None}
    return {
        "success": True, "pair": pair, "timeframe": timeframe,
        "current_candle": CandleResponse(
            pair=candle.pair, timeframe=candle.timeframe,
            open_time=_to_jst(candle.open_time), close_time=_to_jst(candle.close_time),
            open=float(candle.open), high=float(candle.high), low=float(candle.low), close=float(candle.close),
            volume=float(candle.volume), tick_count=candle.tick_count, is_complete=candle.is_complete,
        ),
    }


@router.get("/{pair}/{timeframe}/rsi", summary="RSI 계산")
@handle_api_errors("RSI 조회")
async def get_rsi(
    pair: str,
    timeframe: str,
    period: int = Query(default=14, ge=5, le=50),
):
    _validate_pair_tf(pair, timeframe)
    rsi = await get_candle_service().get_rsi(pair, timeframe, period=period)
    return {
        "success": True, "pair": pair, "timeframe": timeframe,
        "period": period, "rsi": rsi,
        "note": None if rsi is not None else f"캔들 데이터 부족 (최소 {period+1}개 필요)",
    }


@router.get("/{pair}/{timeframe}/ema", summary="EMA 값 + 기울기")
@handle_api_errors("EMA 조회")
async def get_ema(
    pair: str,
    timeframe: str,
    period: int = Query(default=20, ge=5, le=200, description="EMA 기간 (기본 20)"),
):
    """
    EMA(period) 와 기울기(slope_pct) 반환.

    - ema: 최신 기간의 지수이동평균 (JPY)
    - slope_pct: 직전 캔들 EMA 대비 변화율 (%) — 양수=상승 기울기
    - price_vs_ema_pct: 현재가 와 EMA 의 괴리율 (%) — 양수=EMA 위
    - signal: above_rising | above_falling | below_rising | below_falling

    추세추종 진입 조건: signal = above_rising + RSI 40~65
    """
    _validate_pair_tf(pair, timeframe)
    svc = get_candle_service()
    result = await svc.get_ema(pair, timeframe, period=period)
    if result is None:
        return {
            "success": True, "pair": pair, "timeframe": timeframe,
            "period": period, "ema": None, "slope_pct": None,
            "price_vs_ema_pct": None, "signal": None,
            "note": f"캔들 데이터 부족 (최소 {period+1}개 필요)",
        }

    # 현재가 vs EMA
    candles = await svc.get_completed_candles(pair, timeframe, limit=1)
    current_price = float(candles[0].close) if candles else None
    price_vs_ema_pct = None
    signal = None
    if current_price and result["ema"] > 0:
        price_vs_ema_pct = round((current_price - result["ema"]) / result["ema"] * 100, 4)
        above = price_vs_ema_pct >= 0
        rising = result["slope_pct"] >= 0
        signal = ("above" if above else "below") + "_" + ("rising" if rising else "falling")

    return {
        "success": True,
        "pair": pair,
        "timeframe": timeframe,
        "period": period,
        "ema": result["ema"],
        "slope_pct": result["slope_pct"],
        "price_vs_ema_pct": price_vs_ema_pct,
        "signal": signal,
        "current_price": current_price,
        "candles_used": result["candles_used"],
        "note": None,
    }


@router.get("/{pair}/{timeframe}/atr", summary="ATR 값 (손절폭/변동성)")
@handle_api_errors("ATR 조회")
async def get_atr(
    pair: str,
    timeframe: str,
    period: int = Query(default=14, ge=5, le=100, description="ATR 기간 (기본 14)"),
    stop_multiplier: float = Query(default=2.0, ge=0.5, le=5.0, description="손절폭 배수 (기본 2.0)"),
):
    """
    ATR(period) 반환.

    - atr: ATR 절대값 (JPY)
    - atr_pct: ATR ÷ 현재가 × 100 (%)
    - stop_loss_distance: ATR × stop_multiplier — 권장 손절폭 (JPY)
    - trailing_stop_distance: ATR × 1.5 — 트레일링 스탑 거리 (JPY)

    추세추종 손절 기준: 진입가 - stop_loss_distance
    """
    _validate_pair_tf(pair, timeframe)
    svc = get_candle_service()
    result = await svc.get_atr(pair, timeframe, period=period)
    if result is None:
        return {
            "success": True, "pair": pair, "timeframe": timeframe,
            "period": period, "atr": None, "atr_pct": None,
            "stop_loss_distance": None, "trailing_stop_distance": None,
            "note": f"캔들 데이터 부족 (최소 {period+1}개 필요)",
        }

    return {
        "success": True,
        "pair": pair,
        "timeframe": timeframe,
        "period": period,
        "atr": result["atr"],
        "atr_pct": result["atr_pct"],
        "stop_loss_distance": round(result["atr"] * stop_multiplier, 6),
        "trailing_stop_distance": round(result["atr"] * 1.5, 6),
        "stop_multiplier": stop_multiplier,
        "candles_used": result["candles_used"],
        "note": None,
    }
