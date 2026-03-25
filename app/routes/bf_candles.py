"""
BitFlyer Candles API 라우트 — OHLCV 캔들 데이터

GET /api/bf/candles/{product_code}/status              — 파이프라인 가동 상태 + 최신 캔들 시각
GET /api/bf/candles/{product_code}/{timeframe}          — 완성된 캔들 목록 (limit 1–200)
GET /api/bf/candles/{product_code}/{timeframe}/current  — 진행 중 캔들
GET /api/bf/candles/{product_code}/{timeframe}/rsi      — RSI 계산
GET /api/bf/candles/{product_code}/{timeframe}/ema      — EMA 값 + 기울기 (추세추종 진입 판단)
GET /api/bf/candles/{product_code}/{timeframe}/atr      — ATR 값 (손절폭/변동성 측정)
"""
import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.error_handlers import handle_api_errors
from app.services.bf_candle_service import get_bf_candle_service
from app.services.bf_candle_pipeline import get_bf_candle_pipeline

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/bf/candles", tags=["BitFlyer Candles"])

_JST = timezone(timedelta(hours=9))


def _to_jst(dt: datetime) -> datetime:
    """naive UTC datetime → JST(+09:00) aware datetime"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_JST)


# NOTE: /{product_code}/status は /{product_code}/{timeframe} より前に登録すること

_VALID_PRODUCTS = {"BTC_JPY", "ETH_JPY", "XRP_JPY", "BCH_JPY", "LTC_JPY", "FX_BTC_JPY"}
_VALID_TIMEFRAMES = {"1h", "4h"}


class BfCandleResponse(BaseModel):
    product_code: str
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


def _validate_product_tf(product_code: str, timeframe: str):
    if product_code not in _VALID_PRODUCTS:
        raise HTTPException(
            400,
            {"blocked_code": "INVALID_PRODUCT", "message": f"지원하지 않는 product: {product_code}"},
        )
    if timeframe not in _VALID_TIMEFRAMES:
        raise HTTPException(
            400,
            {"blocked_code": "INVALID_TIMEFRAME", "message": f"지원하지 않는 타임프레임: {timeframe} (1h | 4h)"},
        )


@router.get("/{product_code}/status", summary="BF 캔들 파이프라인 가동 상태")
@handle_api_errors("BF 캔들 파이프라인 상태 조회")
async def get_pipeline_status(product_code: str):
    pc = product_code.upper()
    if pc not in _VALID_PRODUCTS:
        raise HTTPException(400, {"blocked_code": "INVALID_PRODUCT"})
    pipeline = get_bf_candle_pipeline()
    candle_svc = get_bf_candle_service()
    latest_4h = await candle_svc.get_latest_complete_open_time(pc, "4h")
    latest_1h = await candle_svc.get_latest_complete_open_time(pc, "1h")
    return {
        "success": True,
        "product_code": pc,
        "is_running": pipeline.is_running(pc),
        "running_products": pipeline.running_products(),
        "latest_4h_candle": _to_jst(latest_4h) if latest_4h else None,
        "latest_1h_candle": _to_jst(latest_1h) if latest_1h else None,
        "rsi_1h": await candle_svc.get_rsi(pc, "1h"),
        "rsi_4h": await candle_svc.get_rsi(pc, "4h"),
    }


@router.get("/{product_code}/{timeframe}", summary="완성된 BF 캔들 목록")
@handle_api_errors("BF 캔들 조회")
async def get_candles(
    product_code: str,
    timeframe: str,
    limit: int = Query(default=30, ge=1, le=500),
):
    pc = product_code.upper()
    _validate_product_tf(pc, timeframe)
    candles = await get_bf_candle_service().get_completed_candles(pc, timeframe, limit=limit)
    return {
        "success": True,
        "product_code": pc,
        "timeframe": timeframe,
        "count": len(candles),
        "candles": [
            BfCandleResponse(
                product_code=c.product_code, timeframe=c.timeframe,
                open_time=_to_jst(c.open_time), close_time=_to_jst(c.close_time),
                open=float(c.open), high=float(c.high), low=float(c.low), close=float(c.close),
                volume=float(c.volume), tick_count=c.tick_count, is_complete=c.is_complete,
            )
            for c in candles
        ],
    }


@router.get("/{product_code}/{timeframe}/current", summary="진행 중인 BF 캔들")
@handle_api_errors("BF 현재 캔들 조회")
async def get_current_candle(product_code: str, timeframe: str):
    pc = product_code.upper()
    _validate_product_tf(pc, timeframe)
    candle = await get_bf_candle_service().get_current_candle(pc, timeframe)
    if not candle:
        return {"success": True, "product_code": pc, "timeframe": timeframe, "current_candle": None}
    return {
        "success": True, "product_code": pc, "timeframe": timeframe,
        "current_candle": BfCandleResponse(
            product_code=candle.product_code, timeframe=candle.timeframe,
            open_time=_to_jst(candle.open_time), close_time=_to_jst(candle.close_time),
            open=float(candle.open), high=float(candle.high), low=float(candle.low),
            close=float(candle.close), volume=float(candle.volume),
            tick_count=candle.tick_count, is_complete=candle.is_complete,
        ),
    }


@router.get("/{product_code}/{timeframe}/rsi", summary="BF RSI 계산")
@handle_api_errors("BF RSI 조회")
async def get_rsi(
    product_code: str,
    timeframe: str,
    period: int = Query(default=14, ge=5, le=50),
    limit: int = Query(default=200, ge=1, le=500, description="시계열 데이터 수"),
):
    pc = product_code.upper()
    _validate_product_tf(pc, timeframe)
    svc = get_bf_candle_service()
    rsi = await svc.get_rsi(pc, timeframe, period=period)
    series = await svc.get_rsi_series(pc, timeframe, period=period, limit=limit)
    values = [{"time": _to_jst(v["time"]).isoformat(), "value": v["value"]} for v in series]
    return {
        "success": True, "product_code": pc, "timeframe": timeframe,
        "period": period, "rsi": rsi,
        "values": values,
        "note": None if rsi is not None else f"캔들 데이터 부족 (최소 {period + 1}개 필요)",
    }


@router.get("/{product_code}/{timeframe}/ema", summary="BF EMA 값 + 기울기")
@handle_api_errors("BF EMA 조회")
async def get_ema(
    product_code: str,
    timeframe: str,
    period: int = Query(default=20, ge=5, le=200, description="EMA 기간 (기본 20)"),
    limit: int = Query(default=200, ge=1, le=500, description="시계열 데이터 수"),
):
    """
    EMA(period) 와 기울기(slope_pct) 반환.

    - ema: 최신 기간의 지수이동평균 (JPY)
    - slope_pct: 직전 캔들 EMA 대비 변화율 (%) — 양수=상승 기울기
    - price_vs_ema_pct: 현재가 와 EMA 의 괴리율 (%) — 양수=EMA 위
    - signal: above_rising | above_falling | below_rising | below_falling

    추세추종 진입 조건: signal = above_rising + RSI 40~65
    """
    pc = product_code.upper()
    _validate_product_tf(pc, timeframe)
    svc = get_bf_candle_service()
    result = await svc.get_ema(pc, timeframe, period=period)
    if result is None:
        return {
            "success": True, "product_code": pc, "timeframe": timeframe,
            "period": period, "ema": None, "slope_pct": None,
            "price_vs_ema_pct": None, "signal": None,
            "values": [],
            "note": f"캔들 데이터 부족 (최소 {period + 1}개 필요)",
        }
    candles = await svc.get_completed_candles(pc, timeframe, limit=1)
    current_price = float(candles[0].close) if candles else None
    price_vs_ema_pct = None
    signal = None
    if current_price and result["ema"] > 0:
        price_vs_ema_pct = round((current_price - result["ema"]) / result["ema"] * 100, 4)
        above = price_vs_ema_pct >= 0
        rising = result["slope_pct"] >= 0
        signal = ("above" if above else "below") + "_" + ("rising" if rising else "falling")
    series = await svc.get_ema_series(pc, timeframe, period=period, limit=limit)
    values = [{"time": _to_jst(v["time"]).isoformat(), "value": v["value"]} for v in series]
    return {
        "success": True,
        "product_code": pc,
        "timeframe": timeframe,
        "period": period,
        "ema": result["ema"],
        "slope_pct": result["slope_pct"],
        "price_vs_ema_pct": price_vs_ema_pct,
        "signal": signal,
        "current_price": current_price,
        "candles_used": result["candles_used"],
        "values": values,
        "note": None,
    }


@router.get("/{product_code}/{timeframe}/atr", summary="BF ATR 값 (손절폭/변동성)")
@handle_api_errors("BF ATR 조회")
async def get_atr(
    product_code: str,
    timeframe: str,
    period: int = Query(default=14, ge=5, le=100, description="ATR 기간 (기본 14)"),
    stop_multiplier: float = Query(default=2.0, ge=0.5, le=5.0, description="손절폭 배수 (기본 2.0)"),
    limit: int = Query(default=200, ge=1, le=500, description="시계열 데이터 수"),
):
    """
    ATR(period) 반환.

    - atr: ATR 절대값 (JPY)
    - atr_pct: ATR ÷ 현재가 × 100 (%)
    - stop_loss_distance: ATR × stop_multiplier — 권장 손절폭 (JPY)
    - trailing_stop_distance: ATR × 1.5 — 트레일링 스탑 거리 (JPY)

    추세추종 손절 기준: 진입가 - stop_loss_distance
    """
    pc = product_code.upper()
    _validate_product_tf(pc, timeframe)
    svc = get_bf_candle_service()
    result = await svc.get_atr(pc, timeframe, period=period)
    if result is None:
        return {
            "success": True, "product_code": pc, "timeframe": timeframe,
            "period": period, "atr": None, "atr_pct": None,
            "stop_loss_distance": None, "trailing_stop_distance": None,
            "values": [],
            "note": f"캔들 데이터 부족 (최소 {period + 1}개 필요)",
        }

    series = await svc.get_atr_series(pc, timeframe, period=period, limit=limit)
    values = [{"time": _to_jst(v["time"]).isoformat(), "value": v["value"]} for v in series]

    return {
        "success": True,
        "product_code": pc,
        "timeframe": timeframe,
        "period": period,
        "atr": result["atr"],
        "atr_pct": result["atr_pct"],
        "stop_loss_distance": round(result["atr"] * stop_multiplier, 6),
        "trailing_stop_distance": round(result["atr"] * 1.5, 6),
        "stop_multiplier": stop_multiplier,
        "candles_used": result["candles_used"],
        "values": values,
        "note": None,
    }
