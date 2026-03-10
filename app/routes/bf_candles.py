"""
BitFlyer Candles API 라우트 — OHLCV 캔들 데이터

GET /api/bf/candles/{product_code}/status              — 파이프라인 가동 상태 + 최신 캔들 시각
GET /api/bf/candles/{product_code}/{timeframe}          — 완성된 캔들 목록 (limit 1–200)
GET /api/bf/candles/{product_code}/{timeframe}/current  — 진행 중 캔들
GET /api/bf/candles/{product_code}/{timeframe}/rsi      — RSI 계산
"""
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.error_handlers import handle_api_errors
from app.services.bf_candle_service import get_bf_candle_service
from app.services.bf_candle_pipeline import get_bf_candle_pipeline

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/bf/candles", tags=["BitFlyer Candles"])

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
        "latest_4h_candle": latest_4h,
        "latest_1h_candle": latest_1h,
        "rsi_1h": await candle_svc.get_rsi(pc, "1h"),
        "rsi_4h": await candle_svc.get_rsi(pc, "4h"),
    }


@router.get("/{product_code}/{timeframe}", summary="완성된 BF 캔들 목록")
@handle_api_errors("BF 캔들 조회")
async def get_candles(
    product_code: str,
    timeframe: str,
    limit: int = Query(default=30, ge=1, le=200),
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
                open_time=c.open_time, close_time=c.close_time,
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
            open_time=candle.open_time, close_time=candle.close_time,
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
):
    pc = product_code.upper()
    _validate_product_tf(pc, timeframe)
    rsi = await get_bf_candle_service().get_rsi(pc, timeframe, period=period)
    return {
        "success": True, "product_code": pc, "timeframe": timeframe,
        "period": period, "rsi": rsi,
        "note": None if rsi is not None else f"캔들 데이터 부족 (최소 {period + 1}개 필요)",
    }
