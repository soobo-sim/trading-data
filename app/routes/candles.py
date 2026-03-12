"""
Candles API 라우트 — Coincheck OHLCV 캔들 데이터

GET /api/ck/candles/{pair}/status              — 파이프라인 가동 상태 + 최신 캔들 시각
GET /api/ck/candles/{pair}/{timeframe}          — 완성된 캔들 목록 (limit 1–200)
GET /api/ck/candles/{pair}/{timeframe}/current  — 진행 중 캔들
GET /api/ck/candles/{pair}/{timeframe}/rsi      — RSI 계산
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
