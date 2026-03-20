"""
GMO FX Candles API 라우트 — OHLCV 캔들 데이터

GET /api/gmo/candles/{pair}/status       — 파이프라인 가동 상태
GET /api/gmo/candles/{pair}/{timeframe}  — 완성된 캔들 목록 (limit 1–200)
"""
import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.error_handlers import handle_api_errors

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gmo/candles", tags=["GMO FX Candles"])

_VALID_PAIRS = {"USD_JPY", "EUR_JPY", "GBP_JPY", "AUD_JPY", "NZD_JPY", "CAD_JPY", "CHF_JPY", "EUR_USD", "GBP_USD"}
_VALID_TIMEFRAMES = {"1h", "4h"}


class GmoCandleResponse(BaseModel):
    pair: str
    timeframe: str
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_complete: bool


def _validate_pair_tf(pair: str, timeframe: str):
    if pair not in _VALID_PAIRS:
        raise HTTPException(
            400,
            {"blocked_code": "INVALID_PAIR", "message": f"지원하지 않는 pair: {pair}"},
        )
    if timeframe not in _VALID_TIMEFRAMES:
        raise HTTPException(
            400,
            {"blocked_code": "INVALID_TIMEFRAME", "message": f"지원하지 않는 타임프레임: {timeframe}"},
        )


@router.get("/{pair}/status", summary="GMO FX 캔들 파이프라인 가동 상태")
@handle_api_errors("GMO FX 캔들 파이프라인 상태 조회")
async def get_pipeline_status(pair: str):
    from app.services.gmo_candle_pipeline import get_gmo_candle_pipeline
    p = pair.upper()
    if p not in _VALID_PAIRS:
        raise HTTPException(400, {"blocked_code": "INVALID_PAIR"})
    pipeline = get_gmo_candle_pipeline()
    return {
        "success": True,
        "pair": p,
        "is_running": pipeline.is_running(p.lower()),
        "running_pairs": pipeline.running_pairs(),
    }


@router.get("/{pair}/{timeframe}", summary="GMO FX 완성된 캔들 목록")
@handle_api_errors("GMO FX 캔들 조회")
async def get_candles(
    pair: str,
    timeframe: str,
    limit: int = Query(default=50, ge=1, le=200),
):
    from sqlalchemy import select, desc
    from app.database import AsyncSessionLocal
    from app.models.database import GmoCandle

    p = pair.upper()
    _validate_pair_tf(p, timeframe)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(GmoCandle)
            .where(
                GmoCandle.pair == p.lower(),
                GmoCandle.timeframe == timeframe,
                GmoCandle.is_complete.is_(True),
            )
            .order_by(desc(GmoCandle.open_time))
            .limit(limit)
        )
        result = await session.execute(stmt)
        candles = result.scalars().all()

    return {
        "success": True,
        "pair": p,
        "timeframe": timeframe,
        "count": len(candles),
        "candles": [
            GmoCandleResponse(
                pair=p,
                timeframe=timeframe,
                open_time=c.open_time,
                close_time=c.close_time,
                open=float(c.open),
                high=float(c.high),
                low=float(c.low),
                close=float(c.close),
                volume=float(c.volume),
                is_complete=c.is_complete,
            ).model_dump()
            for c in reversed(candles)
        ],
    }
