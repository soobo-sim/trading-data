"""
GMO Coin Candles API 라우트 — OHLCV 캔들 데이터

GET /api/gmo-coin/candles/{pair}/status       — 파이프라인 가동 상태
GET /api/gmo-coin/candles/{pair}/{timeframe}  — 완성된 캔들 목록 (limit 1–500)
"""
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.error_handlers import handle_api_errors

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/gmo-coin/candles", tags=["GMO Coin Candles"])

# GMO 코인에서 레버리지 거래 가능한 주요 페어 (소문자 — 라젠카 내부 표준)
_VALID_PAIRS = {
    "btc_jpy", "eth_jpy", "bch_jpy", "ltc_jpy", "xrp_jpy",
    "xem_jpy", "xlm_jpy", "bat_jpy", "omg_jpy", "qtum_jpy",
    "ens_jpy", "mkr_jpy", "dot_jpy", "atom_jpy", "ada_jpy",
    "link_jpy", "doge_jpy", "sol_jpy", "matic_jpy", "avax_jpy",
    "apt_jpy", "flr_jpy", "sui_jpy",
}
_VALID_TIMEFRAMES = {"1h", "4h"}


class GmocCandleResponse(BaseModel):
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


@router.get("/{pair}/status", summary="GMO 코인 캔들 파이프라인 가동 상태")
@handle_api_errors("GMO 코인 캔들 파이프라인 상태 조회")
async def get_pipeline_status(pair: str):
    from app.services.gmo_coin_candle_pipeline import get_gmo_coin_candle_pipeline

    p = pair.strip().lower()
    if p not in _VALID_PAIRS:
        raise HTTPException(400, {"blocked_code": "INVALID_PAIR"})
    pipeline = get_gmo_coin_candle_pipeline()
    return {
        "success": True,
        "pair": p,
        "is_running": pipeline.is_running(p),
        "running_pairs": pipeline.running_pairs(),
    }


@router.get("/{pair}/{timeframe}", summary="GMO 코인 완성된 캔들 목록")
@handle_api_errors("GMO 코인 캔들 조회")
async def get_candles(
    pair: str,
    timeframe: str,
    limit: int = Query(default=50, ge=1, le=500),
):
    from sqlalchemy import select, desc
    from app.database import AsyncSessionLocal
    from app.models.database import GmocCandle

    p = pair.strip().lower()
    _validate_pair_tf(p, timeframe)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(GmocCandle)
            .where(
                GmocCandle.pair == p,
                GmocCandle.timeframe == timeframe,
                GmocCandle.is_complete.is_(True),
            )
            .order_by(desc(GmocCandle.open_time))
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
            GmocCandleResponse(
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
