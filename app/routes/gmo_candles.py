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
    limit: int = Query(default=50, ge=1, le=500),
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


@router.get("/{pair}/{timeframe}/bb", summary="GMO FX 볼린저밴드 값")
@handle_api_errors("GMO BB 조회")
async def get_gmo_bb(
    pair: str,
    timeframe: str,
    period: int = Query(default=20, ge=1, description="BB 기간 (기본 20)"),
    std_dev: float = Query(default=2.0, description="표준편차 배수 (0.1~5.0)"),
    limit: int = Query(default=1, ge=1, le=200, description="시계열 데이터 수"),
):
    """GMO FX 볼린저밴드 upper/middle/lower + width_pct + price_position_pct 반환."""
    from sqlalchemy import desc as _desc, select as _select
    from app.database import AsyncSessionLocal
    from app.models.database import GmoCandle

    if std_dev < 0.1 or std_dev > 5.0:
        raise HTTPException(400, f"std_dev 범위 초과 (0.1~5.0): {std_dev}")

    p = pair.upper()
    if p not in _VALID_PAIRS:
        raise HTTPException(400, f"지원하지 않는 pair: {p}")
    if timeframe not in _VALID_TIMEFRAMES:
        raise HTTPException(400, f"지원하지 않는 timeframe: {timeframe}")

    fetch_limit = limit + period
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            _select(GmoCandle)
            .where(
                GmoCandle.pair == p.lower(),
                GmoCandle.timeframe == timeframe,
                GmoCandle.is_complete.is_(True),
            )
            .order_by(_desc(GmoCandle.open_time))
            .limit(fetch_limit)
        )
        candles = list(reversed(result.scalars().all()))

    if len(candles) < period:
        raise HTTPException(400, f"캔들 부족: pair={p}, timeframe={timeframe}, 최소 {period}개 필요")

    def _to_jst_gmo(dt):
        JST = timezone(timedelta(hours=9))
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST)

    def _calc_bb(window_closes, sd):
        middle = sum(window_closes) / len(window_closes)
        variance = sum((x - middle) ** 2 for x in window_closes) / len(window_closes)
        std = variance ** 0.5
        upper = middle + sd * std
        lower = middle - sd * std
        width_pct = round((upper - lower) / middle * 100, 4) if middle > 0 else 0.0
        return round(upper, 6), round(middle, 6), round(lower, 6), width_pct

    closes = [float(c.close) for c in candles]
    times = [c.open_time for c in candles]

    # latest
    window = closes[-period:]
    upper, middle, lower, width_pct = _calc_bb(window, std_dev)
    current_price = closes[-1]
    price_position_pct = (
        round((current_price - lower) / (upper - lower) * 100, 2)
        if upper != lower else 50.0
    )

    # series
    series = []
    for i in range(period - 1, len(closes)):
        w = closes[i - period + 1: i + 1]
        u, m, lo, wp = _calc_bb(w, std_dev)
        curr = closes[i]
        pp = round((curr - lo) / (u - lo) * 100, 2) if u != lo else 50.0
        series.append({
            "time": _to_jst_gmo(times[i]).isoformat(),
            "upper": u, "middle": m, "lower": lo,
            "width_pct": wp, "price_position_pct": pp,
        })

    values = series[-limit:]
    latest_time = _to_jst_gmo(times[-1]).isoformat() if times else None

    return {
        "success": True,
        "pair": p,
        "timeframe": timeframe,
        "period": period,
        "std_dev": std_dev,
        "current_price": current_price,
        "latest": {
            "time": latest_time,
            "upper": upper, "middle": middle, "lower": lower,
            "width_pct": width_pct,
            "price_position_pct": price_position_pct,
        },
        "values": values,
        "candles_used": len(closes),
    }
