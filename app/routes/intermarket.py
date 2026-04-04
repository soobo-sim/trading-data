"""
FRED 매크로 지표 API 라우트 (F-04 알파 팩터).

trading-engine이 이 API를 조회하여 방향 바이어스 + 스트레스 판단에 활용.

GET /api/intermarket/latest          — 전체 series 최신값 반환
GET /api/intermarket/series/{id}     — 특정 series 최근 N일 시계열
GET /api/intermarket/status          — 수집 서비스 상태
"""
from __future__ import annotations

import logging
from fastapi import APIRouter, Path, Query

from app.core.error_handlers import handle_api_errors
from app.core.exceptions import MarketDataAPIError
from app.services.fred_service import FRED_SERIES, get_fred_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/intermarket", tags=["Intermarket"])

_VALID_SERIES = set(FRED_SERIES)


@router.get("/latest")
@handle_api_errors("FRED 최신값 조회")
async def get_latest_values():
    """
    전체 series 최신관측값 반환.

    trading-engine의 IntermarketClient가 캐시(TTL 1시간)로 조회.
    결측 series는 null로 포함.
    """
    svc = get_fred_service()
    result = {}
    for series_id in FRED_SERIES:
        result[series_id] = await svc.get_latest(series_id)
    return {"series": result}


@router.get("/series/{series_id}")
@handle_api_errors("FRED 시계열 조회")
async def get_series(
    series_id: str = Path(..., description="FRED series ID (예: DGS10, VIXCLS)"),
    days: int = Query(default=10, ge=1, le=365, description="최근 N일"),
):
    """특정 FRED series의 최근 N일 시계열 반환."""
    # 경로 파라미터 검증
    series_id = series_id.upper()
    if series_id not in _VALID_SERIES:
        raise MarketDataAPIError(
            detail=f"유효하지 않은 series_id. 허용: {sorted(_VALID_SERIES)}",
            status_code=400,
            operation="FRED 시계열 조회",
        )

    svc = get_fred_service()
    rows = await svc.get_recent(series_id, days=days)
    return {
        "series_id": series_id,
        "days": days,
        "count": len(rows),
        "data": [{"date": str(d), "value": v} for d, v in rows],
    }


@router.get("/status")
@handle_api_errors("FRED 수집 상태 조회")
async def get_fred_status():
    """FRED 수집 서비스 상태."""
    svc = get_fred_service()
    return svc.get_status()
