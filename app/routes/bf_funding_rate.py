"""
BitFlyer Funding Rate API 라우트 (trading-data 서비스)

GET /api/bf/funding-rate          — FX_BTC_JPY 최신 펀딩레이트
GET /api/bf/funding-rate/history  — 수집 이력 (limit 1–200)
"""
import logging
from fastapi import APIRouter, Query

from app.core.error_handlers import handle_api_errors
from app.services.bf_funding_rate_service import get_bf_funding_rate_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/bf/funding-rate", tags=["BitFlyer Funding Rate"])


# NOTE: /history 는 고정 경로이므로 /{id} 앞에 등록


@router.get("/history")
@handle_api_errors("BF 펀딩레이트 이력 조회")
async def get_funding_rate_history(
    product_code: str = Query("FX_BTC_JPY"),
    limit: int = Query(96, ge=1, le=200),
):
    """수집 이력 반환 (최신순). 기본 96건 = 24시간분 (15분 주기)."""
    rows = await get_bf_funding_rate_service().get_history(product_code=product_code, limit=limit)
    return {"product_code": product_code, "count": len(rows), "data": rows}


@router.get("")
@handle_api_errors("BF 펀딩레이트 최신값 조회")
async def get_funding_rate(
    product_code: str = Query("FX_BTC_JPY"),
):
    """최신 펀딩레이트 1건 반환. 아직 수집 이력이 없으면 null."""
    data = await get_bf_funding_rate_service().get_latest(product_code=product_code)
    return {"product_code": product_code, "data": data}
