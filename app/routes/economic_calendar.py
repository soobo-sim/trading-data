"""
경제 캘린더 API 라우트 (F-01 알파 팩터).

trading-engine이 이 API를 조회하여 이벤트 전 진입 차단에 활용.

GET /api/economic-calendar/upcoming  — 향후 N시간 이내 이벤트 조회
GET /api/economic-calendar/status    — 수집 서비스 상태
"""
from __future__ import annotations

import logging
from fastapi import APIRouter, Query

from app.core.error_handlers import handle_api_errors
from app.services.economic_calendar import get_economic_calendar_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/economic-calendar", tags=["Economic Calendar"])

# 허용 통화 집합 (사용자 입력 검증용)
_VALID_COUNTRIES = {"USD", "JPY", "GBP", "EUR", "AUD", "CAD", "CHF", "NZD"}


@router.get("/upcoming")
@handle_api_errors("경제 이벤트 조회")
async def get_upcoming_events(
    hours: int = Query(default=24, ge=1, le=168, description="향후 N시간 이내 조회"),
    country: str = Query(default="", description="필터 통화 (쉼표 구분, 예: USD,JPY)"),
):
    """
    향후 N시간 이내 경제 이벤트 반환.

    trading-engine의 EventFilter가 이 엔드포인트를 5분 TTL 캐시로 조회.
    country 미지정 시 USD/JPY/GBP/EUR 전체 반환.
    """
    # 통화 파라미터 검증
    if country.strip():
        countries = [c.strip().upper() for c in country.split(",") if c.strip()]
        countries = [c for c in countries if c in _VALID_COUNTRIES]
        if not countries:
            countries = list(_VALID_COUNTRIES)
    else:
        countries = ["USD", "JPY", "GBP", "EUR"]

    svc = get_economic_calendar_service()
    events = await svc.get_upcoming_events(countries=countries, hours_ahead=hours)

    return {
        "hours_ahead": hours,
        "countries": countries,
        "count": len(events),
        "events": events,
    }


@router.get("/status")
@handle_api_errors("경제 캘린더 수집 상태 조회")
async def get_calendar_status():
    """경제 캘린더 수집 서비스 상태."""
    svc = get_economic_calendar_service()
    return svc.get_status()
