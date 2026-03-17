"""
BitFlyer 마켓 데이터 라우트
prefix: /api/bf
"""
import logging
from typing import Optional
from fastapi import APIRouter, Query
from app.core.error_handlers import handle_api_errors
from app.services.bf_public_client import get_bitflyer_public_client
from app.services.bf_ws_client import get_bitflyer_ws_client
from app.services.bf_market_pulse import calculate_bf_market_pulse
from app.core.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/bf", tags=["BitFlyer Market"])


@router.get("/ticker")
@handle_api_errors("BF Ticker 조회")
async def get_ticker(product_code: Optional[str] = Query(None)):
    return await get_bitflyer_public_client().get_ticker(product_code)


@router.get("/order_books")
@handle_api_errors("BF 호가창 조회")
async def get_order_books(product_code: Optional[str] = Query(None)):
    return await get_bitflyer_public_client().get_board(product_code)


@router.get("/trades")
@handle_api_errors("BF 체결 이력 조회")
async def get_trades(
    product_code: Optional[str] = Query(None),
    count: int = Query(50, ge=1, le=500),
):
    return await get_bitflyer_public_client().get_executions(product_code, count=count)


@router.get("/exchange_status")
@handle_api_errors("BF 거래소 상태 조회")
async def get_exchange_status():
    return await get_bitflyer_public_client().get_exchange_status()


@router.get("/ws/status")
async def get_ws_status():
    """BitFlyer Public WS 연결 상태 (멀티 product)"""
    ws = get_bitflyer_ws_client()
    return ws.get_status()


@router.get("/ws/market-pulse")
@handle_api_errors("BF Market Pulse 조회")
async def get_market_pulse(
    product_code: Optional[str] = Query(None),
    window_sec: int = Query(None, ge=5, le=600),
):
    """WS 버퍼 기반 BitFlyer 시장 분석 (fx_spread_pct 포함 시 FX_BTC_JPY 수집 중이어야 함)"""
    settings = get_settings()
    win = window_sec or settings.WS_WINDOW_SEC
    pc = (product_code or settings.bf_ws_products_list[0]).upper()
    ws = get_bitflyer_ws_client()
    trades = ws.get_recent_trades(seconds=win, product_code=pc)
    orderbook = ws.get_latest_board(pc)
    fx_spread_pct = ws.get_fx_spread()
    return calculate_bf_market_pulse(trades, orderbook, pc, win, fx_spread_pct=fx_spread_pct)


@router.get("/ws/recent-trades")
async def get_recent_trades(
    product_code: Optional[str] = Query(None),
    seconds: int = Query(30, ge=1, le=600),
):
    settings = get_settings()
    ws = get_bitflyer_ws_client()
    pc = (product_code or settings.bf_ws_products_list[0]).upper()
    return {"product_code": pc, "trades": ws.get_recent_trades(seconds=seconds, product_code=pc), "window_sec": seconds}


@router.get("/ws/status")
async def get_ws_status():
    """BitFlyer WS 전체 연결 상태 (멀티 product)"""
    ws = get_bitflyer_ws_client()
    return ws.get_status()
