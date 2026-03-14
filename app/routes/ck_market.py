"""
Coincheck 마켓 데이터 라우트
prefix: /api/ck
"""
import logging
from typing import Optional
from fastapi import APIRouter, Query, Path
from app.core.error_handlers import handle_api_errors
from app.services.ck_public_client import get_coincheck_public_client
from app.services.ck_ws_client import get_coincheck_ws_client
from app.services.ck_market_pulse import calculate_market_pulse
from app.core.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ck", tags=["Coincheck Market"])


@router.get("/ticker")
@handle_api_errors("CK Ticker 조회")
async def get_ticker(pair: str = Query("btc_jpy")):
    return await get_coincheck_public_client().get_ticker(pair)


@router.get("/order_books")
@handle_api_errors("CK 호가창 조회")
async def get_order_books(pair: str = Query("btc_jpy")):
    return await get_coincheck_public_client().get_order_books(pair)


@router.get("/trades")
@handle_api_errors("CK 체결 이력 조회")
async def get_trades(
    pair: str = Query("btc_jpy"),
    limit: int = Query(20, ge=1, le=100),
):
    if not pair or pair.strip() == "":
        pair = "btc_jpy"
    return await get_coincheck_public_client().get_trades(pair, limit)


@router.get("/rate/{pair}")
@handle_api_errors("CK 기준 환율 조회")
async def get_rate(pair: str = Path(...)):
    return await get_coincheck_public_client().get_rate(pair)


@router.get("/exchange_status")
@handle_api_errors("CK 거래소 상태 조회")
async def get_exchange_status():
    return await get_coincheck_public_client().get_exchange_status()


@router.get("/ws/status")
async def get_ws_status():
    """Coincheck Public WS 연결 상태"""
    return get_coincheck_ws_client().get_status()


@router.get("/ws/market-pulse")
@handle_api_errors("CK Market Pulse 조회")
async def get_market_pulse(
    pair: str = Query("btc_jpy"),
    window_sec: int = Query(None, ge=5, le=600),
):
    """WS 버퍼 기반 Coincheck 시장 분석"""
    settings = get_settings()
    win = window_sec or settings.WS_WINDOW_SEC
    ws = get_coincheck_ws_client()
    trades = ws.get_recent_trades(pair, window_sec=win)
    orderbook = ws.get_latest_orderbook(pair)
    return calculate_market_pulse(trades, orderbook, pair, win)


@router.get("/ws/recent-trades")
async def get_recent_trades(
    pair: str = Query("btc_jpy"),
    seconds: int = Query(30, ge=1, le=600),
):
    ws = get_coincheck_ws_client()
    return {"trades": ws.get_recent_trades(pair, window_sec=seconds), "window_sec": seconds}
