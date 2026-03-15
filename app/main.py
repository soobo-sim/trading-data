"""
CoinMarket Data Service
FastAPI 진입점 — Coincheck / BitFlyer 마켓 데이터 수집 및 API 제공
Port: 8002
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app import __version__
from app.core.config import get_settings
from app.core.exceptions import MarketDataAPIError
from app.database import init_db, close_db
from app.routes import ck_market, bf_market, candles as candles_router, bf_candles as bf_candles_router
from app.routes import system as system_router
from app.routes import status as status_router
from app.routes import bf_funding_rate as bf_funding_rate_router


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    s = get_settings()
    level = getattr(logging, s.LOG_LEVEL.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    for noisy in ("websockets", "websockets.client", "httpcore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    fh = TimedRotatingFileHandler(
        filename="logs/coinmarket-data.log",
        when="midnight", interval=1, backupCount=14,
        encoding="utf-8", utc=False,
    )
    fh.setLevel(level)
    fh.setFormatter(formatter)
    fh.suffix = "%Y-%m-%d"
    root.addHandler(fh)


setup_logging()
logger = logging.getLogger(__name__)
settings = get_settings()

# 백그라운드 태스크 레퍼런스 유지 (GC 방지)
_background_tasks: list = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """startup → yield (serving) → shutdown"""
    logger.info(f"CoinMarket Data Service v{__version__} 시작...")

    # 1. DB 초기화 (테이블 확인용 — Alembic이 정본)
    await init_db()
    logger.info("DB 연결 완료")

    # 1b. 장애 복구: 이전 기동에서 완결되지 못한 stale 캔들 일괄 처리
    try:
        from app.services.candle_service import get_candle_service
        from app.services.bf_candle_service import get_bf_candle_service
        ck_fixed = await get_candle_service().recover_stale_candles()
        bf_fixed = await get_bf_candle_service().recover_stale_candles()
        if ck_fixed or bf_fixed:
            logger.warning(f"[Startup] stale 캔들 복구 완료: CK={ck_fixed}건, BF={bf_fixed}건")
        else:
            logger.info("[Startup] stale 캔들 없음 (정상 종료 이력)")
    except Exception as e:
        logger.warning(f"[Startup] stale 캔들 복구 오류: {e}")

    # 2. Coincheck Public WebSocket 시작
    try:
        from app.services.ck_ws_client import get_coincheck_ws_client
        ck_ws = get_coincheck_ws_client()
        task = asyncio.create_task(ck_ws.run(), name="ck_ws_client")
        _background_tasks.append(task)
        logger.info(f"Coincheck WS 클라이언트 시작. Pairs: {ck_ws._pairs}")
    except Exception as e:
        logger.warning(f"Coincheck WS 시작 실패: {e}")

    # 3. BitFlyer Public WebSocket 시작
    try:
        from app.services.bf_ws_client import get_bitflyer_ws_client
        bf_ws = get_bitflyer_ws_client()
        task = asyncio.create_task(bf_ws.connect(), name="bf_ws_client")
        _background_tasks.append(task)
        logger.info(f"BitFlyer WS 클라이언트 시작. Products: {settings.bf_ws_products_list}")
    except Exception as e:
        logger.warning(f"BitFlyer WS 시작 실패: {e}")

    # 4. BitFlyer 캔들 파이프라인 시작 (설정된 모든 product)
    try:
        from app.services.bf_candle_pipeline import get_bf_candle_pipeline
        bf_pipeline = get_bf_candle_pipeline()
        for pc in settings.bf_ws_products_list:
            task = asyncio.create_task(
                bf_pipeline.start(pc),
                name=f"bf_candle_pipeline_start:{pc}",
            )
            _background_tasks.append(task)
        logger.info(f"BitFlyer 캔들 파이프라인 시작 요청: products={settings.bf_ws_products_list}")
    except Exception as e:
        logger.warning(f"BitFlyer 캔들 파이프라인 시작 실패: {e}")

    # 5. Coincheck 캔들 파이프라인 시작 (설정된 모든 pair)
    try:
        from app.services.candle_pipeline import get_candle_pipeline
        pipeline = get_candle_pipeline()
        for pair in settings.ck_ws_pairs_list:
            task = asyncio.create_task(
                pipeline.start(pair),
                name=f"candle_pipeline_start:{pair}",
            )
            _background_tasks.append(task)
        logger.info(f"캔들 파이프라인 시작 요청: pairs={settings.ck_ws_pairs_list}")
    except Exception as e:
        logger.warning(f"캔들 파이프라인 시작 실패: {e}")
    # 6. BitFlyer 펀딩레이트 폴러 시작 (FX_BTC_JPY, 15분 주기)
    try:
        from app.services.bf_funding_rate_service import get_bf_funding_rate_service
        await get_bf_funding_rate_service().start()
        logger.info("BitFlyer 펀딩레이트 폴러 시작 (FX_BTC_JPY)")
    except Exception as e:
        logger.warning(f"BitFlyer 펀딩레이트 폴러 시작 실패: {e}")
    logger.info(f"CoinMarket Data Service 준비 완료 — port 8002")
    yield

    # ── Shutdown ─────────────────────────────────────────────────────
    logger.info("CoinMarket Data Service 종료 중...")
    # 펀딩레이트 폴러 종료
    try:
        from app.services.bf_funding_rate_service import get_bf_funding_rate_service
        await get_bf_funding_rate_service().stop()
        logger.info("펀딩레이트 폴러 종료")
    except Exception as e:
        logger.warning(f"펀딩레이트 폴러 종료 오류: {e}")
    # Coincheck 캔들 파이프라인 종료
    try:
        from app.services.candle_pipeline import get_candle_pipeline
        await get_candle_pipeline().stop_all()
        logger.info("Coincheck 캔들 파이프라인 종료")
    except Exception as e:
        logger.warning(f"Coincheck 캔들 파이프라인 종료 오류: {e}")

    # BitFlyer 캔들 파이프라인 종료
    try:
        from app.services.bf_candle_pipeline import get_bf_candle_pipeline
        await get_bf_candle_pipeline().stop_all()
        logger.info("BitFlyer 캔들 파이프라인 종료")
    except Exception as e:
        logger.warning(f"BitFlyer 캔들 파이프라인 종료 오류: {e}")

    for task in _background_tasks:
        if not task.done():
            task.cancel()
    if _background_tasks:
        await asyncio.gather(*_background_tasks, return_exceptions=True)

    try:
        from app.services.ck_ws_client import close_coincheck_ws_client
        await close_coincheck_ws_client()
        logger.info("Coincheck WS 클라이언트 종료")
    except Exception:
        pass

    try:
        from app.services.bf_ws_client import close_bitflyer_ws_client
        await close_bitflyer_ws_client()
        logger.info("BitFlyer WS 클라이언트 종료")
    except Exception:
        pass

    try:
        from app.services.ck_public_client import close_coincheck_public_client
        await close_coincheck_public_client()
    except Exception:
        pass

    try:
        from app.services.bf_public_client import close_bitflyer_public_client
        await close_bitflyer_public_client()
    except Exception:
        pass

    await close_db()
    logger.info("CoinMarket Data Service 종료 완료")


# ── FastAPI 앱 ─────────────────────────────────────────────────
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=__version__,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 예외 핸들러 ────────────────────────────────────────────────

@app.exception_handler(MarketDataAPIError)
async def market_data_error_handler(request: Request, exc: MarketDataAPIError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "error": "market_data_api_error", "message": exc.detail},
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"success": False, "error": "validation_error", "message": str(exc)},
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": "internal_server_error", "message": "Internal server error"},
    )


# ── 라우터 등록 ────────────────────────────────────────────────
app.include_router(ck_market.router)
app.include_router(bf_market.router)
app.include_router(candles_router.router)
app.include_router(bf_candles_router.router)
app.include_router(bf_funding_rate_router.router)
app.include_router(system_router.router)
app.include_router(status_router.router)


@app.get("/", tags=["Root"])
async def root():
    return {
        "service": "CoinMarket Data API",
        "version": __version__,
        "docs": "/docs",
        "status": "running",
        "endpoints": {
            "coincheck_market": "/api/ck/ticker | /api/ck/order_books | /api/ck/trades | /api/ck/ws/market-pulse",
            "bitflyer_market": "/api/bf/ticker | /api/bf/order_books | /api/bf/trades | /api/bf/ws/market-pulse",
            "ck_candles": "/api/ck/candles/{pair}/{timeframe} | /api/ck/candles/{pair}/status",
            "bf_candles": "/api/bf/candles/{product_code}/{timeframe} | /api/bf/candles/{product_code}/status",
            "bf_funding_rate": "/api/bf/funding-rate | /api/bf/funding-rate/history",
        },
    }


@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": __version__,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8002, reload=True, log_level="info")
