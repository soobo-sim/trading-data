"""
CoinMarket Data Service
FastAPI 진입점 — BitFlyer / GMO FX 마켓 데이터 수집 및 API 제공
Port: 8002
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from logging.handlers import TimedRotatingFileHandler

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app import __version__
from app.core.config import get_settings
from app.core.exceptions import MarketDataAPIError
from app.database import init_db, close_db
from app.routes import bf_market, bf_candles as bf_candles_router
from app.routes import system as system_router
from app.routes import status as status_router
from app.routes import bf_funding_rate as bf_funding_rate_router
from app.routes import gmo_candles as gmo_candles_router
from app.routes import economic_calendar as economic_calendar_router
from app.routes import intermarket as intermarket_router
from app.routes import news as news_router
from app.routes import sentiment as sentiment_router


class JSONFormatter(logging.Formatter):
    """JSON 구조화 로그 포맷터 (trading-engine과 동일 형식)."""

    _JST = timezone(timedelta(hours=9))

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "ts": datetime.fromtimestamp(record.created, tz=self._JST).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "exchange": "trading-data",
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    s = get_settings()
    console_level = getattr(logging, s.LOG_LEVEL.upper(), logging.INFO)
    json_fmt = JSONFormatter()

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # 루트는 DEBUG — 파일에 전부 남김
    root.handlers.clear()

    for noisy in ("websockets", "websockets.client", "httpcore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # 1) 콘솔: LOG_LEVEL 이상
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(json_fmt)
    root.addHandler(ch)

    # 2) 파일: DEBUG 이상 (30일 보관)
    fh = TimedRotatingFileHandler(
        filename="logs/trading-data.log",
        when="midnight", interval=1, backupCount=30,
        encoding="utf-8", utc=False,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(json_fmt)
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
        from app.services.bf_candle_service import get_bf_candle_service
        from app.services.gmo_candle_service import get_gmo_candle_service
        bf_fixed = await get_bf_candle_service().recover_stale_candles()
        gmo_fixed = await get_gmo_candle_service().recover_stale_candles()
        if bf_fixed or gmo_fixed:
            logger.warning(f"[Startup] stale 캔들 복구 완료: BF={bf_fixed}건, GMO={gmo_fixed}건")
        else:
            logger.info("[Startup] stale 캔들 없음 (정상 종료 이력)")
    except Exception as e:
        logger.warning(f"[Startup] stale 캔들 복구 오류: {e}")

    # 2. BitFlyer Public WebSocket 시작
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

    # 4. BitFlyer 펀딩레이트 폴러 시작 (FX_BTC_JPY, 15분 주기)
    try:
        from app.services.bf_funding_rate_service import get_bf_funding_rate_service
        await get_bf_funding_rate_service().start()
        logger.info("BitFlyer 펀딩레이트 폴러 시작 (FX_BTC_JPY)")
    except Exception as e:
        logger.warning(f"BitFlyer 펀딩레이트 폴러 시작 실패: {e}")

    # 7. GMO FX 캔들 파이프라인 시작 (설정된 모든 pair)
    try:
        gmo_pairs = settings.gmo_fx_pairs_list
        if gmo_pairs:
            from app.services.gmo_candle_pipeline import get_gmo_candle_pipeline
            gmo_pipeline = get_gmo_candle_pipeline()
            for pair in gmo_pairs:
                task = asyncio.create_task(
                    gmo_pipeline.start(pair),
                    name=f"gmo_candle_pipeline_start:{pair}",
                )
                _background_tasks.append(task)
            logger.info(f"GMO FX 캔들 파이프라인 시작 요청: pairs={gmo_pairs}")
        else:
            logger.info("GMO FX 캔들 파이프라인 비활성 (GMO_FX_PAIRS 미설정)")
    except Exception as e:
        logger.warning(f"GMO FX 캔들 파이프라인 시작 실패: {e}")
    # 8. 경제 캘린더 수집기 시작
    try:
        from app.services.economic_calendar import get_economic_calendar_service
        await get_economic_calendar_service().start()
        logger.info("경제 캘린더 폴러 시작 (6시간 주기)")
    except Exception as e:
        logger.warning(f"경제 캘린더 폴러 시작 실패: {e}")

    # 9. FRED 매크로 지표 수집기 시작 (F-04)
    try:
        from app.services.fred_service import get_fred_service
        await get_fred_service().start()
        logger.info("FRED 매크로 지표 수집기 시작 (매일 08:00 JST)")
    except Exception as e:
        logger.warning(f"FRED 수집기 시작 실패: {e}")

    # 10. 뉴스 수집기 시작 (Marketaux, 15분 주기)
    try:
        from app.services.news_collector import get_news_collector_service
        await get_news_collector_service().start()
        logger.info("뉴스 수집기 시작 (15분 주기, Marketaux)")
    except Exception as e:
        logger.warning(f"뉴스 수집기 시작 실패: {e}")

    # 11. 센티먼트 수집기 시작 (Alternative.me FNG, 1시간 주기)
    try:
        from app.services.sentiment_collector import get_sentiment_collector_service
        await get_sentiment_collector_service().start()
        logger.info("센티먼트 수집기 시작 (1시간 주기, Alternative.me FNG)")
    except Exception as e:
        logger.warning(f"센티먼트 수집기 시작 실패: {e}")

    logger.info(f"CoinMarket Data Service 준비 완료 — port 8002")
    yield

    # ── Shutdown ─────────────────────────────────────────────────────
    logger.info("CoinMarket Data Service 종료 중...")    # 경제 캘린더 폴러 종료
    try:
        from app.services.economic_calendar import get_economic_calendar_service
        await get_economic_calendar_service().stop()
        logger.info("경제 캘린더 폴러 종료")
    except Exception as e:
        logger.warning(f"경제 캘린더 폴러 종료 오류: {e}")

    # FRED 매크로 지표 수집기 종료
    try:
        from app.services.fred_service import get_fred_service
        await get_fred_service().stop()
        logger.info("FRED 수집기 종료")
    except Exception as e:
        logger.warning(f"FRED 수집기 종료 오류: {e}")

    # 뉴스 수집기 종료
    try:
        from app.services.news_collector import get_news_collector_service
        await get_news_collector_service().stop()
        logger.info("뉴스 수집기 종료")
    except Exception as e:
        logger.warning(f"뉴스 수집기 종료 오류: {e}")

    # 센티먼트 수집기 종료
    try:
        from app.services.sentiment_collector import get_sentiment_collector_service
        await get_sentiment_collector_service().stop()
        logger.info("센티먼트 수집기 종료")
    except Exception as e:
        logger.warning(f"센티먼트 수집기 종료 오류: {e}")

    # 펀딩레이트 폴러 종료
    try:
        from app.services.bf_funding_rate_service import get_bf_funding_rate_service
        await get_bf_funding_rate_service().stop()
        logger.info("펀딩레이트 폴러 종료")
    except Exception as e:
        logger.warning(f"펀딩레이트 폴러 종료 오류: {e}")

    # BitFlyer 캔들 파이프라인 종료
    try:
        from app.services.bf_candle_pipeline import get_bf_candle_pipeline
        await get_bf_candle_pipeline().stop_all()
        logger.info("BitFlyer 캔들 파이프라인 종료")
    except Exception as e:
        logger.warning(f"BitFlyer 캔들 파이프라인 종료 오류: {e}")

    # GMO FX 캔들 파이프라인 종료
    try:
        if settings.gmo_fx_pairs_list:
            from app.services.gmo_candle_pipeline import get_gmo_candle_pipeline
            await get_gmo_candle_pipeline().stop_all()
            logger.info("GMO FX 캔들 파이프라인 종료")
    except Exception as e:
        logger.warning(f"GMO FX 캔들 파이프라인 종료 오류: {e}")

    for task in _background_tasks:
        if not task.done():
            task.cancel()
    if _background_tasks:
        await asyncio.gather(*_background_tasks, return_exceptions=True)

    try:
        from app.services.bf_ws_client import close_bitflyer_ws_client
        await close_bitflyer_ws_client()
        logger.info("BitFlyer WS 클라이언트 종료")
    except Exception:
        pass

    try:
        from app.services.bf_public_client import close_bitflyer_public_client
        await close_bitflyer_public_client()
    except Exception:
        pass

    try:
        from app.services.gmo_public_client import close_gmo_public_client
        await close_gmo_public_client()
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

app.include_router(bf_market.router)
app.include_router(bf_candles_router.router)
app.include_router(bf_funding_rate_router.router)
app.include_router(gmo_candles_router.router)
app.include_router(economic_calendar_router.router)
app.include_router(intermarket_router.router)
app.include_router(news_router.router)
app.include_router(sentiment_router.router)
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
            "bitflyer_market": "/api/bf/ticker | /api/bf/order_books | /api/bf/trades | /api/bf/ws/market-pulse",
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
