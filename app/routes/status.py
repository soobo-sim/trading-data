"""
인프라 상태 API

GET /api/status  — 레이첼(분석 에이전트)이 분석 전 인프라 현황 파악용
  응답 항목:
  - ck_candles: 수집 중인 CK pair 목록 + pair별 최신 캔들 정보 + DB 총 건수
  - bf_candles: 수집 중인 BF product 목록 + product별 최신 캔들 정보 + DB 총 건수
  - gmo_candles: 수집 중인 GMO FX pair 목록 + pair별 최신 캔들 정보 + DB 총 건수
  - ws: WebSocket 연결 상태 요약
"""
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import select, func as sa_func, and_

from app.database import AsyncSessionLocal
from app.models.database import CkCandle, BfCandle, GmoCandle
from app.services.candle_pipeline import get_candle_pipeline
from app.services.bf_candle_pipeline import get_bf_candle_pipeline
from app.services.gmo_candle_pipeline import get_gmo_candle_pipeline
from app.services.ck_ws_client import get_coincheck_ws_client
from app.services.bf_ws_client import get_bitflyer_ws_client
from app.services.bf_funding_rate_service import get_bf_funding_rate_service

router = APIRouter(prefix="/api", tags=["Status"])


async def _ck_pair_detail(db, pair: str) -> dict:
    """pair별 timeframe(1h/4h) 최신 캔들 + 완성 건수 조회."""
    result = {}
    for tf in ("1h", "4h"):
        # 최신 캔들 (is_complete 무관하게 가장 최신 open_time)
        latest_row = await db.execute(
            select(CkCandle)
            .where(and_(CkCandle.pair == pair, CkCandle.timeframe == tf))
            .order_by(CkCandle.open_time.desc())
            .limit(1)
        )
        latest = latest_row.scalars().first()

        # 완성 캔들 건수
        count_row = await db.execute(
            select(sa_func.count())
            .select_from(CkCandle)
            .where(and_(
                CkCandle.pair == pair,
                CkCandle.timeframe == tf,
                CkCandle.is_complete == True,
            ))
        )
        complete_count = count_row.scalar() or 0

        result[tf] = {
            "latest_open_time": latest.open_time.isoformat() if latest else None,
            "latest_close": float(latest.close) if latest else None,
            "latest_volume": float(latest.volume) if latest else None,
            "latest_tick_count": latest.tick_count if latest else None,
            "latest_is_complete": latest.is_complete if latest else None,
            "complete_candle_count": complete_count,
        }
    return result


async def _bf_product_detail(db, product_code: str) -> dict:
    """product별 timeframe(1h/4h) 최신 캔들 + 완성 건수 조회."""
    result = {}
    for tf in ("1h", "4h"):
        latest_row = await db.execute(
            select(BfCandle)
            .where(and_(BfCandle.product_code == product_code, BfCandle.timeframe == tf))
            .order_by(BfCandle.open_time.desc())
            .limit(1)
        )
        latest = latest_row.scalars().first()

        count_row = await db.execute(
            select(sa_func.count())
            .select_from(BfCandle)
            .where(and_(
                BfCandle.product_code == product_code,
                BfCandle.timeframe == tf,
                BfCandle.is_complete == True,
            ))
        )
        complete_count = count_row.scalar() or 0

        result[tf] = {
            "latest_open_time": latest.open_time.isoformat() if latest else None,
            "latest_close": float(latest.close) if latest else None,
            "latest_volume": float(latest.volume) if latest else None,
            "latest_tick_count": latest.tick_count if latest else None,
            "latest_is_complete": latest.is_complete if latest else None,
            "complete_candle_count": complete_count,
        }
    return result


async def _gmo_pair_detail(db, pair: str) -> dict:
    """GMO FX pair별 timeframe(1h/4h) 최신 캔들 + 완성 건수 조회."""
    result = {}
    for tf in ("1h", "4h"):
        latest_row = await db.execute(
            select(GmoCandle)
            .where(and_(GmoCandle.pair == pair, GmoCandle.timeframe == tf))
            .order_by(GmoCandle.open_time.desc())
            .limit(1)
        )
        latest = latest_row.scalars().first()

        count_row = await db.execute(
            select(sa_func.count())
            .select_from(GmoCandle)
            .where(and_(
                GmoCandle.pair == pair,
                GmoCandle.timeframe == tf,
                GmoCandle.is_complete == True,
            ))
        )
        complete_count = count_row.scalar() or 0

        result[tf] = {
            "latest_open_time": latest.open_time.isoformat() if latest else None,
            "latest_close": float(latest.close) if latest else None,
            "latest_is_complete": latest.is_complete if latest else None,
            "complete_candle_count": complete_count,
        }
    return result


async def _ck_all_known_pairs(db) -> list[str]:
    """DB에 실제로 캔들이 존재하는 CK pair 목록 (수집 이력 전체)."""
    rows = await db.execute(
        select(CkCandle.pair).distinct()
    )
    return sorted(rows.scalars().all())


async def _bf_all_known_products(db) -> list[str]:
    """DB에 실제로 캔들이 존재하는 BF product 목록 (수집 이력 전체)."""
    rows = await db.execute(
        select(BfCandle.product_code).distinct()
    )
    return sorted(rows.scalars().all())


async def _gmo_all_known_pairs(db) -> list[str]:
    """DB에 실제로 캔들이 존재하는 GMO FX pair 목록."""
    rows = await db.execute(
        select(GmoCandle.pair).distinct()
    )
    return sorted(rows.scalars().all())


@router.get("/status", summary="인프라 수집 현황 (레이첼 전용)")
async def get_status():
    """
    현재 수집 중인 캔들 페어·최신 캔들·총 건수를 반환한다.
    레이첼이 분석 시작 전 "어떤 통화 페어 데이터가 준비되어 있는가"를 확인하는 용도.
    """
    now = datetime.now(timezone.utc).isoformat()

    # ── Pipeline / WS 상태 ─────────────────────────────────────
    ck_pipeline = get_candle_pipeline()
    bf_pipeline = get_bf_candle_pipeline()
    ck_ws = get_coincheck_ws_client()
    bf_ws = get_bitflyer_ws_client()

    gmo_pipeline = get_gmo_candle_pipeline()

    ck_running_pairs: list[str] = ck_pipeline.running_pairs()
    bf_running_products: list[str] = bf_pipeline.running_products()
    gmo_running_pairs: list[str] = gmo_pipeline.running_pairs()
    ck_ws_status = ck_ws.get_status()
    bf_ws_status = bf_ws.get_status()
    funding_rate_status = get_bf_funding_rate_service().get_status()

    # ── DB 조회 ────────────────────────────────────────────────
    async with AsyncSessionLocal() as db:
        ck_known = await _ck_all_known_pairs(db)
        bf_known = await _bf_all_known_products(db)
        gmo_known = await _gmo_all_known_pairs(db)

        # 수집 중 pair가 DB에 없으면 목록에 추가 (아직 캔들 없는 신규 pair 대응)
        all_ck_pairs = sorted(set(ck_known) | set(ck_running_pairs))
        all_bf_products = sorted(set(bf_known) | set(bf_running_products))
        all_gmo_pairs = sorted(set(gmo_known) | set(gmo_running_pairs))

        ck_pairs_detail: dict = {}
        for pair in all_ck_pairs:
            ck_pairs_detail[pair] = {
                "is_running": pair in ck_running_pairs,
                "candles": await _ck_pair_detail(db, pair),
            }

        bf_products_detail: dict = {}
        for product in all_bf_products:
            bf_products_detail[product] = {
                "is_running": product in bf_running_products,
                "candles": await _bf_product_detail(db, product),
            }

        gmo_pairs_detail: dict = {}
        for pair in all_gmo_pairs:
            gmo_pairs_detail[pair] = {
                "is_running": pair in gmo_running_pairs,
                "candles": await _gmo_pair_detail(db, pair),
            }

    return {
        "checked_at": now,
        "ck_candles": {
            "collecting_pairs": ck_running_pairs,
            "known_pairs_in_db": ck_known,
            "pairs": ck_pairs_detail,
        },
        "bf_candles": {
            "collecting_products": bf_running_products,
            "known_products_in_db": bf_known,
            "products": bf_products_detail,
        },
        "ws": {
            "ck_public": {
                "connected": ck_ws_status.get("connected", False),
                "subscribed_pairs": ck_ws_status.get("subscribed_pairs", []),
            },
            "bf_public": {
                "connected": bf_ws_status.get("connected", False),
                "subscribed_products": bf_ws_status.get("subscribed_products", []),
            },
        },
        "gmo_candles": {
            "collecting_pairs": gmo_running_pairs,
            "known_pairs_in_db": gmo_known,
            "pairs": gmo_pairs_detail,
        },
        "bf_funding_rate_poller": funding_rate_status,
    }
