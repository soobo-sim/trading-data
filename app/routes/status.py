"""
인프라 상태 API

GET /api/status  — 레이첼(분석 에이전트)이 분석 전 인프라 현황 파악용
  응답 항목:
  - gmoc_candles: 수집 중인 GMO Coin pair 목록 + pair별 최신 캔들 정보 + DB 총 건수
"""
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import select, func as sa_func, and_

from app.database import AsyncSessionLocal
from app.models.database import GmocCandle
from app.services.gmo_coin_candle_pipeline import get_gmo_coin_candle_pipeline

router = APIRouter(prefix="/api", tags=["Status"])


async def _gmoc_pair_detail(db, pair: str) -> dict:
    """GMO Coin pair별 timeframe(1h/4h) 최신 캔들 + 완성 건수 조회."""
    result = {}
    for tf in ("1h", "4h"):
        latest_row = await db.execute(
            select(GmocCandle)
            .where(and_(GmocCandle.pair == pair, GmocCandle.timeframe == tf))
            .order_by(GmocCandle.open_time.desc())
            .limit(1)
        )
        latest = latest_row.scalars().first()

        count_row = await db.execute(
            select(sa_func.count())
            .select_from(GmocCandle)
            .where(and_(
                GmocCandle.pair == pair,
                GmocCandle.timeframe == tf,
                GmocCandle.is_complete == True,
            ))
        )
        complete_count = count_row.scalar() or 0

        result[tf] = {
            "latest_open_time": latest.open_time.isoformat() if latest else None,
            "latest_close": float(latest.close) if latest else None,
            "latest_volume": float(latest.volume) if latest else None,
            "latest_is_complete": latest.is_complete if latest else None,
            "complete_candle_count": complete_count,
        }
    return result


async def _gmoc_all_known_pairs(db) -> list[str]:
    """DB에 실제로 캔들이 존재하는 GMO Coin pair 목록."""
    rows = await db.execute(
        select(GmocCandle.pair).distinct()
    )
    return sorted(rows.scalars().all())


@router.get("/status", summary="인프라 수집 현황 (레이첼 전용)")
async def get_status():
    """
    현재 수집 중인 캔들 페어·최신 캔들·총 건수를 반환한다.
    레이첼이 분석 시작 전 "어떤 통화 페어 데이터가 준비되어 있는가"를 확인하는 용도.
    """
    now = datetime.now(timezone.utc).isoformat()

    # ── Pipeline 상태 ──────────────────────────────────────────
    gmoc_pipeline = get_gmo_coin_candle_pipeline()
    gmoc_running_pairs: list[str] = gmoc_pipeline.running_pairs()

    # ── DB 조회 ────────────────────────────────────────────────
    async with AsyncSessionLocal() as db:
        gmoc_known = await _gmoc_all_known_pairs(db)

        # 수집 중 pair가 DB에 없으면 목록에 추가 (아직 캔들 없는 신규 pair 대응)
        all_gmoc_pairs = sorted(set(gmoc_known) | set(gmoc_running_pairs))

        gmoc_pairs_detail: dict = {}
        for pair in all_gmoc_pairs:
            gmoc_pairs_detail[pair] = {
                "is_running": pair in gmoc_running_pairs,
                "candles": await _gmoc_pair_detail(db, pair),
            }

    return {
        "checked_at": now,
        "gmoc_candles": {
            "collecting_pairs": gmoc_running_pairs,
            "known_pairs_in_db": gmoc_known,
            "pairs": gmoc_pairs_detail,
        },
    }


