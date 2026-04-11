"""
System Health API (CoinMarket Data)

GET /api/system/health  — WebSocket 연결 상태 및 캔들 파이프라인 헬스 체크
  - BitFlyer Public WS 연결 상태
  - BfCandlePipeline 태스크 생존 여부 (BF product별)
  - 당일 로그 에러 최근 5건
"""
import os
from datetime import datetime, timezone, date

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.bf_ws_client import get_bitflyer_ws_client
from app.services.bf_candle_pipeline import get_bf_candle_pipeline
from app.services.gmo_candle_pipeline import get_gmo_candle_pipeline
from app.core.config import get_settings

router = APIRouter(prefix="/api/system", tags=["System"])


def _get_recent_log_errors(log_name: str, max_lines: int = 5) -> list[str]:
    """당일 로그 파일에서 ERROR 레벨 최근 N줄을 반환."""
    today = date.today().strftime("%Y-%m-%d")
    log_path = f"logs/{log_name}.log.{today}"
    try:
        if not os.path.exists(log_path):
            return []
        errors = []
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if " - ERROR - " in line or " ERROR " in line:
                    errors.append(line.strip())
        return errors[-max_lines:]
    except Exception:
        return []


@router.get("/health")
async def system_health():
    """
    CoinMarket Data 전체 헬스 체크.
    healthy=false 또는 issues 목록이 비어있지 않으면 장애 상태.
    """
    issues = []
    now = datetime.now(timezone.utc).isoformat()

    # ── BitFlyer Public WS ────────────────────────────────────
    bf_ws = get_bitflyer_ws_client()
    bf_ws_status = bf_ws.get_status()
    bf_ws_ok = bf_ws_status.get("connected", False)
    if not bf_ws_ok:
        issues.append("bf_public_ws: 연결 끊김")

    # ── BF Candle Pipeline Tasks ──────────────────────────────
    bf_pipeline = get_bf_candle_pipeline()
    bf_running = bf_pipeline.running_products()
    bf_pipeline_ok = len(bf_running) > 0
    if not bf_pipeline_ok:
        issues.append("bf_candle_pipeline: 실행 중인 product 없음")

    # ── GMO FX Candle Pipeline Tasks ──────────────────────────
    gmo_pipeline = get_gmo_candle_pipeline()
    gmo_running = gmo_pipeline.running_pairs()
    gmo_fx_pairs = get_settings().gmo_fx_pairs_list
    gmo_pipeline_ok = True
    if gmo_fx_pairs:
        # GMO FX가 설정되어 있으면 파이프라인도 실행 중이어야 함
        if len(gmo_running) == 0:
            issues.append("gmo_candle_pipeline: 실행 중인 pair 없음")
            gmo_pipeline_ok = False

    recent_errors = _get_recent_log_errors("trading-data")
    healthy = bf_ws_ok and bf_pipeline_ok and gmo_pipeline_ok

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "healthy": healthy,
            "checked_at": now,
            "recent_log_errors": recent_errors,
            "services": {
                "bf_public_ws": {
                    "ok": bf_ws_ok,
                    "connected": bf_ws_status.get("connected"),
                    "subscribed_products": bf_ws_status.get("subscribed_products", []),
                },
                "bf_candle_pipeline": {
                    "running_products": bf_running,
                },
                "gmo_candle_pipeline": {
                    "ok": gmo_pipeline_ok,
                    "running_pairs": gmo_running,
                    "configured_pairs": gmo_fx_pairs,
                },
            },
            "issues": issues,
        },
    )
