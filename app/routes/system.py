"""
System Health API (CoinMarket Data)

GET /api/system/health  — WebSocket 연결 상태 및 캔들 파이프라인 헬스 체크
  - Coincheck Public WS 연결 상태
  - BitFlyer Public WS 연결 상태
  - CandlePipeline 태스크 생존 여부 (CK pair별)
  - BfCandlePipeline 태스크 생존 여부 (BF product별)
  - 당일 로그 에러 최근 5건
"""
import os
from datetime import datetime, timezone, date

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.ck_ws_client import get_coincheck_ws_client
from app.services.bf_ws_client import get_bitflyer_ws_client
from app.services.candle_pipeline import get_candle_pipeline
from app.services.bf_candle_pipeline import get_bf_candle_pipeline

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

    # ── Coincheck Public WS ────────────────────────────────────
    ck_ws = get_coincheck_ws_client()
    ck_ws_status = ck_ws.get_status()
    ck_ws_ok = ck_ws_status.get("connected", False)
    if not ck_ws_ok:
        issues.append("ck_public_ws: 연결 끊김")

    # ── BitFlyer Public WS ────────────────────────────────────
    bf_ws = get_bitflyer_ws_client()
    bf_ws_status = bf_ws.get_status()
    bf_ws_ok = bf_ws_status.get("connected", False)
    if not bf_ws_ok:
        issues.append("bf_public_ws: 연결 끊김")

    # ── CK Candle Pipeline Tasks ──────────────────────────────
    ck_pipeline = get_candle_pipeline()
    ck_running = ck_pipeline.running_pairs()
    ck_pipeline_ok = len(ck_running) > 0
    if not ck_pipeline_ok:
        issues.append("ck_candle_pipeline: 실행 중인 pair 없음")

    # ── BF Candle Pipeline Tasks ──────────────────────────────
    bf_pipeline = get_bf_candle_pipeline()
    bf_running = bf_pipeline.running_products()
    bf_pipeline_ok = len(bf_running) > 0
    if not bf_pipeline_ok:
        issues.append("bf_candle_pipeline: 실행 중인 product 없음")

    recent_errors = _get_recent_log_errors("coinmarket-data")
    healthy = ck_ws_ok and bf_ws_ok and ck_pipeline_ok and bf_pipeline_ok

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "healthy": healthy,
            "checked_at": now,
            "recent_log_errors": recent_errors,
            "services": {
                "ck_public_ws": {
                    "ok": ck_ws_ok,
                    "connected": ck_ws_status.get("connected"),
                    "uptime_sec": ck_ws_status.get("uptime_sec"),
                    "subscribed_pairs": ck_ws_status.get("subscribed_pairs", []),
                },
                "bf_public_ws": {
                    "ok": bf_ws_ok,
                    "connected": bf_ws_status.get("connected"),
                    "subscribed_products": bf_ws_status.get("subscribed_products", []),
                },
                "ck_candle_pipeline": {
                    "running_pairs": ck_running,
                },
                "bf_candle_pipeline": {
                    "running_products": bf_running,
                },
            },
            "issues": issues,
        },
    )
