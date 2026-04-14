"""
System Health API (Trading Data)

GET /api/system/health  — GMO Coin 캔들 파이프라인 헬스 체크
  - GmoCoinCandlePipeline 태스크 생존 여부
  - 당일 로그 에러 최근 5건
"""
import os
from datetime import datetime, timezone, date

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.gmo_coin_candle_pipeline import get_gmo_coin_candle_pipeline
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
    Trading Data 전체 헬스 체크.
    healthy=false 또는 issues 목록이 비어있지 않으면 장애 상태.
    """
    issues = []
    now = datetime.now(timezone.utc).isoformat()

    # ── GMO Coin Candle Pipeline Tasks ────────────────────────
    gmoc_pipeline = get_gmo_coin_candle_pipeline()
    gmoc_running = gmoc_pipeline.running_pairs()
    gmoc_pipeline_ok = len(gmoc_running) > 0
    if not gmoc_pipeline_ok:
        issues.append("gmo_coin_candle_pipeline: 실행 중인 pair 없음")

    recent_errors = _get_recent_log_errors("trading-data")
    healthy = gmoc_pipeline_ok

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "healthy": healthy,
            "checked_at": now,
            "recent_log_errors": recent_errors,
            "services": {
                "gmo_coin_candle_pipeline": {
                    "ok": gmoc_pipeline_ok,
                    "running_pairs": gmoc_running,
                },
            },
            "issues": issues,
        },
    )
