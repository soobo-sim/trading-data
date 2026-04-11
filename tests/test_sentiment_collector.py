"""
센티먼트 수집 파이프라인 단위 테스트.

SentimentCollectorService — Alternative.me Fear & Greed Index 수집 로직 (DB 없이 mock 사용).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.sentiment_collector import SentimentCollectorService


def _make_fng_response(value: str = "25", classification: str = "Extreme Fear") -> dict:
    """Alternative.me FNG API 응답 mock."""
    return {
        "name": "Fear and Greed Index",
        "data": [
            {
                "value": value,
                "value_classification": classification,
                "timestamp": "1744329600",
                "time_until_update": "3600",
            }
        ],
        "metadata": {"error": None},
    }


def _make_mock_fng_client(response: dict) -> AsyncMock:
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value=response)

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=mock_resp)
    return mock_client


def _make_mock_session() -> tuple:
    """DB session mock."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    mock_session_local = MagicMock(return_value=mock_session)
    return mock_session_local, mock_session


# ── TC1: 정상 응답 → INSERT ───────────────────────────────────

@pytest.mark.asyncio
async def test_poll_once_inserts_on_success():
    """
    TC1: Alternative.me 정상 응답 (score=25, "Extreme Fear")
    → SentimentScore INSERT, score=25, classification="Extreme Fear"
    """
    svc = SentimentCollectorService()
    mock_client = _make_mock_fng_client(_make_fng_response("25", "Extreme Fear"))
    mock_session_local, mock_session = _make_mock_session()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.sentiment_collector.AsyncSessionLocal", mock_session_local):
        await svc._poll_once()

    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()
    assert svc._last_score == 25


# ── TC2: API 실패 → WARNING, 크래시 없음 ─────────────────────

@pytest.mark.asyncio
async def test_poll_once_api_failure_no_crash(caplog):
    """
    TC2: API timeout/4xx
    → WARNING 로그, 예외 전파 없음
    """
    import httpx

    svc = SentimentCollectorService()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

    mock_session_local, mock_session = _make_mock_session()

    import logging
    with caplog.at_level(logging.WARNING), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.sentiment_collector.AsyncSessionLocal", mock_session_local):
        await svc._poll_once()  # 예외 전파 없어야 함

    mock_session.execute.assert_not_called()
    assert any("FNG" in r.message or "실패" in r.message for r in caplog.records)


# ── TC3: 동일 score → INSERT 스킵 ────────────────────────────

@pytest.mark.asyncio
async def test_poll_once_skips_insert_on_same_score():
    """
    TC3: 동일 score 연속 폴링
    → 두 번째 INSERT 스킵 (self._last_score 체크)
    """
    svc = SentimentCollectorService()
    svc._last_score = 25  # 이미 score=25로 캐시됨

    mock_client = _make_mock_fng_client(_make_fng_response("25", "Extreme Fear"))
    mock_session_local, mock_session = _make_mock_session()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.sentiment_collector.AsyncSessionLocal", mock_session_local):
        await svc._poll_once()

    mock_session.execute.assert_not_called()
    mock_session.commit.assert_not_called()


# ── 엣지 케이스 보강 ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_poll_once_empty_data_array_no_crash(caplog):
    """
    엣지: API 응답 data[] 빈 배열 → IndexError → WARNING, 크래시 없음.
    """
    import logging

    svc = SentimentCollectorService()
    empty_response = {"name": "Fear and Greed Index", "data": [], "metadata": {"error": None}}
    mock_client = _make_mock_fng_client(empty_response)
    mock_session_local, mock_session = _make_mock_session()

    with caplog.at_level(logging.WARNING), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.sentiment_collector.AsyncSessionLocal", mock_session_local):
        await svc._poll_once()

    mock_session.execute.assert_not_called()
    assert any("FNG" in r.message or "실패" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_start_double_call_no_duplicate_task(caplog):
    """
    엣지: start() 중복 호출 → 이미 실행 중 경고, 태스크 중복 생성 안 함.
    """
    import logging
    import asyncio

    svc = SentimentCollectorService(poll_interval=9999)

    # 첫 번째 start — 실제 asyncio 루프에서 태스크 생성
    await svc.start()
    first_task = svc._task
    assert first_task is not None

    with caplog.at_level(logging.WARNING):
        await svc.start()  # 두 번째 호출

    # 태스크 교체 없음
    assert svc._task is first_task
    assert any("이미" in r.message for r in caplog.records)

    # 정리
    await svc.stop()


@pytest.mark.asyncio
async def test_poll_once_score_update_on_change():
    """
    엣지: 직전 25에서 50으로 변경 → _last_score=50, INSERT 호출.
    """
    svc = SentimentCollectorService()
    svc._last_score = 25  # 이전 score

    mock_client = _make_mock_fng_client(_make_fng_response("50", "Neutral"))
    mock_session_local, mock_session = _make_mock_session()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.sentiment_collector.AsyncSessionLocal", mock_session_local):
        await svc._poll_once()

    mock_session.execute.assert_called_once()
    assert svc._last_score == 50
