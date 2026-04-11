"""
뉴스 수집 파이프라인 단위 테스트.

NewsCollectorService — Marketaux API 수집 로직 (DB 없이 mock 사용).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.news_collector import NewsCollectorService


def _make_settings(token: str = "test-token") -> MagicMock:
    """테스트용 Settings mock."""
    s = MagicMock()
    s.MARKETAUX_API_TOKEN = token
    s.MARKETAUX_POLL_INTERVAL = 900
    return s


def _make_article(
    uuid: str = "uuid-1",
    title: str = "BTC surges",
    published_at: str = "2026-04-09T10:00:00+00:00",
    entities: list | None = None,
) -> dict:
    if entities is None:
        entities = [
            {"symbol": "BTC", "type": "cryptocurrency", "sentiment_score": 0.5}
        ]
    return {
        "uuid": uuid,
        "title": title,
        "snippet": "BTC price analysis",
        "source": "reuters.com",
        "url": "https://reuters.com/btc",
        "published_at": published_at,
        "language": "en",
        "entities": entities,
    }


def _make_mock_httpx_client(articles: list) -> AsyncMock:
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"data": articles, "meta": {"found": len(articles)}})

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=mock_resp)
    return mock_client


def _make_mock_session(rowcount: int = 1) -> tuple:
    """DB session mock — UPSERT rowcount 반환."""
    mock_result = MagicMock()
    mock_result.rowcount = rowcount

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.commit = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_session_local = MagicMock(return_value=mock_session)
    return mock_session_local, mock_session


# ── 정상 수집 ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_and_store_returns_count():
    """정상 응답 2건 → DB 2건 INSERT, 반환 2."""
    svc = NewsCollectorService()
    articles = [_make_article("a1"), _make_article("a2", "ETH dips", "2026-04-09T10:05:00+00:00")]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, _ = _make_mock_session(rowcount=1)

    with patch("app.services.news_collector.get_settings", return_value=_make_settings()), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"search": "bitcoin", "category": "crypto"})

    assert count == 2


@pytest.mark.asyncio
async def test_fetch_and_store_uuid_conflict_returns_zero():
    """UUID 중복 (rowcount=0) → 저장 0건."""
    svc = NewsCollectorService()
    articles = [_make_article("dup-uuid")]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, _ = _make_mock_session(rowcount=0)

    with patch("app.services.news_collector.get_settings", return_value=_make_settings()), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "crypto"})

    assert count == 0


@pytest.mark.asyncio
async def test_fetch_and_store_no_token_skips_api():
    """API 토큰 미설정 → 0건 반환, API 호출 없음."""
    svc = NewsCollectorService()
    mock_client = _make_mock_httpx_client([])

    with patch("app.services.news_collector.get_settings", return_value=_make_settings(token="")), \
         patch("httpx.AsyncClient", return_value=mock_client):
        count = await svc._fetch_and_store({"category": "crypto"})

    assert count == 0
    mock_client.get.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_and_store_api_timeout_raises():
    """API 타임아웃 → 예외 전파 (poll_loop에서 catch)."""
    import httpx
    svc = NewsCollectorService()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

    with patch("app.services.news_collector.get_settings", return_value=_make_settings()), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         pytest.raises(httpx.TimeoutException):
        await svc._fetch_and_store({"category": "crypto"})


@pytest.mark.asyncio
async def test_fetch_and_store_empty_response():
    """API 응답 data=[] → 0건 반환."""
    svc = NewsCollectorService()
    mock_client = _make_mock_httpx_client([])
    mock_session_local, _ = _make_mock_session()

    with patch("app.services.news_collector.get_settings", return_value=_make_settings()), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "crypto"})

    assert count == 0


@pytest.mark.asyncio
async def test_fetch_and_store_entities_empty_sentiment_null():
    """entities=[] → symbols=None, sentiment=None으로 저장."""
    svc = NewsCollectorService()
    articles = [_make_article("no-entities", entities=[])]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, mock_session = _make_mock_session(rowcount=1)

    with patch("app.services.news_collector.get_settings", return_value=_make_settings()), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "general"})

    assert count == 1
    # execute 호출 시 stmt에 symbols=None, sentiment=None 포함 확인
    call_args = mock_session.execute.call_args[0][0]
    compiled = call_args.compile()
    params = compiled.params
    assert params.get("symbols") is None
    assert params.get("sentiment_score") is None


# ── 라운드 로빈 ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_round_robin_query_index():
    """3회 연속 호출 → query_index 0→1→2, 카테고리 순환."""
    from app.services.news_collector import _SEARCH_QUERIES
    svc = NewsCollectorService()

    categories = []

    async def fake_fetch(query_config: dict) -> int:
        categories.append(query_config["category"])
        return 0

    svc._fetch_and_store = fake_fetch  # type: ignore[method-assign]

    for _ in range(3):
        query = _SEARCH_QUERIES[svc._query_index % len(_SEARCH_QUERIES)]
        await svc._fetch_and_store(query)
        svc._query_index += 1

    assert categories == ["crypto", "forex", "macro"]


# ── published_after 커서 ──────────────────────────────────────

@pytest.mark.asyncio
async def test_last_published_at_updated():
    """2건 수집 후 _last_published_at = max(published_at) 갱신."""
    svc = NewsCollectorService()
    articles = [
        _make_article("b1", published_at="2026-04-09T10:00:00+00:00"),
        _make_article("b2", published_at="2026-04-09T10:30:00+00:00"),
    ]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, _ = _make_mock_session(rowcount=1)

    with patch("app.services.news_collector.get_settings", return_value=_make_settings()), \
         patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        await svc._fetch_and_store({"category": "crypto"})

    expected = datetime(2026, 4, 9, 10, 30, 0, tzinfo=timezone.utc)
    assert svc._last_published_at == expected


# ── get_status ────────────────────────────────────────────────

def test_get_status_not_running():
    """미기동 상태 status 확인."""
    svc = NewsCollectorService()

    with patch("app.services.news_collector.get_settings", return_value=_make_settings()):
        status = svc.get_status()

    assert status["running"] is False
    assert status["api_configured"] is True
    assert status["last_published_at"] is None



def _make_article(
    uuid: str = "uuid-1",
    title: str = "BTC surges",
    published_at: str = "2026-04-09T10:00:00+00:00",
    entities: list | None = None,
) -> dict:
    if entities is None:
        entities = [
            {"symbol": "BTC", "type": "cryptocurrency", "sentiment_score": 0.5}
        ]
    return {
        "uuid": uuid,
        "title": title,
        "snippet": "BTC price analysis",
        "source": "reuters.com",
        "url": "https://reuters.com/btc",
        "published_at": published_at,
        "language": "en",
        "entities": entities,
    }


def _make_mock_httpx_client(articles: list) -> AsyncMock:
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"data": articles, "meta": {"found": len(articles)}})

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=mock_resp)
    return mock_client


def _make_mock_session(rowcount: int = 1) -> tuple:
    """DB session mock — UPSERT rowcount 반환."""
    mock_result = MagicMock()
    mock_result.rowcount = rowcount

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.commit = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_session_local = MagicMock(return_value=mock_session)
    return mock_session_local, mock_session


# ── 정상 수집 ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_and_store_returns_count(monkeypatch):
    """정상 응답 2건 → DB 2건 INSERT, 반환 2."""
    svc = NewsCollectorService()
    articles = [_make_article("a1"), _make_article("a2", "ETH dips", "2026-04-09T10:05:00+00:00")]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, _ = _make_mock_session(rowcount=1)

    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    # Settings 캐시 리셋
    from app.core.config import get_settings
    get_settings.cache_clear()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"search": "bitcoin", "category": "crypto"})

    assert count == 2


@pytest.mark.asyncio
async def test_fetch_and_store_uuid_conflict_returns_zero(monkeypatch):
    """UUID 중복 (rowcount=0) → 저장 0건."""
    svc = NewsCollectorService()
    articles = [_make_article("dup-uuid")]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, _ = _make_mock_session(rowcount=0)

    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "crypto"})

    assert count == 0


@pytest.mark.asyncio
async def test_fetch_and_store_no_token_skips_api(monkeypatch):
    """API 토큰 미설정 → 0건 반환, API 호출 없음."""
    svc = NewsCollectorService()
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "")
    from app.core.config import get_settings
    get_settings.cache_clear()

    mock_client = _make_mock_httpx_client([])
    with patch("httpx.AsyncClient", return_value=mock_client):
        count = await svc._fetch_and_store({"category": "crypto"})

    assert count == 0
    mock_client.get.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_and_store_api_timeout_raises(monkeypatch):
    """API 타임아웃 → 예외 전파 (poll_loop에서 catch)."""
    import httpx
    svc = NewsCollectorService()
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

    with patch("httpx.AsyncClient", return_value=mock_client), pytest.raises(httpx.TimeoutException):
        await svc._fetch_and_store({"category": "crypto"})


@pytest.mark.asyncio
async def test_fetch_and_store_empty_response(monkeypatch):
    """API 응답 data=[] → 0건 반환."""
    svc = NewsCollectorService()
    mock_client = _make_mock_httpx_client([])
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    mock_session_local, _ = _make_mock_session()
    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "crypto"})

    assert count == 0


@pytest.mark.asyncio
async def test_fetch_and_store_entities_empty_sentiment_null(monkeypatch):
    """entities=[] → symbols=None, sentiment=None으로 저장."""
    svc = NewsCollectorService()
    articles = [_make_article("no-entities", entities=[])]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, mock_session = _make_mock_session(rowcount=1)
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "general"})

    assert count == 1
    # execute 호출 시 stmt에 symbols=None, sentiment=None 포함 확인
    call_args = mock_session.execute.call_args[0][0]
    compiled = call_args.compile()
    params = compiled.params
    assert params.get("symbols") is None
    assert params.get("sentiment_score") is None


# ── 라운드 로빈 ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_round_robin_query_index(monkeypatch):
    """3회 연속 호출 → query_index 0→1→2, 카테고리 순환."""
    from app.services.news_collector import _SEARCH_QUERIES
    svc = NewsCollectorService()
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    categories = []

    async def fake_fetch(query_config: dict) -> int:
        categories.append(query_config["category"])
        return 0

    svc._fetch_and_store = fake_fetch  # type: ignore[method-assign]

    # poll_loop 1 사이클을 직접 시뮬레이션
    for _ in range(3):
        query = _SEARCH_QUERIES[svc._query_index % len(_SEARCH_QUERIES)]
        await svc._fetch_and_store(query)
        svc._query_index += 1

    assert categories == ["crypto", "forex", "macro"]


# ── published_after 커서 ──────────────────────────────────────

@pytest.mark.asyncio
async def test_last_published_at_updated(monkeypatch):
    """2건 수집 후 _last_published_at = max(published_at) 갱신."""
    svc = NewsCollectorService()
    articles = [
        _make_article("b1", published_at="2026-04-09T10:00:00+00:00"),
        _make_article("b2", published_at="2026-04-09T10:30:00+00:00"),
    ]
    mock_client = _make_mock_httpx_client(articles)
    mock_session_local, _ = _make_mock_session(rowcount=1)
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        await svc._fetch_and_store({"category": "crypto"})

    expected = datetime(2026, 4, 9, 10, 30, 0, tzinfo=timezone.utc)
    assert svc._last_published_at == expected


# ── get_status ────────────────────────────────────────────────

def test_get_status_not_running(monkeypatch):
    """미기동 상태 status 확인."""
    svc = NewsCollectorService()
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    status = svc.get_status()
    assert status["running"] is False
    assert status["api_configured"] is True
    assert status["last_published_at"] is None


# ── published_at 파싱 ────────────────────────────────────────

@pytest.mark.asyncio
async def test_upsert_article_published_at_z_suffix_parsed(monkeypatch):
    """published_at 'Z' 접미사 문자열 → datetime으로 파싱되어 DB에 저장."""
    svc = NewsCollectorService()
    # Marketaux API 실제 응답 형식: "2022-01-25T22:45:01.000000Z"
    article = _make_article("z-suffix-uuid", published_at="2022-01-25T22:45:01.000000Z")
    mock_client = _make_mock_httpx_client([article])
    mock_session_local, mock_session = _make_mock_session(rowcount=1)
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "crypto"})

    assert count == 1
    call_args = mock_session.execute.call_args[0][0]
    compiled = call_args.compile()
    params = compiled.params
    # published_at이 datetime 객체여야 함 (문자열 아님)
    assert isinstance(params.get("published_at"), datetime), (
        f"published_at must be datetime, got {type(params.get('published_at'))}"
    )
    assert params["published_at"].tzinfo is not None


@pytest.mark.asyncio
async def test_upsert_article_published_at_offset_format_parsed(monkeypatch):
    """+00:00 오프셋 형식 published_at → datetime으로 정상 파싱."""
    svc = NewsCollectorService()
    article = _make_article("offset-uuid", published_at="2026-04-09T10:30:00+00:00")
    mock_client = _make_mock_httpx_client([article])
    mock_session_local, mock_session = _make_mock_session(rowcount=1)
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "forex"})

    assert count == 1
    call_args = mock_session.execute.call_args[0][0]
    compiled = call_args.compile()
    params = compiled.params
    assert isinstance(params.get("published_at"), datetime)
    expected = datetime(2026, 4, 9, 10, 30, 0, tzinfo=timezone.utc)
    assert params["published_at"] == expected


@pytest.mark.asyncio
async def test_upsert_article_published_at_none_uses_fallback(monkeypatch):
    """published_at 없음 → datetime.now(utc) fallback 사용, insert 성공."""
    svc = NewsCollectorService()
    article = _make_article("none-published-uuid")
    article.pop("published_at")  # 필드 제거
    mock_client = _make_mock_httpx_client([article])
    mock_session_local, mock_session = _make_mock_session(rowcount=1)
    monkeypatch.setenv("MARKETAUX_API_TOKEN", "test-token")
    from app.core.config import get_settings
    get_settings.cache_clear()

    with patch("httpx.AsyncClient", return_value=mock_client), \
         patch("app.services.news_collector.AsyncSessionLocal", mock_session_local):
        count = await svc._fetch_and_store({"category": "macro"})

    assert count == 1
    call_args = mock_session.execute.call_args[0][0]
    compiled = call_args.compile()
    params = compiled.params
    # fallback → datetime 객체 (문자열 아님)
    assert isinstance(params.get("published_at"), datetime)
