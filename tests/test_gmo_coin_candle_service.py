"""
GMO 코인 캔들 서비스 테스트.

DB/API 의존성 없이 순수 로직 + 소스 분석 테스트.
"""
import asyncio
import os
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch, call


# ── 소스 분석 헬퍼 ──────────────────────────────────────────────

def _read_source(rel_path: str) -> str:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(base, rel_path), "r") as f:
        return f.read()


# ── 소스 분석 테스트 ────────────────────────────────────────────

class TestGmoCoinCandleServiceSource:
    """소스 코드 구조 검증 (import 없이)."""

    def test_no_price_type_param(self):
        """get_klines 호출에 price_type 파라미터 없음 (GMO Coin은 단일 가격)."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert "price_type" not in src

    def test_volume_from_kline_not_zero(self):
        """volume을 KLine 응답에서 읽음 (Decimal("0") 하드코딩 없음)."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert 'Decimal("0")' not in src
        assert 'Decimal(kl["volume"])' in src

    def test_constraint_gmoc_candles_pkey(self):
        """on_conflict_do_update에 gmoc_candles_pkey 사용."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert '"gmoc_candles_pkey"' in src

    def test_pair_to_kline_symbol_mapping(self):
        """btc_jpy→BTC 심볼 변환 매핑 정의."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert '"btc_jpy": "BTC"' in src
        assert '"eth_jpy": "ETH"' in src

    def test_backfill_default_180(self):
        """기본 백필 일수 180일."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert "days: int = 180" in src

    def test_4h_uses_year_date_format(self):
        """4H 백필이 연도(%Y) 포맷 사용."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert '"4h": "%Y"' in src or "_DATE_FORMAT_MAP" in src

    def test_1h_uses_yyyymmdd_date_format(self):
        """1H 백필이 YYYYMMDD 포맷 사용."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert '"%Y%m%d"' in src

    def test_recover_stale_candles_exists(self):
        """recover_stale_candles 메서드 정의."""
        src = _read_source("app/services/gmo_coin_candle_service.py")
        assert "async def recover_stale_candles" in src


class TestGmoCoinPublicClientSource:
    """gmo_coin_public_client.py 소스 검증."""

    def test_no_price_type_in_get_klines(self):
        """get_klines 메서드의 params dict에 priceType/price_type 없음."""
        src = _read_source("app/services/gmo_coin_public_client.py")
        # params dict 블록 추출 (함수 본문만)
        params_start = src.find('params = {')
        params_end = src.find('}', params_start) + 1
        params_block = src[params_start:params_end]
        assert "priceType" not in params_block
        assert "price_type" not in params_block

    def test_base_url_coin_not_forex(self):
        """Base URL이 api.coin.z.com (GMO FX의 forex-api와 구분)."""
        src = _read_source("app/services/gmo_coin_public_client.py")
        assert "api.coin.z.com" in src
        # 설정값/실제 URL에 forex-api 없음 (docstring 제외)
        code_section = src[src.find("class GmoCoinPublicClient"):]
        assert "forex-api" not in code_section

    def test_close_function_exists(self):
        """close_gmo_coin_public_client 팩토리 함수 존재."""
        src = _read_source("app/services/gmo_coin_public_client.py")
        assert "async def close_gmo_coin_public_client" in src


class TestGmoCoinCandlePipelineSource:
    """gmo_coin_candle_pipeline.py 소스 검증."""

    def test_poll_interval_300(self):
        """폴링 주기 300초 (5분)."""
        src = _read_source("app/services/gmo_coin_candle_pipeline.py")
        assert "_POLL_INTERVAL = 300" in src

    def test_two_tasks_per_pair(self):
        """pair당 2개 태스크 (백필 + 폴 워커)."""
        src = _read_source("app/services/gmo_coin_candle_pipeline.py")
        assert "gmoc_candle_backfill" in src
        assert "gmoc_candle_poll" in src

    def test_stop_all_exists(self):
        """stop_all 메서드 존재."""
        src = _read_source("app/services/gmo_coin_candle_pipeline.py")
        assert "async def stop_all" in src


# ── 로직 테스트 (mock 기반) ─────────────────────────────────────

class TestGmoCoinCandleServiceLogic:
    """GmoCoinCandleService 로직 테스트 (DB/API mock)."""

    @pytest.fixture
    def service(self):
        """DB와 API를 mock한 서비스 인스턴스."""
        from app.services.gmo_coin_candle_service import GmoCoinCandleService
        return GmoCoinCandleService()

    def test_to_kline_symbol_btc(self, service):
        """btc_jpy → BTC 변환."""
        assert service._to_kline_symbol("btc_jpy") == "BTC"

    def test_to_kline_symbol_eth(self, service):
        """eth_jpy → ETH 변환."""
        assert service._to_kline_symbol("ETH_JPY") == "ETH"

    def test_to_kline_symbol_fallback(self, service):
        """매핑 없는 페어 → 앞부분 대문자 fallback."""
        symbol = service._to_kline_symbol("aaa_jpy")
        assert symbol == "AAA"

    def test_to_kline_symbol_case_insensitive(self, service):
        """대소문자 무관."""
        assert service._to_kline_symbol("BTC_JPY") == "BTC"
        assert service._to_kline_symbol("btc_jpy") == "BTC"

    @pytest.mark.asyncio
    async def test_poll_and_upsert_empty_response(self, service):
        """KLine 응답이 비어있으면 0 반환."""
        mock_client = AsyncMock()
        mock_client.get_klines.return_value = []

        with patch(
            "app.services.gmo_coin_candle_service.get_gmo_coin_public_client",
            return_value=mock_client,
        ):
            count = await service.poll_and_upsert("btc_jpy", "4h")

        assert count == 0
        mock_client.get_klines.assert_called_once()
        # price_type 없이 호출됐는지 확인
        call_kwargs = mock_client.get_klines.call_args.kwargs
        assert "price_type" not in call_kwargs

    @pytest.mark.asyncio
    async def test_poll_and_upsert_no_price_type_arg(self, service):
        """poll_and_upsert: get_klines에 price_type를 전달하지 않음."""
        mock_client = AsyncMock()
        mock_client.get_klines.return_value = []

        with patch(
            "app.services.gmo_coin_candle_service.get_gmo_coin_public_client",
            return_value=mock_client,
        ):
            await service.poll_and_upsert("btc_jpy", "4h")

        _, call_kwargs = mock_client.get_klines.call_args
        assert "price_type" not in call_kwargs

    @pytest.mark.asyncio
    async def test_backfill_4h_calls_two_years(self, service):
        """4H 백필: 올해 + 작년 2회 API 호출."""
        mock_client = AsyncMock()
        mock_client.get_klines.return_value = []

        with patch(
            "app.services.gmo_coin_candle_service.get_gmo_coin_public_client",
            return_value=mock_client,
        ):
            await service.backfill("btc_jpy", "4h")

        assert mock_client.get_klines.call_count == 2
        # date 파라미터가 연도 문자열
        dates_called = [c.kwargs["date"] for c in mock_client.get_klines.call_args_list]
        for d in dates_called:
            assert len(d) == 4  # "2026", "2025" 형식


class TestGmoCoinConfigSource:
    """config.py GMO Coin 설정 검증."""

    def test_gmo_coin_base_url_defined(self):
        src = _read_source("app/core/config.py")
        assert "GMO_COIN_BASE_URL" in src

    def test_gmo_coin_pairs_defined(self):
        src = _read_source("app/core/config.py")
        assert "GMO_COIN_PAIRS" in src

    def test_gmo_coin_pairs_list_property(self):
        src = _read_source("app/core/config.py")
        assert "gmo_coin_pairs_list" in src

    def test_gmo_coin_pairs_list_returns_list(self):
        """Settings.gmo_coin_pairs_list 동작 검증."""
        import os
        os.environ["GMO_COIN_PAIRS"] = "BTC_JPY,ETH_JPY"
        os.environ["DATABASE_URL"] = "postgresql+asyncpg://x:y@localhost/z"
        from app.core import config as cfg_mod
        # 캐시 우회
        cfg_mod.get_settings.cache_clear()
        settings = cfg_mod.Settings(GMO_COIN_PAIRS="btc_jpy, eth_jpy")
        result = settings.gmo_coin_pairs_list
        assert result == ["BTC_JPY", "ETH_JPY"]

    def test_gmo_coin_pairs_empty_string_returns_empty(self):
        """GMO_COIN_PAIRS가 빈 문자열이면 빈 리스트."""
        from app.core.config import Settings
        s = Settings(GMO_COIN_PAIRS="")
        assert s.gmo_coin_pairs_list == []


class TestGmocCandleDbModel:
    """GmocCandle ORM 모델 검증."""

    def test_gmoc_candle_tablename(self):
        src = _read_source("app/models/database.py")
        assert '__tablename__ = "gmoc_candles"' in src

    def test_gmoc_candle_pkey_name(self):
        src = _read_source("app/models/database.py")
        assert '"gmoc_candles_pkey"' in src

    def test_gmoc_candle_has_volume_without_default_zero(self):
        """volume 컬럼: GMO FX와 달리 volume에 default=0 없음 (실거래량 저장)."""
        src = _read_source("app/models/database.py")
        gmoc_start = src.find("class GmocCandle")
        economic_start = src.find("class EconomicEvent")
        gmoc_section = src[gmoc_start:economic_start]
        assert "volume" in gmoc_section
        # volume 줄에 default=0 없어야 함 (tick_count는 default=0 있으나 volume에는 없어야 함)
        for line in gmoc_section.splitlines():
            if "volume" in line and "Column" in line:
                assert "default=0" not in line, f"volume 컬럼에 default=0 존재: {line}"


class TestMainPyGmoCoinIntegration:
    """main.py에 GMO Coin 파이프라인 통합 검증."""

    def test_gmo_coin_candle_pipeline_startup(self):
        src = _read_source("app/main.py")
        assert "gmo_coin_candle_pipeline" in src
        assert "gmo_coin_pairs_list" in src

    def test_gmo_coin_pipeline_shutdown(self):
        src = _read_source("app/main.py")
        assert "stop_all" in src
        # shutdown 섹션에 gmo_coin_candle_pipeline 있는지
        shutdown_start = src.find("# ── Shutdown")
        assert "gmo_coin_candle_pipeline" in src[shutdown_start:]

    def test_gmo_coin_router_registered(self):
        src = _read_source("app/main.py")
        assert "gmo_coin_candles_router" in src
        assert "app.include_router(gmo_coin_candles_router.router)" in src

    def test_close_gmo_coin_public_client_in_shutdown(self):
        src = _read_source("app/main.py")
        assert "close_gmo_coin_public_client" in src


# ── 엣지 케이스: Public Client HTTP/비즈니스 에러 ──────────────

class TestGmoCoinPublicClientEdgeCases:
    """GmoCoinPublicClient 에러 처리 엣지케이스."""

    @pytest.mark.asyncio
    async def test_get_klines_status_nonzero_returns_empty(self):
        """KLine API status≠0 → 빈 리스트 반환."""
        from app.services.gmo_coin_public_client import GmoCoinPublicClient

        client = GmoCoinPublicClient()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(return_value={"status": -1, "messages": [{"message_code": "ERR-001"}]})
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        result = await client.get_klines("BTC", "4hour", "2026")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_klines_http_error_returns_empty(self):
        """KLine API HTTP 에러 → 빈 리스트, 크래시 없음."""
        import httpx
        from app.services.gmo_coin_public_client import GmoCoinPublicClient

        client = GmoCoinPublicClient()
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.ConnectError("timeout"))
        client._client = mock_http

        result = await client.get_klines("BTC", "4hour", "2026")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_ticker_status_nonzero_returns_empty_dict(self):
        """Ticker API status≠0 → 빈 dict 반환."""
        from app.services.gmo_coin_public_client import GmoCoinPublicClient

        client = GmoCoinPublicClient()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(return_value={"status": -1})
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        result = await client.get_ticker("BTC")
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_ticker_empty_data_returns_empty_dict(self):
        """Ticker API data=[] → 빈 dict 반환."""
        from app.services.gmo_coin_public_client import GmoCoinPublicClient

        client = GmoCoinPublicClient()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(return_value={"status": 0, "data": []})
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        result = await client.get_ticker("BTC")
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_ticker_dict_data_returned_as_is(self):
        """Ticker API data가 dict인 경우 그대로 반환."""
        from app.services.gmo_coin_public_client import GmoCoinPublicClient

        client = GmoCoinPublicClient()
        ticker_data = {"symbol": "BTC", "last": "10000000"}
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(return_value={"status": 0, "data": ticker_data})
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        client._client = mock_http

        result = await client.get_ticker("BTC")
        assert result == ticker_data

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        """close 중복 호출 → 에러 없음."""
        from app.services.gmo_coin_public_client import GmoCoinPublicClient

        client = GmoCoinPublicClient()
        mock_http = AsyncMock()
        mock_http.aclose = AsyncMock()
        client._client = mock_http

        await client.close()
        await client.close()  # 두 번째 호출 — 에러 없어야 함
        assert client._client is None


# ── 엣지 케이스: CandleService DB mock ─────────────────────────

class TestGmoCoinCandleServiceDbMock:
    """GmoCoinCandleService DB mock 기반 테스트 (설계서 케이스 #1, #6)."""

    def _make_mock_session(self):
        """DB session mock (AsyncSessionLocal 대체)."""
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        return MagicMock(return_value=mock_session), mock_session

    def _make_klines(self, count: int = 3, volume: str = "1.234") -> list[dict]:
        """테스트용 KLine 데이터."""
        base_ms = 1618588800000  # 2021-04-16 UTC
        interval_ms = 4 * 60 * 60 * 1000  # 4H
        return [
            {
                "openTime": str(base_ms + i * interval_ms),
                "open": "6000000",
                "high": "6100000",
                "low": "5900000",
                "close": "6050000",
                "volume": volume,
            }
            for i in range(count)
        ]

    @pytest.mark.asyncio
    async def test_poll_and_upsert_returns_kline_count(self):
        """설계서 케이스 #1: 정상 KLine 3건 → upsert count=3 반환."""
        from app.services.gmo_coin_candle_service import GmoCoinCandleService

        svc = GmoCoinCandleService()
        mock_client = AsyncMock()
        mock_client.get_klines = AsyncMock(return_value=self._make_klines(3))
        mock_session_local, _ = self._make_mock_session()

        with patch(
            "app.services.gmo_coin_candle_service.get_gmo_coin_public_client",
            return_value=mock_client,
        ), patch(
            "app.services.gmo_coin_candle_service.AsyncSessionLocal",
            mock_session_local,
        ):
            count = await svc.poll_and_upsert("btc_jpy", "4h")

        assert count == 3

    @pytest.mark.asyncio
    async def test_volume_passed_to_upsert_not_zero(self):
        """설계서 케이스 #6: volume이 0이 아닌 실제 값(Decimal("1.234"))으로 UPSERT."""
        from decimal import Decimal
        from app.services.gmo_coin_candle_service import GmoCoinCandleService

        svc = GmoCoinCandleService()
        mock_client = AsyncMock()
        mock_client.get_klines = AsyncMock(return_value=self._make_klines(1, volume="1.234"))
        mock_session_local, mock_session = self._make_mock_session()

        # pg_insert mock: stmt.values()가 호출되는 인자를 캡처
        captured_values = {}

        original_pg_insert = None
        from sqlalchemy.dialects.postgresql import insert as pg_insert_orig

        def mock_pg_insert(model):
            stmt_mock = MagicMock()
            def capture_values(**kwargs):
                captured_values.update(kwargs)
                inner = MagicMock()
                inner.on_conflict_do_update = MagicMock(return_value=inner)
                return inner
            stmt_mock.values = MagicMock(side_effect=capture_values)
            return stmt_mock

        with patch(
            "app.services.gmo_coin_candle_service.get_gmo_coin_public_client",
            return_value=mock_client,
        ), patch(
            "app.services.gmo_coin_candle_service.AsyncSessionLocal",
            mock_session_local,
        ), patch(
            "app.services.gmo_coin_candle_service.pg_insert",
            side_effect=mock_pg_insert,
        ):
            await svc.poll_and_upsert("btc_jpy", "4h")

        assert "volume" in captured_values
        assert captured_values["volume"] == Decimal("1.234")
        assert captured_values["volume"] != Decimal("0")

    @pytest.mark.asyncio
    async def test_poll_and_upsert_kline_missing_field_skipped(self):
        """KeyError 있는 KLine 항목은 건너뛰고, 나머지는 정상 처리."""
        from app.services.gmo_coin_candle_service import GmoCoinCandleService

        svc = GmoCoinCandleService()
        # 첫 번째: 'volume' 필드 누락 → KeyError → skip
        # 두 번째: 정상
        bad_kline = {"openTime": "1618588800000", "open": "6000000", "high": "6100000", "low": "5900000", "close": "6050000"}
        good_klines = self._make_klines(2, "0.5")
        klines = [bad_kline, *good_klines]

        mock_client = AsyncMock()
        mock_client.get_klines = AsyncMock(return_value=klines)
        mock_session_local, _ = self._make_mock_session()

        with patch(
            "app.services.gmo_coin_candle_service.get_gmo_coin_public_client",
            return_value=mock_client,
        ), patch(
            "app.services.gmo_coin_candle_service.AsyncSessionLocal",
            mock_session_local,
        ):
            # 크래시 없이 처리 (bad_kline skip, good 2건 처리)
            count = await svc.poll_and_upsert("btc_jpy", "4h")

        assert count == 2  # bad_kline 1건 skip

    @pytest.mark.asyncio
    async def test_backfill_1h_rate_limit_sleep(self):
        """1H 백필: days번 API 호출 + asyncio.sleep 호출 확인."""
        from app.services.gmo_coin_candle_service import GmoCoinCandleService

        svc = GmoCoinCandleService()
        mock_client = AsyncMock()
        mock_client.get_klines = AsyncMock(return_value=[])  # 빈 응답으로 빠르게

        with patch(
            "app.services.gmo_coin_candle_service.get_gmo_coin_public_client",
            return_value=mock_client,
        ), patch("app.services.gmo_coin_candle_service.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await svc.backfill("btc_jpy", timeframe="1h", days=5)

        # days=5번 sleep 호출 (레이트 리밋 배려)
        assert mock_sleep.call_count == 5
        # API는 5번 호출
        assert mock_client.get_klines.call_count == 5


# ── 엣지 케이스: Pipeline ──────────────────────────────────────

class TestGmoCoinCandlePipelineEdgeCases:
    """GmoCoinCandlePipeline 엣지케이스."""

    def test_pipeline_is_running_false_initially(self):
        """시작 전 is_running=False."""
        from app.services.gmo_coin_candle_pipeline import GmoCoinCandlePipeline

        p = GmoCoinCandlePipeline()
        assert p.is_running("btc_jpy") is False

    def test_pipeline_running_pairs_empty_initially(self):
        """시작 전 running_pairs=[]."""
        from app.services.gmo_coin_candle_pipeline import GmoCoinCandlePipeline

        p = GmoCoinCandlePipeline()
        assert p.running_pairs() == []

    @pytest.mark.asyncio
    async def test_pipeline_stop_nonexistent_pair_no_error(self):
        """없는 pair stop 호출 → pop(key, [])으로 조용히 처리."""
        from app.services.gmo_coin_candle_pipeline import GmoCoinCandlePipeline

        p = GmoCoinCandlePipeline()
        # 시작도 안 한 pair를 stop — 에러 없어야 함
        await p.stop("nonexistent_pair")

    def test_pipeline_idempotent_restart_in_source(self):
        """소스 확인: start() 시 이미 실행 중이면 재시작 로직 존재."""
        src = _read_source("app/services/gmo_coin_candle_pipeline.py")
        # "이미 실행 중 → 재시작" 로직
        assert "이미 실행 중" in src or "in self._tasks" in src


# ── 엣지 케이스: Route 검증 ────────────────────────────────────

class TestGmoCoinRouteValidation:
    """gmo_coin_candles.py route 검증."""

    def test_route_prefix(self):
        src = _read_source("app/routes/gmo_coin_candles.py")
        assert 'prefix="/api/gmo-coin/candles"' in src

    def test_route_valid_pairs_btc_jpy(self):
        src = _read_source("app/routes/gmo_coin_candles.py")
        assert '"BTC_JPY"' in src

    def test_route_valid_pairs_sui_jpy(self):
        """설계서 12번 페어 SUI_JPY 포함."""
        src = _read_source("app/routes/gmo_coin_candles.py")
        assert '"SUI_JPY"' in src

    def test_route_validate_pair_tf_invalid_pair_raises(self):
        """_validate_pair_tf: 미지원 pair → HTTPException 400."""
        from fastapi import HTTPException
        from app.routes.gmo_coin_candles import _validate_pair_tf

        with pytest.raises(HTTPException) as exc_info:
            _validate_pair_tf("INVALID_PAIR", "4h")
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail["blocked_code"] == "INVALID_PAIR"

    def test_route_validate_pair_tf_invalid_timeframe_raises(self):
        """_validate_pair_tf: 미지원 timeframe → HTTPException 400."""
        from fastapi import HTTPException
        from app.routes.gmo_coin_candles import _validate_pair_tf

        with pytest.raises(HTTPException) as exc_info:
            _validate_pair_tf("BTC_JPY", "3h")
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail["blocked_code"] == "INVALID_TIMEFRAME"

    def test_route_validate_valid_pair_and_timeframe_no_exception(self):
        """_validate_pair_tf: 유효한 pair+timeframe → 예외 없음."""
        from app.routes.gmo_coin_candles import _validate_pair_tf

        _validate_pair_tf("BTC_JPY", "4h")  # 예외 없어야 함
        _validate_pair_tf("SOL_JPY", "1h")

    def test_route_status_endpoint_checks_valid_pair(self):
        """status 엔드포인트: 미지원 pair → HTTPException."""
        src = _read_source("app/routes/gmo_coin_candles.py")
        # status 엔드포인트에서 _VALID_PAIRS 검증 로직
        assert "INVALID_PAIR" in src

    def test_route_gmo_coin_not_gmo_fx(self):
        """route는 GmocCandle, get_gmo_coin_candle_pipeline 사용 (GMO FX 것 아님)."""
        src = _read_source("app/routes/gmo_coin_candles.py")
        assert "GmocCandle" in src
        assert "get_gmo_coin_candle_pipeline" in src
        # GMO FX 클래스는 사용 안 함
        assert "GmoCandle" not in src.replace("GmocCandle", "")


# ── handle_api_errors HTTPException 처리 버그 수정 검증 ─────────

class TestHandleApiErrorsHttpException:
    """handle_api_errors 데코레이터가 HTTPException을 re-raise 하는지 검증."""

    def test_handle_api_errors_reraises_http_exception(self):
        """HTTPException은 except로 잡히지 않고 re-raise 되어야 함."""
        src = _read_source("app/core/error_handlers.py")
        assert "except HTTPException" in src
        assert "raise  # FastAPI" in src or "raise  " in src

    @pytest.mark.asyncio
    async def test_handle_api_errors_400_not_converted_to_500(self):
        """@handle_api_errors 내부에서 HTTPException(400) 발생 → 400 유지."""
        from fastapi import HTTPException as _HTTPException
        from app.core.error_handlers import handle_api_errors

        @handle_api_errors("test_op")
        async def _raise_400():
            raise _HTTPException(400, {"blocked_code": "TEST"})

        with pytest.raises(_HTTPException) as exc_info:
            await _raise_400()

        assert exc_info.value.status_code == 400
