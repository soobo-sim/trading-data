# CoinMarket Data

Coincheck / BitFlyer 시장 데이터 수집·배포 독립 서비스 (port **8002**)

## 역할

| 기능 | 설명 |
|------|------|
| 공개 REST 프록시 | Coincheck / BitFlyer 시세·오더북·체결 |
| WebSocket 집계 | 실시간 틱 수집 → 캔들 생성 (CK/BF) |
| KLine 폴링 | GMO FX 캔들 수집 (5분 주기) |
| 캔들 저장 | `ck_candles`, `bf_candles`, `gmo_candles` |
| Market Pulse REST | 최근 틱 기반 시장 강도 지표 |

이 서비스가 캔들 데이터를 전담하므로, `coincheck-trader`와 `bitflyer-trader`는 캔들 관련 WS·서비스를 직접 운용하지 않습니다.

---

## 엔드포인트

### Coincheck

| 경로 | 설명 |
|------|------|
| `GET /api/ck/ticker` | 시세 |
| `GET /api/ck/order_books` | 오더북 |
| `GET /api/ck/trades` | 최근 체결 |
| `GET /api/ck/rate/{pair}` | 환율 |
| `GET /api/ck/exchange_status` | 거래소 상태 |
| `GET /api/ck/ws/status` | WS 연결 상태 |
| `GET /api/ck/ws/market-pulse` | 시장 강도 지표 |
| `GET /api/ck/ws/recent-trades` | 최근 WS 체결 |
| `GET /api/ck/candles/{pair}/status` | 캔들 파이프라인 상태 |
| `GET /api/ck/candles/{pair}/{timeframe}` | 완성된 캔들 목록 |
| `GET /api/ck/candles/{pair}/{timeframe}/current` | 진행 중 캔들 |
| `GET /api/ck/candles/{pair}/{timeframe}/rsi` | RSI 산출 |

### BitFlyer

| 경로 | 설명 |
|------|------|
| `GET /api/bf/ticker` | 시세 |
| `GET /api/bf/order_books` | 오더북 |
| `GET /api/bf/trades` | 최근 체결 |
| `GET /api/bf/exchange_status` | 거래소 상태 |
| `GET /api/bf/ws/status` | WS 연결 상태 |
| `GET /api/bf/ws/market-pulse` | 시장 강도 지표 |
| `GET /api/bf/ws/recent-trades` | 최근 WS 체결 |
| `GET /api/bf/funding-rate` | FX_BTC_JPY 최신 펀딩레이트 |
| `GET /api/bf/funding-rate/history` | 펀딩레이트 수집 이력 |

### GMO FX

| 경로 | 설명 |
|------|------|
| `GET /api/gmo/candles/{pair}/status` | GMO FX 캔들 파이프라인 상태 |
| `GET /api/gmo/candles/{pair}/{timeframe}` | GMO FX 완성된 캔들 목록 |

### 인프라 공통

| 경로 | 설명 |
|------|------|
| `GET /api/status` | 전체 수집 현황 — CK/BF/GMO 캔들·WS·펀딩레이트 폴러 상태 (레이첼 분석 전 사전 확인용) |

---

## 시작 순서

coinmarket-data가 **PostgreSQL과 `trader-network`를 제공**합니다. 반드시 먼저 기동하세요.

```bash
# 1. coinmarket-data 먼저 기동 (PostgreSQL + trader-network 제공)
cd coinmarket-data && docker-compose up -d --build

# 2. 테이블 마이그레이션 (bf_candles, gmo_candles 등)
docker exec coinmarket-data alembic upgrade head

# 3. trading-engine (CK + BF + GMO FX)
cd trading-engine && docker-compose up -d --build
```

---

## 환경 변수

```bash
cp .env.example .env
# DATABASE_URL, COINCHECK_BASE_URL, BITFLYER_BASE_URL, CK_WS_PAIRS 등 설정
# GMO FX: GMOFX_BASE_URL, GMO_FX_PAIRS=USD_JPY (쉼표 구분)
```

---

## 구조

```
app/
├── core/        # config, exceptions, error_handlers
├── models/      # CkCandle, BfCandle, GmoCandle ORM
├── services/
│   ├── ck_public_client.py   # Coincheck REST
│   ├── ck_ws_client.py       # Coincheck Public WS
│   ├── bf_public_client.py   # BitFlyer REST
│   ├── bf_ws_client.py       # BitFlyer Public WS
│   ├── gmo_public_client.py  # GMO FX REST (KLine/Ticker)
│   ├── gmo_candle_service.py # GMO FX 캔들 UPSERT
│   ├── gmo_candle_pipeline.py # GMO FX 파이프라인 (5분 폴링)
│   ├── ck_market_pulse.py    # CK 시장 강도
│   ├── bf_market_pulse.py    # BF 시장 강도
│   ├── candle_service.py     # ck_candles DB 읽기/쓰기
│   └── candle_pipeline.py    # CK 캔들 집계 오케스트레이터
├── routes/
│   ├── ck_market.py   # /api/ck/* 시장 데이터
│   ├── bf_market.py   # /api/bf/* 시장 데이터
│   ├── gmo_candles.py # /api/gmo/candles/* GMO FX 캔들
│   └── candles.py     # /api/ck/candles/*
└── main.py            # lifespan: WS + CandlePipeline
```

---

## DB 테이블

| 테이블 | 마이그레이션 | 설명 |
|--------|-------------|------|
| `ck_candles` | coincheck-trader (기존) | CK 캔들 |
| `bf_candles` | coinmarket-data `001` | BF 캔들 |

Alembic 버전 테이블: `alembic_version_cmd` (coincheck-trader의 `alembic_version_ck`와 독립)
