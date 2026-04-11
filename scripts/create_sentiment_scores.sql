-- sentiment_scores 테이블 생성 + 인덱스
-- Alternative.me Fear & Greed Index 수집 결과 저장
-- 실행: psql trader_db -U trader -f create_sentiment_scores.sql

CREATE TABLE IF NOT EXISTS sentiment_scores (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)     NOT NULL,       -- "alternative_me_fng"
    score           INTEGER         NOT NULL,        -- 0~100
    classification  VARCHAR(30)     NOT NULL,        -- "Extreme Fear" | "Fear" | "Neutral" | "Greed" | "Extreme Greed"
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    raw_timestamp   BIGINT                           -- FNG API timestamp (Unix epoch)
);

CREATE INDEX IF NOT EXISTS idx_sentiment_scores_source_fetched
    ON sentiment_scores (source, fetched_at);

COMMENT ON TABLE  sentiment_scores                  IS 'Crypto Fear & Greed Index (Alternative.me FNG API)';
COMMENT ON COLUMN sentiment_scores.source           IS '수집 소스 식별자 — alternative_me_fng';
COMMENT ON COLUMN sentiment_scores.score            IS '0=Extreme Fear … 100=Extreme Greed';
COMMENT ON COLUMN sentiment_scores.classification   IS 'FNG API value_classification 원문';
COMMENT ON COLUMN sentiment_scores.raw_timestamp    IS 'FNG API 응답의 Unix timestamp (갱신 시각)';
