-- F-04 인터마켓 데이터 테이블 생성
-- 실행: docker exec trader-postgres psql -U trader -d trader_db -f /tmp/create_intermarket.sql

CREATE TABLE IF NOT EXISTS intermarket_data (
    id SERIAL PRIMARY KEY,
    series_id VARCHAR(30) NOT NULL,
    obs_date TIMESTAMP NOT NULL,
    value NUMERIC(12, 4),
    source VARCHAR(20) NOT NULL DEFAULT 'fred',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_intermarket_series_date UNIQUE (series_id, obs_date)
);

CREATE INDEX IF NOT EXISTS idx_intermarket_series_date
    ON intermarket_data (series_id, obs_date DESC);
