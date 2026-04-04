-- Alpha Factors Phase 1: economic_events 테이블 생성 (F-01 경제 캘린더)
-- 실행: docker exec trader-postgres psql -U trader -d trader_db -f /tmp/create_economic_events.sql

CREATE TABLE IF NOT EXISTS economic_events (
    id SERIAL PRIMARY KEY,
    title VARCHAR NOT NULL,
    country VARCHAR(3) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    impact VARCHAR(10) NOT NULL,
    forecast VARCHAR(50),
    previous VARCHAR(50),
    actual VARCHAR(50),
    source VARCHAR(20) NOT NULL DEFAULT 'forexfactory',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_economic_events_title_time UNIQUE (title, event_time)
);

CREATE INDEX IF NOT EXISTS idx_economic_events_time ON economic_events(event_time);
CREATE INDEX IF NOT EXISTS idx_economic_events_country ON economic_events(country);
