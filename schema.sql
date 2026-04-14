-- Run as the sxa user against market_news_analysis_db
-- psql -U sxa -d market_news_analysis_db -f schema.sql

CREATE TABLE IF NOT EXISTS market_news (
    id                   BIGSERIAL PRIMARY KEY,
    source               TEXT,
    title                TEXT,
    summary              TEXT,
    link                 TEXT NOT NULL UNIQUE,
    content              TEXT,
    published            TIMESTAMPTZ,
    timestamp            TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    score_raw            DOUBLE PRECISION,
    score                DOUBLE PRECISION,
    impact               TEXT,
    sentiment            DOUBLE PRECISION,
    sentiment_label      TEXT,
    categories           TEXT[],
    tickers              TEXT[],
    affected_indices     TEXT[],
    relevance_multiplier DOUBLE PRECISION,
    title_fingerprint    TEXT
);

CREATE INDEX IF NOT EXISTS idx_market_news_published   ON market_news (published DESC);
CREATE INDEX IF NOT EXISTS idx_market_news_ingested_at ON market_news (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_news_impact      ON market_news (impact);
CREATE INDEX IF NOT EXISTS idx_market_news_tickers     ON market_news USING GIN (tickers);
CREATE INDEX IF NOT EXISTS idx_market_news_indices     ON market_news USING GIN (affected_indices);
CREATE INDEX IF NOT EXISTS idx_market_news_categories  ON market_news USING GIN (categories);
