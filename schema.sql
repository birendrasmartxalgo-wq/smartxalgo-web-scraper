-- ============================================================================
-- SmartxAlgo market news schema — minimal write-optimized indexing
--
-- Run:
--   PGPASSWORD='sxa@2025' psql -h 127.0.0.1 -U sxa \
--     -d market_news_analysis_db -f schema.sql
-- ============================================================================

-- ---------- Main table ------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_news (
    id                   BIGSERIAL   PRIMARY KEY,
    source               TEXT        NOT NULL,
    title                TEXT        NOT NULL,
    summary              TEXT,
    link                 TEXT        NOT NULL UNIQUE,   -- required by ON CONFLICT upsert
    content              TEXT,
    published            TIMESTAMPTZ,
    timestamp            TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    score_raw            REAL,
    score                REAL,
    impact               TEXT        NOT NULL DEFAULT 'LOW',
    sentiment            REAL,
    sentiment_label      TEXT,
    categories           TEXT[]      NOT NULL DEFAULT '{}',
    tickers              TEXT[]      NOT NULL DEFAULT '{}',
    affected_indices     TEXT[]      NOT NULL DEFAULT '{}',
    relevance_multiplier REAL,
    title_fingerprint    CHAR(32)
);

-- ---------- Indexes (minimal, write-friendly) -------------------------------
-- PK on id and UNIQUE on link are created by the table definition above.

-- Composite: serves both  WHERE impact=... ORDER BY published DESC
-- AND the unfiltered /feed ORDER BY published DESC LIMIT 20 (bitmap/merge scan).
CREATE INDEX IF NOT EXISTS idx_mn_impact_published
    ON market_news (impact, published DESC NULLS LAST);

-- GIN for  $1 = ANY(tickers)
CREATE INDEX IF NOT EXISTS idx_mn_tickers_gin
    ON market_news USING GIN (tickers);

-- GIN for  $1 = ANY(categories)
CREATE INDEX IF NOT EXISTS idx_mn_categories_gin
    ON market_news USING GIN (categories);

-- ---------- Supporting lookup table -----------------------------------------
CREATE TABLE IF NOT EXISTS news_category_images (
    category   TEXT        PRIMARY KEY,
    image_url  TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO news_category_images (category, image_url) VALUES
    ('Banking',      'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/banking.avif'),
    ('IT',           'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/it.avif'),
    ('Auto',         'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/auto.avif'),
    ('Pharma',       'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/pharma.avif'),
    ('Energy',       'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/energy.avif'),
    ('Metals',       'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/metals.avif'),
    ('FMCG',         'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/fmcg.avif'),
    ('Realty',       'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/realty.avif'),
    ('Telecom',      'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/telecom.avif'),
    ('Macro',        'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/macro.avif'),
    ('Geopolitical', 'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/geopolitical.avif')
ON CONFLICT (category) DO NOTHING;

-- Refresh planner stats after first bulk load:
-- ANALYZE market_news;
