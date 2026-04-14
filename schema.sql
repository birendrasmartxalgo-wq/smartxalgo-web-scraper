-- ============================================================================
-- SmartxAlgo market news schema
-- Run as superuser (for extensions) or as sxa if extensions are pre-installed:
--   sudo -u postgres psql -d market_news_analysis_db -f schema.sql
-- ============================================================================

-- ---------- Extensions ------------------------------------------------------
-- pg_trgm: GIN trigram indexes on TEXT — required for fast ILIKE '%foo%'.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ---------- Main table ------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_news (
    id                   BIGSERIAL   PRIMARY KEY,
    source               TEXT        NOT NULL,
    title                TEXT        NOT NULL,
    summary              TEXT,
    link                 TEXT        NOT NULL UNIQUE,
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

-- ---------- Indexes ---------------------------------------------------------

-- 1. Unfiltered /feed: ORDER BY published DESC NULLS LAST LIMIT 20
--    Covering index -> index-only scan -> no heap fetch for the widget query.
CREATE INDEX IF NOT EXISTS idx_mn_published_desc
    ON market_news (published DESC NULLS LAST)
    INCLUDE (id, source, title, summary, categories, impact, sentiment_label);

-- 2. Filter + sort composites. Leading column is the (low-card) filter,
--    trailing column is the (high-card) sort — planner seeks then walks.
CREATE INDEX IF NOT EXISTS idx_mn_impact_published
    ON market_news (impact, published DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_mn_sentiment_published
    ON market_news (sentiment_label, published DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_mn_source_published
    ON market_news (source, published DESC NULLS LAST);

-- 3. GIN for array membership — $1 = ANY(col) / col @> ARRAY[...]
CREATE INDEX IF NOT EXISTS idx_mn_tickers_gin
    ON market_news USING GIN (tickers);

CREATE INDEX IF NOT EXISTS idx_mn_categories_gin
    ON market_news USING GIN (categories);

CREATE INDEX IF NOT EXISTS idx_mn_indices_gin
    ON market_news USING GIN (affected_indices);

-- 4. Trigram indexes for ILIKE '%foo%' search across title + summary.
--    These speed up substring search by 10–100x vs seq scan.
CREATE INDEX IF NOT EXISTS idx_mn_title_trgm
    ON market_news USING GIN (title gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_mn_summary_trgm
    ON market_news USING GIN (summary gin_trgm_ops);

-- 5. Retention / cleanup scans (WHERE ingested_at < NOW() - INTERVAL '30 days')
CREATE INDEX IF NOT EXISTS idx_mn_ingested_at
    ON market_news (ingested_at DESC);

-- ---------- Supporting lookup table -----------------------------------------
CREATE TABLE IF NOT EXISTS news_category_images (
    category   TEXT        PRIMARY KEY,
    image_url  TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed a few example rows (safe to re-run thanks to ON CONFLICT)
INSERT INTO news_category_images (category, image_url) VALUES
    ('Banking',        'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/banking.avif'),
    ('IT',             'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/it.avif'),
    ('Auto',           'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/auto.avif'),
    ('Pharma',         'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/pharma.avif'),
    ('Energy',         'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/energy.avif'),
    ('Metals',         'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/metals.avif'),
    ('FMCG',           'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/fmcg.avif'),
    ('Realty',         'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/realty.avif'),
    ('Telecom',        'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/telecom.avif'),
    ('Macro',          'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/macro.avif'),
    ('Geopolitical',   'https://YOUR_BUCKET.s3.amazonaws.com/market-news-images/geopolitical.avif')
ON CONFLICT (category) DO NOTHING;

-- ---------- Table/index stats ------------------------------------------------
-- Refresh planner stats immediately after bulk load:
-- ANALYZE market_news;
