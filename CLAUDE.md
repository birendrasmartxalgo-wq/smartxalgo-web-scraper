## CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Run / Develop

```bash
pip install -r requirements.txt
python app.py                      # starts Flask + SocketIO on PORT (default 5000)
```

There is no test suite, linter config, or build step. Deployment is Render (`render.yaml`, Python 3.11.6, `python app.py`).

First run downloads ProsusAI/finbert (~440 MB) into `.hf-cache/`. The model is loaded eagerly at import time via `_bootstrap_seen_from_disk()` at the bottom of `scraper.py`, so importing `scraper` is slow on a cold start — this is intentional so the first scrape cycle isn't penalized.

Optional Twitter env vars (used by `twscrape`): `TW_USERNAME`, `TW_PASSWORD`, `TW_EMAIL`, `TW_EMAIL_PASSWORD`, `TW_DB_PATH`. If `twscrape` isn't available or no creds are set, the scraper falls back to a guest-token path (`_fetch_twitter_via_guest_token`).

## Architecture

Two-process-in-one-process design: Flask serves HTTP/WebSocket while a daemon thread runs the scraper loop.

- `app.py` — thin Flask + Flask-SocketIO shell. Spawns `background_scraper(socketio)` in a daemon thread on startup. Routes: `GET /scrape` (force a fetch + return all), `GET /news` (read cached JSON), `/` and `/<path>` serve a `frontend/` static dir (note: not currently in the repo). Uses `async_mode="threading"` and `allow_unsafe_werkzeug=True` — the WSGI server is dev-grade.
- `scraper.py` — everything else. ~1300 lines, single module by design.
- `scraper_sentiment.py` — FinBERT singleton wrapper. `score_text(text) -> (compound, label)`. Replaces VADER because financial domain language ("rate cut", "FII selling") was being misclassified.
- `market_news.json` — the canonical store. There is no database. `save_news()` does an atomic read-merge-write under `_file_lock`, with corrupt-file quarantine (`.corrupt-<timestamp>`) instead of silent loss.

### Scraper pipeline

`background_scraper()` loops every 60s and runs `fetch_news()` → `enrich_items()` → `save_news()` → `socketio.emit("news_update", ...)`. After 5 consecutive failures it backs off for 5 minutes.

1. **fetch_news()** — fans out across sources concurrently via `ThreadPoolExecutor`. Sources: `fetch_moneycontrol_html`, `fetch_reuters_news`, `fetch_twitter_news`, plus everything in `RSS_FEEDS` (ET, CNBC, Bloomberg, AlJazeera, NYTimes, Reddit). Moneycontrol and Reuters are intentionally excluded from `RSS_FEEDS` because they have dedicated fetchers. Items older than `MAX_ITEM_AGE_DAYS` (15) are dropped here.
2. **enrich_items()** — for each item, fetches the full article body in parallel (`EXTRACT_WORKERS=16`, 12s timeout). Hosts in `NO_EXTRACT_HOSTS` (Bloomberg/NYT/FT/WSJ paywalls) are skipped; hosts in `RSS_INLINE_HOSTS` (Reddit) reuse the RSS body. Extraction tries trafilatura → BS4 → falls back to cleaned RSS summary. Then **rescores everything** using the richest available text.
3. **Scoring** is the heart of the file and has two distinct outputs per item:
   - `score` / `score_raw` / `impact` — keyword-based. `get_news_score()` matches against `BULLISH/BEARISH/FINANCIAL/GEOPOLITICAL/KEYWORDS/HIGH_IMPACT` regex sets compiled by `_compile_word_set` (word-boundary, case-insensitive). Each category is capped at `MAX_HITS_PER_CATEGORY=3` to defeat repetition spam ("war war war war"). `HIGH_IMPACT` and `GEOPOLITICAL` hits only count when a `HARD_ANCHORS` term is also present — this prevents pure-geopolitics articles (Houthis, synagogue) from leaking through. The raw score is then multiplied by `SOURCE_WEIGHT[source]` (Reuters 1.5 → Reddit 0.5) and a time-decay factor `exp(-age_h / DECAY_TAU_HOURS)` with `DECAY_TAU_HOURS=48`.
   - `sentiment` / `sentiment_label` — FinBERT, blended `0.6 * title + 0.4 * body` because titles are directional and bodies dilute with boilerplate. Thresholds at ±0.15 for the label.

   **When tuning scoring:** the keyword sets at lines ~228–274 have many comments explaining *why* certain bare words ("strong", "weak", "bear", "support") were removed — read those before adding new keywords or you'll reintroduce known false positives.
4. **save_news()** — merges new items into the on-disk list, dedupes by `link`, expires stale items via `filter_recent_items`, caps at `ON_DISK_MAX=2000`, atomic tmp+rename write under `_file_lock`.

### Dedupe

Two layers, both bounded LRU sets (`BoundedSeen`, `SEEN_MAX=5000`):
- `seen_news` keyed by URL.
- `seen_titles` keyed by `_title_fingerprint(title)` — md5 of lowercased, alnum-only, first 80 chars. This is what catches the same story republished by Reuters/ET/Bloomberg under different URLs.

Both are bootstrapped from `market_news.json` at module import (`_bootstrap_seen_from_disk`) so a restart doesn't re-emit articles already on disk.

### Concurrency & locking

- `_file_lock` (threading.Lock) — guards every read-modify-write of `OUTPUT_FILE`.
- `BoundedSeen` is internally locked.
- The FinBERT pipeline is a lazy singleton behind a lock in `scraper_sentiment.py`.
- HTTP fetching uses two thread pools: one for fan-out across sources (`fetch_news`, max 8 workers), one for article body extraction (`enrich_items`, `EXTRACT_WORKERS=16`).
