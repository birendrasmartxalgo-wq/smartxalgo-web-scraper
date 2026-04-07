import feedparser
import json
import time
import os
import re
import asyncio
import logging
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
from bs4 import BeautifulSoup

try:
    from twscrape import API as TwscrapeAPI, gather as twgather
    TWSCRAPE_AVAILABLE = True
except ImportError:
    TWSCRAPE_AVAILABLE = False

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("scraper")

# =========================
# CONFIG
# =========================
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "market_news.json")

# Bounded dedupe set: keeps the last N links seen, then evicts oldest.
SEEN_MAX        = 5000
# On-disk cache cap — prevents market_news.json from growing forever.
ON_DISK_MAX     = 2000
# Per-source cap inside one fetch_news() cycle.
PER_SOURCE_MAX  = 50

# =========================
# ARTICLE BODY ENRICHMENT
# =========================
MIN_BODY_WORDS    = 100      # below this, fall back to RSS summary
MAX_BODY_CHARS    = 8000     # truncate stored body (~1200 words) to keep JSON sane
EXTRACT_WORKERS   = 16
EXTRACT_TIMEOUT   = 12       # seconds per article
NO_EXTRACT_HOSTS  = {        # known hard paywalls — skip the HTTP call
    "bloomberg.com", "www.bloomberg.com",
    "nytimes.com",   "www.nytimes.com",
    "ft.com",        "www.ft.com",
    "wsj.com",       "www.wsj.com",
}
RSS_INLINE_HOSTS  = {        # RSS already gives full body, no need to fetch
    "reddit.com",    "www.reddit.com",   "old.reddit.com",
}
# Drop items older than this. We only want fresh market news.
MAX_ITEM_AGE_DAYS = 15
EXTRACT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
EXTRACT_HEADERS = {
    "User-Agent":      EXTRACT_UA,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# =========================
# THREAD-SAFE BOUNDED SEEN SET
# =========================
class BoundedSeen:
    def __init__(self, maxlen):
        self.maxlen = maxlen
        self._d = OrderedDict()
        self._lock = threading.Lock()

    def __contains__(self, key):
        with self._lock:
            return key in self._d

    def add(self, key):
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
                return
            self._d[key] = None
            if len(self._d) > self.maxlen:
                self._d.popitem(last=False)

    def update(self, keys):
        for k in keys:
            self.add(k)


seen_news = BoundedSeen(SEEN_MAX)

# Lock guarding read-modify-write of OUTPUT_FILE.
_file_lock = threading.Lock()

# =========================
# TWITTER CREDENTIALS (env vars)
# =========================
TW_USERNAME       = os.getenv("TW_USERNAME", "")
TW_PASSWORD       = os.getenv("TW_PASSWORD", "")
TW_EMAIL          = os.getenv("TW_EMAIL", "")
TW_EMAIL_PASSWORD = os.getenv("TW_EMAIL_PASSWORD", "")
TW_DB_PATH        = os.getenv("TW_DB_PATH", os.path.join(BASE_DIR, "twscrape_accounts.db"))

# =========================
# TWITTER OFFICIAL ACCOUNTS TO MONITOR
# =========================
TWITTER_ACCOUNTS_HIGH_PRIORITY = [
    # Indian Regulators & Exchanges
    "RBI", "SEBI_updates", "NSEIndia", "BSEIndia",
    "nsitharaman", "FinMinIndia", "DasShaktikanta",
    "PMOIndia", "narendramodi", "NITI_Aayog",
    # US Market Movers
    "federalreserve", "USTreasury", "realDonaldTrump",
    "SecScottBessent", "POTUS", "SEC_News",
    # Indian Market Experts
    "Nithin0dha", "NithinKamath", "RadhikaGupta29",
    "deepakshenoy", "Ajay_Bagga", "TamalBandyo",
]

TWITTER_ACCOUNTS_MEDIUM_PRIORITY = [
    "nsitharamanoffc", "MEAIndia", "PiyushGoyal", "nitin_gadkari",
    "AmitShah", "MIB_India", "pib_india", "GST_Council",
    "NPCI_NPCI", "UIDAI", "IncomeTaxIndia", "CBIC_India",
    "MinOfPower", "MundaArjun", "HardeepSPuri", "AshwiniVaishnaw",
    "investindia", "SIDBIofficial", "OfficialNAM",
    "ChairmanSBI", "TheOfficialSBI", "LICIndiaForever",
    "IRDAI_India", "PFRDAOfficial", "IDBI_Bank",
    "anandmahindra", "udaykotak", "Iamsamirarora",
    "RNTata2000", "HarshGoenka", "NandanNilekani",
    "kiranshaw", "DeepinderGoyal", "kunalb11", "kunalbshah",
    "vijayshekhar", "AnupamMittal", "FalguniNayar",
    "GhazalAlagh", "vineetasng", "Nikhil0dha",
    "Mitesh_Engr", "dmuthuk", "Arunstockguru",
    "indiacharts", "nakulvibhor", "MashraniVivek",
    "whitehouse", "CMEGroup", "Nasdaq", "NYSE",
    "elonmusk", "GoldmanSachs", "MorganStanley", "BlackRock",
    "EconAtState", "CommerceGov", "BEA_News",
    "stlouisfed", "NewYorkFed", "StateDept",
    # Russia
    "KremlinRussia_E", "mfa_russia_en", "tass_agency",
    "CentralBankRF", "RF_EnergyMin", "RusEmbIndia",
    "ru_minfin", "MedvedevRussiaE", "AmbRus_India",
    "mod_russia", "GovernmentRF", "RusEmbUSA",
    "KremlinRussia", "mfa_russia", "Russia",
    "sundarpichai", "riteshagar",
]

ALL_TWITTER_ACCOUNTS = TWITTER_ACCOUNTS_HIGH_PRIORITY + TWITTER_ACCOUNTS_MEDIUM_PRIORITY

# =========================
# RSS FEEDS  (Moneycontrol & Reuters intentionally excluded —
#  they have dedicated fetchers above.)
# =========================
RSS_FEEDS = {
    "EconomicTimes": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "CNBC":          "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "Bloomberg":     "https://feeds.bloomberg.com/markets/news.rss",
    "AlJazeera":     "https://www.aljazeera.com/xml/rss/all.xml",
    "NYTimes":       "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "Reddit":        "https://www.reddit.com/r/stocks/.rss",
}

FEEDPARSER_UA = "Mozilla/5.0 (compatible; MarketNewsBot/1.0)"

analyzer = SentimentIntensityAnalyzer()

# =========================
# KEYWORDS  (matched as whole words, case-insensitive)
# =========================
BULLISH_KEYWORDS = {
    "bullish", "bull", "rally", "surge", "moon", "rocket",
    "breakout", "strong", "buy", "accumulate", "long", "golden cross",
}
BEARISH_KEYWORDS = {
    "bearish", "bear", "crash", "dump", "plunge", "sell-off",
    "downtrend", "weak", "sell", "short", "death cross",
}
FINANCIAL_KEYWORDS = {
    "support", "resistance", "breakout", "breakdown",
    "nifty", "banknifty", "sensex", "bse", "nse",
    "rsi", "macd", "bollinger", "volume",
}
GEOPOLITICAL_KEYWORDS = {
    "war", "conflict", "sanction", "fed", "rbi",
    "rate hike", "inflation", "recession",
}
KEYWORDS = {
    "nifty", "bank nifty", "sensex", "market crash", "inflation",
    "interest rate", "rbi", "fii", "dii", "expiry", "pcr",
    "stocks", "market", "shares", "trading",
    "breakout", "support", "resistance", "rally", "selloff",
}
HIGH_IMPACT = {
    "rbi policy", "repo rate", "rate hike", "market crash",
    "recession", "budget", "war", "fii selling", "fed", "interest cut",
}


def _compile_word_set(words):
    """Compile a set of phrases into a single word-boundary regex."""
    escaped = sorted((re.escape(w) for w in words), key=len, reverse=True)
    pattern = r"(?<!\w)(?:" + "|".join(escaped) + r")(?!\w)"
    return re.compile(pattern, re.IGNORECASE)


_RE_HIGH_IMPACT  = _compile_word_set(HIGH_IMPACT)
_RE_KEYWORDS     = _compile_word_set(KEYWORDS)
_RE_BULLISH      = _compile_word_set(BULLISH_KEYWORDS)
_RE_BEARISH      = _compile_word_set(BEARISH_KEYWORDS)
_RE_FINANCIAL    = _compile_word_set(FINANCIAL_KEYWORDS)
_RE_GEOPOLITICAL = _compile_word_set(GEOPOLITICAL_KEYWORDS)


# =========================
# SCORING
# =========================
def get_news_score(text):
    if not text:
        return 0
    score  = 0
    score += 5 * len(_RE_HIGH_IMPACT.findall(text))
    score += 1 * len(_RE_KEYWORDS.findall(text))
    score += 2 * len(_RE_BULLISH.findall(text))
    score += 2 * len(_RE_BEARISH.findall(text))
    score += 1 * len(_RE_FINANCIAL.findall(text))
    score += 2 * len(_RE_GEOPOLITICAL.findall(text))
    return score


def get_impact(score):
    if score >= 6:
        return "HIGH"
    if score >= 3:
        return "MEDIUM"
    return "LOW"


# =========================
# BUILD ITEM
# =========================
def build_news_item(source, title, link, summary="", published=""):
    # Some RSS feeds (Google News in particular) wrap the title in HTML
    # tags as the "summary". Strip those so the stored summary is clean text.
    summary = _clean_summary_html(summary) if summary else ""
    combined = f"{title} {summary}".strip()
    sentiment = analyzer.polarity_scores(combined)
    score = get_news_score(combined)
    return {
        "source": source,
        "title": title,
        "summary": summary,
        "link": link,
        "published": published,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "score": score,
        "impact": get_impact(score),
        "sentiment": sentiment["compound"],
        "sentiment_label": (
            "bullish" if sentiment["compound"] > 0.05
            else "bearish" if sentiment["compound"] < -0.05
            else "neutral"
        ),
    }


# =========================
# MONEYCONTROL
# =========================
# Article URLs on moneycontrol look like:
#   /news/business/markets/...
#   /news/business/stocks/...
# Anything else (login, category index, app downloads) is navigation junk.
_MC_ARTICLE_RE = re.compile(
    r"^https?://(?:www\.)?moneycontrol\.com/news/[a-z0-9\-/]+\-\d+\.html",
    re.IGNORECASE,
)
_MC_JUNK_TITLES = {
    "english", "hindi", "login", "register", "subscribe", "home",
    "markets", "news", "business", "more", "menu", "search",
}

def fetch_moneycontrol_html():
    news_list = []
    try:
        res = requests.get(
            "https://www.moneycontrol.com/news/business/markets/",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        for a in soup.find_all("a", href=True):
            link  = a["href"]
            title = a.get_text(strip=True)
            if not title or len(title) < 25:
                continue
            if title.lower() in _MC_JUNK_TITLES:
                continue
            if not _MC_ARTICLE_RE.match(link):
                continue
            if link in seen_news:
                continue
            news_list.append(build_news_item("Moneycontrol", title, link))
            seen_news.add(link)
            if len(news_list) >= PER_SOURCE_MAX:
                break
    except Exception as e:
        log.warning("Moneycontrol error: %s", e)
    log.info("Moneycontrol: %d articles", len(news_list))
    return news_list


# =========================
# REUTERS  (public RSS dead → use Google News scoped to reuters.com)
# =========================
REUTERS_GOOGLE_RSS = [
    "https://news.google.com/rss/search?q=site:reuters.com+india+market+economy&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=site:reuters.com+nifty+sensex+rbi+sebi&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=site:reuters.com+india+finance+trade&hl=en-IN&gl=IN&ceid=IN:en",
]
REUTERS_DIRECT_RSS = [
    "https://feeds.reuters.com/reuters/INbusinessNews",
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/topNews",
]


def _harvest_reuters_feed(rss_url, news_list, limit):
    try:
        feed = feedparser.parse(rss_url, agent=FEEDPARSER_UA)
        if not feed.entries:
            return

        # 1. Cheap pre-filter: drop stale + malformed entries before any resolves.
        candidates = []
        remaining = limit - len(news_list)
        for entry in feed.entries:
            if remaining <= 0:
                break
            raw_link  = entry.get("link", "")
            title     = entry.get("title", "")
            published = entry.get("published", "")
            if not title or not raw_link:
                continue
            if published and not _is_recent({"published": published}):
                continue
            candidates.append(entry)
            remaining -= 1

        if not candidates:
            return

        # 2. Resolve Google News URLs in parallel (small pool to be polite).
        def resolve(entry):
            return entry, _resolve_google_news_url(entry.get("link", ""))

        with ThreadPoolExecutor(max_workers=4) as pool:
            for entry, link in pool.map(resolve, candidates):
                if not link or link in seen_news:
                    continue
                news_list.append(build_news_item(
                    "Reuters",
                    entry.get("title", ""),
                    link,
                    entry.get("summary", ""),
                    entry.get("published", ""),
                ))
                seen_news.add(link)
                if len(news_list) >= limit:
                    return
    except Exception as e:
        log.warning("Reuters feed error [%s]: %s", rss_url[:55], e)


def fetch_reuters_news(limit=PER_SOURCE_MAX):
    news_list = []
    # Try ALL Google News URLs (don't return on first hit — they cover
    # different topical queries).
    for rss_url in REUTERS_GOOGLE_RSS:
        _harvest_reuters_feed(rss_url, news_list, limit)
        if len(news_list) >= limit:
            break
    if len(news_list) < limit:
        for rss_url in REUTERS_DIRECT_RSS:
            _harvest_reuters_feed(rss_url, news_list, limit)
            if len(news_list) >= limit:
                break
    log.info("Reuters: %d articles", len(news_list))
    return news_list


# =========================
# TWITTER — twscrape (real timelines, needs credentials)
# =========================
_tw_api_instance = None
_tw_api_lock     = threading.Lock()


async def _twscrape_init_api():
    global _tw_api_instance
    if _tw_api_instance is not None:
        return _tw_api_instance
    api = TwscrapeAPI(TW_DB_PATH)
    if TW_USERNAME and TW_PASSWORD and TW_EMAIL:
        try:
            await api.pool.add_account(
                TW_USERNAME, TW_PASSWORD,
                TW_EMAIL, TW_EMAIL_PASSWORD,
            )
            await api.pool.login_all()
        except Exception as e:
            log.warning("Twitter login warning (may already be logged in): %s", e)
    _tw_api_instance = api
    return api


async def _twscrape_fetch_async(accounts, limit_per_account=3):
    news_list = []
    try:
        api = await _twscrape_init_api()

        async def fetch_one(username):
            items = []
            try:
                user = await api.user_by_login(username)
                if not user:
                    return items
                tweets = await twgather(api.user_tweets(user.id, limit=limit_per_account))
                for tw in tweets:
                    text = tw.rawContent or ""
                    link = f"https://x.com/{username}/status/{tw.id}"
                    if not text or link in seen_news:
                        continue
                    items.append(build_news_item(
                        "Twitter",
                        f"@{username}: {text[:120]}",
                        link,
                        text,
                        str(tw.date) if tw.date else "",
                    ))
                    seen_news.add(link)
            except Exception as e:
                log.debug("twscrape @%s: %s", username, e)
            return items

        BATCH = 10
        for i in range(0, len(accounts), BATCH):
            batch = accounts[i:i + BATCH]
            results = await asyncio.gather(*[fetch_one(u) for u in batch])
            for r in results:
                news_list.extend(r)
            if i + BATCH < len(accounts):
                await asyncio.sleep(1.5)
    except Exception as e:
        log.warning("twscrape async error: %s", e)
    return news_list


def _fetch_twitter_via_twscrape(limit_per_account=3):
    accounts = ALL_TWITTER_ACCOUNTS
    with _tw_api_lock:
        try:
            news = asyncio.run(_twscrape_fetch_async(accounts, limit_per_account))
        except RuntimeError:
            # An event loop is already running in this thread. Run on a fresh
            # thread that owns its own loop.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                news = pool.submit(
                    asyncio.run,
                    _twscrape_fetch_async(accounts, limit_per_account),
                ).result(timeout=180)
    log.info("Twitter (twscrape): %d tweets from %d accounts", len(news), len(accounts))
    return news


# =========================
# TWITTER GUEST TOKEN  (no login needed)
# =========================
_TW_BEARER = os.getenv(
    "TW_GUEST_BEARER",
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
    "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCbke80A1X8Yb0",
)
_guest_token_cache = {"token": None, "ts": 0}
_guest_token_lock  = threading.Lock()


def _get_guest_token():
    now = time.time()
    with _guest_token_lock:
        if _guest_token_cache["token"] and now - _guest_token_cache["ts"] < 900:
            return _guest_token_cache["token"]
        try:
            res = requests.post(
                "https://api.twitter.com/1.1/guest/activate.json",
                headers={"Authorization": f"Bearer {_TW_BEARER}"},
                timeout=10,
            )
            if res.status_code == 200:
                token = res.json().get("guest_token")
                _guest_token_cache["token"] = token
                _guest_token_cache["ts"]    = now
                return token
            log.warning("Guest token HTTP %s", res.status_code)
        except Exception as e:
            log.warning("Guest token error: %s", e)
    return None


def _fetch_user_tweets_guest(username, guest_token, count=3):
    """Returns ([(text, link), ...], rate_limited:bool)."""
    try:
        headers = {
            "Authorization": f"Bearer {_TW_BEARER}",
            "x-guest-token": guest_token,
            "User-Agent":    "Mozilla/5.0",
        }
        res = requests.get(
            "https://api.twitter.com/1.1/statuses/user_timeline.json",
            params={
                "screen_name":     username,
                "count":           count,
                "tweet_mode":      "extended",
                "exclude_replies": True,
                "include_rts":     False,
            },
            headers=headers,
            timeout=10,
        )
        if res.status_code == 429:
            return [], True
        if res.status_code != 200:
            return [], False
        result = []
        for tw in res.json():
            text = tw.get("full_text") or tw.get("text", "")
            tid  = tw.get("id_str", "")
            if text and tid:
                result.append((text, f"https://x.com/{username}/status/{tid}"))
        return result, False
    except Exception as e:
        log.debug("Guest fetch @%s: %s", username, e)
        return [], False


def _fetch_twitter_via_guest_token(accounts, limit_per_account=3):
    guest_token = _get_guest_token()
    if not guest_token:
        log.warning("Twitter: could not get guest token")
        return []

    news_list      = []
    rate_limited   = False

    def worker(username):
        if rate_limited:
            return []
        tweets, throttled = _fetch_user_tweets_guest(username, guest_token, limit_per_account)
        items = []
        for text, link in tweets:
            if link in seen_news:
                continue
            items.append(build_news_item(
                "Twitter",
                f"@{username}: {text[:120]}",
                link,
                text,
                "",
            ))
            seen_news.add(link)
        return (items, throttled)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(worker, u): u for u in accounts}
        for fut in as_completed(futures):
            try:
                items, throttled = fut.result()
            except Exception as e:
                log.debug("guest worker error: %s", e)
                continue
            if throttled:
                rate_limited = True
            news_list.extend(items)

    if rate_limited:
        log.warning("Twitter guest token: rate-limited mid-fetch")
    log.info("Twitter (guest token): %d tweets", len(news_list))
    return news_list


# =========================
# TWITTER ENTRY POINT
# =========================
def fetch_twitter_news(accounts=None, limit_per_account=3):
    """Priority: twscrape → guest token. No search-URL fallback —
    we never link customers to external sources."""
    if TWSCRAPE_AVAILABLE and TW_USERNAME and TW_PASSWORD:
        try:
            result = _fetch_twitter_via_twscrape(limit_per_account)
            if result:
                return result
            log.info("twscrape returned 0 tweets — trying guest token")
        except Exception as e:
            log.warning("twscrape failed: %s — trying guest token", e)

    try:
        accs   = accounts if accounts else ALL_TWITTER_ACCOUNTS
        result = _fetch_twitter_via_guest_token(accs, limit_per_account)
        if result:
            return result
    except Exception as e:
        log.warning("Guest token failed: %s", e)

    log.info("Twitter: all sources exhausted; no items this cycle")
    return []


# =========================
# RSS FETCHER
# =========================
def fetch_rss_source(source, url, limit=PER_SOURCE_MAX):
    items = []
    try:
        feed = feedparser.parse(url, agent=FEEDPARSER_UA)
        for entry in feed.entries:
            link  = entry.get("link", "")
            title = entry.get("title", "")
            if not title or not link or link in seen_news:
                continue
            items.append(build_news_item(
                source, title, link,
                entry.get("summary", ""),
                entry.get("published", ""),
            ))
            seen_news.add(link)
            if len(items) >= limit:
                break
    except Exception as e:
        log.warning("%s RSS error: %s", source, e)
    log.info("%s: %d articles", source, len(items))
    return items


# =========================
# ARTICLE BODY EXTRACTION
# =========================
_google_news_host_re = re.compile(r"(^|\.)news\.google\.com$", re.IGNORECASE)


def _host(url):
    from urllib.parse import urlparse
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def _resolve_google_news_url(url):
    """Decode a news.google.com/rss/articles/CBM... URL to the underlying article."""
    if not url:
        return url
    try:
        host = _host(url)
        if not _google_news_host_re.search(host):
            return url
        from googlenewsdecoder import gnewsdecoder
        result = gnewsdecoder(url, interval=1)
        if result and result.get("status") and result.get("decoded_url"):
            return result["decoded_url"]
    except Exception as e:
        log.debug("google news decode failed for %s: %s", url[:60], e)
    return url


def extract_article_body(url):
    """Return cleaned article body text (>= MIN_BODY_WORDS), or '' on failure."""
    host = _host(url)
    if host in NO_EXTRACT_HOSTS:
        return ""
    try:
        res = requests.get(
            url,
            headers=EXTRACT_HEADERS,
            timeout=EXTRACT_TIMEOUT,
            allow_redirects=True,
        )
        if res.status_code != 200 or not res.text:
            return ""
        # Primary: trafilatura
        try:
            import trafilatura
            body = trafilatura.extract(
                res.text,
                include_comments=False,
                include_tables=False,
                favor_precision=True,
                deduplicate=True,
            )
            if body:
                body = body.strip()
                if len(body.split()) >= MIN_BODY_WORDS:
                    return body[:MAX_BODY_CHARS]
        except Exception as e:
            log.debug("trafilatura failed for %s: %s", host, e)
        # Fallback: BS4 paragraph join
        soup = BeautifulSoup(res.text, "html.parser")
        paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        text  = " ".join(p for p in paras if len(p) > 40)
        if text and len(text.split()) >= MIN_BODY_WORDS:
            return text[:MAX_BODY_CHARS]
    except Exception as e:
        log.debug("extract_article_body %s: %s", host, e)
    return ""


def _clean_summary_html(text):
    """Strip HTML tags + extra whitespace from RSS summary fields."""
    if not text:
        return ""
    try:
        cleaned = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
        return re.sub(r"\s+", " ", cleaned).strip()
    except Exception:
        return text


# =========================
# DATE / FRESHNESS FILTER
# =========================
def _parse_item_date(item):
    """Best-effort parse of an item's age. Falls back to its 'timestamp'
    field (set when we fetched it), so freshly-fetched items always survive."""
    from email.utils import parsedate_to_datetime
    pub = item.get("published") or ""
    if pub:
        try:
            dt = parsedate_to_datetime(pub)
            if dt is not None:
                return dt
        except Exception:
            pass
        try:
            import dateparser
            dt = dateparser.parse(pub)
            if dt is not None:
                return dt
        except Exception:
            pass
    ts = item.get("timestamp") or ""
    if ts:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return None


def _is_recent(item, max_age_days=MAX_ITEM_AGE_DAYS):
    """Return True if the item's date is within max_age_days, OR if we
    cannot determine the date at all (don't drop unknowns)."""
    dt = _parse_item_date(item)
    if dt is None:
        return True
    # Normalise to naive UTC for comparison.
    if dt.tzinfo is not None:
        dt = dt.astimezone(tz=None).replace(tzinfo=None)
    age = datetime.now() - dt
    return age.days <= max_age_days


def filter_recent_items(items, max_age_days=MAX_ITEM_AGE_DAYS):
    """Drop items older than max_age_days. Items with no parseable date
    are kept (we have no basis to drop them)."""
    if not items:
        return items
    fresh = [it for it in items if _is_recent(it, max_age_days)]
    dropped = len(items) - len(fresh)
    if dropped:
        log.info("Dropped %d items older than %d days", dropped, max_age_days)
    return fresh


def enrich_items(items):
    """Fetch full article bodies in parallel and rescore items in place.

    Every item ends up with a 'content' field, even if extraction fails:
    - Reddit:        RSS already has the post body  -> use it
    - Hard paywalls: skip HTTP, fall back to summary
    - Everything else: try trafilatura, then BS4, then summary
    Items are then re-scored using the richest available text.
    """
    if not items:
        return items

    def needs_fetch(item):
        if item.get("content"):
            return False
        host = _host(item.get("link", ""))
        if host in RSS_INLINE_HOSTS:
            return False
        if host in NO_EXTRACT_HOSTS:
            return False
        return bool(item.get("link"))

    targets = [it for it in items if needs_fetch(it)]

    def worker(item):
        body = extract_article_body(item["link"])
        if body:
            item["content"] = body

    if targets:
        with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as pool:
            list(pool.map(worker, targets))

    # Backfill: anything still without content uses the (cleaned) RSS summary.
    for item in items:
        if not item.get("content"):
            fallback = _clean_summary_html(item.get("summary", ""))
            item["content"] = fallback or item.get("title", "")

    # Re-score and re-sentiment using the richest text we now have.
    for item in items:
        combined = f"{item.get('title','')} {item['content']}"
        item["score"]           = get_news_score(combined)
        item["impact"]          = get_impact(item["score"])
        sent                    = analyzer.polarity_scores(combined)
        item["sentiment"]       = sent["compound"]
        item["sentiment_label"] = (
            "bullish" if sent["compound"] >  0.05 else
            "bearish" if sent["compound"] < -0.05 else "neutral"
        )

    full = sum(1 for it in items if len(it["content"].split()) >= MIN_BODY_WORDS)
    log.info("Enriched %d / %d items with full body (>=%d words)",
             full, len(items), MIN_BODY_WORDS)
    return items


# =========================
# FETCH NEWS  (concurrent)
# =========================
def fetch_news():
    """Fetch all sources in parallel and merge results."""
    new_items = []
    tasks = {
        "Moneycontrol": fetch_moneycontrol_html,
        "Reuters":      fetch_reuters_news,
        "Twitter":      fetch_twitter_news,
    }
    for source, url in RSS_FEEDS.items():
        # capture defaults
        tasks[source] = (lambda s=source, u=url: fetch_rss_source(s, u))

    with ThreadPoolExecutor(max_workers=min(8, len(tasks))) as pool:
        futures = {pool.submit(fn): name for name, fn in tasks.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                new_items.extend(fut.result())
            except Exception as e:
                log.warning("source %s crashed: %s", name, e)

    log.info("Total news fetched: %d", len(new_items))
    new_items = filter_recent_items(new_items)
    return new_items


# =========================
# SAVE  (atomic, deduped, capped)
# =========================
def save_news(news):
    if not news:
        return
    with _file_lock:
        try:
            with open(OUTPUT_FILE, "r") as f:
                existing = json.load(f)
                if not isinstance(existing, list):
                    existing = []
        except Exception:
            existing = []

        # Expire stale items already on disk before merging fresh ones.
        existing = filter_recent_items(
            [it for it in existing if isinstance(it, dict)]
        )

        # Dedupe vs on-disk by link.
        existing_links = {item.get("link") for item in existing}
        for item in news:
            link = item.get("link")
            if link and link not in existing_links:
                existing.append(item)
                existing_links.add(link)

        # Cap total — keep the most recent ON_DISK_MAX.
        if len(existing) > ON_DISK_MAX:
            existing = existing[-ON_DISK_MAX:]

        # Atomic write: tmp file + rename.
        tmp_path = OUTPUT_FILE + ".tmp"
        try:
            with open(tmp_path, "w") as f:
                json.dump(existing, f, indent=4)
            os.replace(tmp_path, OUTPUT_FILE)
        except Exception as e:
            log.error("save_news write failed: %s", e)
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass


# =========================
# BACKGROUND SCRAPER
# =========================
def background_scraper(socketio, interval=60):
    log.info("Real-Time Scraper Started (interval=%ss)...", interval)
    consecutive_failures = 0
    while True:
        try:
            news = fetch_news()
            if news:
                news = enrich_items(news)
                save_news(news)
                socketio.emit("news_update", {"news": news})
                consecutive_failures = 0
            else:
                log.info("No news found")
        except Exception as e:
            consecutive_failures += 1
            log.error("Scraper error (#%d): %s", consecutive_failures, e)
            if consecutive_failures >= 5:
                # Back off when something is persistently broken.
                log.error("5 consecutive failures — backing off for 5 minutes")
                time.sleep(300)
                consecutive_failures = 0
                continue
        time.sleep(interval)


# =========================
# BOOTSTRAP DEDUPE FROM DISK
# =========================
def _bootstrap_seen_from_disk():
    """Pre-populate seen_news with links already on disk so a restart
    doesn't re-fetch + re-enrich articles we already have."""
    try:
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            count = 0
            for it in data:
                link = it.get("link") if isinstance(it, dict) else None
                if link:
                    seen_news.add(link)
                    count += 1
            log.info("Bootstrapped seen_news with %d existing links", count)
    except FileNotFoundError:
        pass
    except Exception as e:
        log.debug("seen_news bootstrap skipped: %s", e)


_bootstrap_seen_from_disk()
