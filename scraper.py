import feedparser
import hashlib
import json
import math
import time
import os
import re
import asyncio
import logging
import threading
import unicodedata
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
import requests
import websocket
from bs4 import BeautifulSoup

from scraper_sentiment import score_text, warm_pipeline

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

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

# Node.js uWebSockets.js backend — persistent WS push
NODE_WS_URL       = os.getenv("NODE_WS_URL", "ws://localhost:3000/ws/ingest")
NODE_PUSH_RETRIES = 3

# PostgreSQL — persistent storage (local + shared Supabase)
PG_DSN = os.getenv("DATABASE_URL", "postgresql://sxa:sxa%402025@127.0.0.1:5432/market_news_analysis_db")

# Queue processor — reads JSON backlog, dedupes vs DB, writes fresh rows.
QUEUE_POINTER_FILE     = os.path.join(BASE_DIR, "queue.pointer")
QUEUE_INTERVAL         = int(os.getenv("QUEUE_INTERVAL", "60"))            # seconds between runs
SIMILARITY_THRESHOLD   = float(os.getenv("SIMILARITY_THRESHOLD", "0.95"))  # 0..1 — fuzzy dedupe cutoff
SIMILARITY_WINDOW_DAYS = int(os.getenv("SIMILARITY_WINDOW_DAYS", "2"))     # how far back to compare same-source titles

# Retention — keep only the last N days in PostgreSQL.
RETENTION_DAYS     = int(os.getenv("RETENTION_DAYS", "10"))
RETENTION_INTERVAL = int(os.getenv("RETENTION_INTERVAL", str(6 * 3600)))   # run every 6h

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
MAX_ITEM_AGE_DAYS = 10
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
# Second dedupe layer keyed by a normalized-title hash. Catches the same
# story published by Reuters/ET/Bloomberg under different URLs.
seen_titles = BoundedSeen(SEEN_MAX)

_TITLE_FP_RE = re.compile(r"[^a-z0-9]+")


def _title_fingerprint(title):
    """Lowercase, strip non-alnum, take first 80 chars, md5. Stable across
    minor punctuation/spacing differences."""
    if not title:
        return ""
    norm = _TITLE_FP_RE.sub("", title.lower())[:80]
    if not norm:
        return ""
    return hashlib.md5(norm.encode("utf-8")).hexdigest()


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

# =========================
# SCORING WEIGHTS / CONFIG
# =========================
# Cap on how many times a single keyword category can contribute to the
# raw score. Stops repetition-attacks like "war war war war" outscoring
# real headlines.
MAX_HITS_PER_CATEGORY = 3

# Source credibility multiplier applied to the raw score in enrich_items.
# Reuters / regulators get a boost; Reddit gets cut.
SOURCE_WEIGHT = {
    "Reuters":       1.5,
    "Bloomberg":     1.4,
    "NYTimes":       1.3,
    "Twitter":       1.2,  # mostly official handles in our list
    "EconomicTimes": 1.1,
    "Moneycontrol":  1.0,
    "CNBC":          1.0,
    "AlJazeera":     1.0,
    "Reddit":        0.5,
}
DEFAULT_SOURCE_WEIGHT = 1.0

# Time-decay half-life for scores, in hours. exp(-age_h / DECAY_TAU_HOURS).
# 48h tau ≈ 33h half-life, so 2-day-old news is ~half as actionable.
DECAY_TAU_HOURS = 48.0

# =========================
# KEYWORDS  (matched as whole words, case-insensitive)
# =========================
BULLISH_KEYWORDS = {
    # Removed polysemous adjectives ("strong", "long", "moon", "rocket")
    # that match anywhere. Kept words that are nearly always market-context.
    "bullish", "bull market", "rally", "surge", "breakout",
    "buy", "accumulate", "golden cross", "uptrend",
}
BEARISH_KEYWORDS = {
    # Removed "weak", "short", "bear" (bare) for the same reason.
    "bearish", "bear market", "crash", "dump", "plunge", "sell-off",
    "downtrend", "selloff", "death cross", "rout",
}
# Trading-context only — these phrases never appear outside of charts.
FINANCIAL_KEYWORDS = {
    "support level", "resistance level", "breakout", "breakdown",
    "nifty", "banknifty", "sensex", "bse", "nse",
    "rsi", "macd", "bollinger", "trading volume",
}
GEOPOLITICAL_KEYWORDS = {
    "war", "conflict", "sanction", "fed", "rbi",
    "rate hike", "inflation", "recession",
}
# General market vocabulary — single words/phrases that are usually
# market-context. Removed bare "support", "resistance", "market", "trading"
# because they fired on "support for X" / "axis of resistance" etc.
KEYWORDS = {
    "nifty", "bank nifty", "sensex", "market crash", "inflation",
    "interest rate", "rbi", "fii", "dii", "expiry", "pcr",
    "stock market", "stocks", "equities", "shares",
    "selloff", "rally",
}
HIGH_IMPACT = {
    "rbi policy", "repo rate", "rate hike", "rate cut", "market crash",
    "recession", "budget", "war", "fii selling", "fed", "interest cut",
}
# HARD anchors: words/phrases that are unambiguously about markets.
# F4's anchor requirement (HIGH_IMPACT / GEOPOLITICAL only count when an
# anchor is present) is satisfied ONLY by hits in this set, not by the
# softer KEYWORDS / FINANCIAL sets. Keeps pure-geopolitics articles
# (Houthis, synagogue) from leaking through.
HARD_ANCHORS = {
    "nifty", "bank nifty", "banknifty", "sensex", "bse", "nse",
    "rbi", "sebi", "fii", "dii", "rupee", "repo rate",
    "rate hike", "rate cut", "interest rate", "inflation",
    "bond yield", "crude oil", "brent",
    "breakout", "breakdown", "support level", "resistance level",
    "stock market", "equities",
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
_RE_HARD_ANCHOR  = _compile_word_set(HARD_ANCHORS)


# =========================
# SECTOR / TICKER / INDEX MAPS
# =========================
# The four indices that gate the relevance filter. Anything that doesn't
# touch one of these is dropped post-enrichment.
TARGET_INDICES = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"})

# Sector → which indices it influences. Banking touches all four; broad
# macro/geopolitical news affects all four; pure single-sector news only
# moves Nifty + Sensex (and Realty is small-cap-heavy so just Nifty).
SECTOR_INDICES = {
    "Banking":      ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"],
    "Financials":   ["NIFTY", "FINNIFTY", "SENSEX"],
    "IT":           ["NIFTY", "SENSEX"],
    "Auto":         ["NIFTY", "SENSEX"],
    "Pharma":       ["NIFTY", "SENSEX"],
    "FMCG":         ["NIFTY", "SENSEX"],
    "Energy":       ["NIFTY", "SENSEX"],
    "Metals":       ["NIFTY", "SENSEX"],
    "Realty":       ["NIFTY"],
    "Telecom":      ["NIFTY", "SENSEX"],
    "Cement":       ["NIFTY", "SENSEX"],
    "Power":        ["NIFTY", "SENSEX"],
    "Macro":        ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"],
    "Geopolitical": ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"],
}

# Sector → phrase set. Phrases are matched case-insensitively with word
# boundaries via _compile_word_set, so order/case in the source doesn't
# matter. Keep phrases unambiguous (avoid bare words like "power" or "auto"
# that fire on unrelated articles — same lesson as the keyword sets above).
SECTOR_KEYWORDS = {
    "Banking": {
        "bank nifty", "banknifty", "private bank", "psu bank",
        "hdfc bank", "icici bank", "axis bank", "kotak bank",
        "kotak mahindra bank", "indusind bank", "state bank of india",
        "sbi", "punjab national bank", "bank of baroda", "canara bank",
        "union bank", "idfc first bank", "yes bank", "federal bank",
        "au small finance", "bandhan bank",
    },
    "Financials": {
        "nbfc", "bajaj finance", "bajaj finserv", "shriram finance",
        "cholamandalam", "muthoot finance", "manappuram", "hdfc amc",
        "sbi life", "hdfc life", "icici prudential", "icici lombard",
        "lic of india", "lic india", "life insurance corporation",
        "fin nifty", "finnifty",
    },
    "IT": {
        "infosys", "tcs", "tata consultancy", "wipro", "hcl tech",
        "hcltech", "tech mahindra", "lti mindtree", "ltimindtree",
        "mphasis", "persistent systems", "coforge",
        "indian it sector", "nifty it",
    },
    "Auto": {
        "maruti suzuki", "tata motors", "mahindra & mahindra",
        "m&m auto", "bajaj auto", "hero motocorp", "eicher motors",
        "tvs motor", "ashok leyland", "bosch india", "auto sales",
        "passenger vehicle sales", "two wheeler sales", "nifty auto",
    },
    "Pharma": {
        "sun pharma", "dr reddy", "dr. reddy", "cipla", "divi's lab",
        "divis lab", "lupin", "aurobindo pharma", "torrent pharma",
        "biocon", "zydus lifesciences", "apollo hospitals",
        "indian pharma", "nifty pharma",
    },
    "FMCG": {
        "hindustan unilever", "hul india", "itc ltd", "nestle india",
        "britannia", "dabur", "marico", "godrej consumer",
        "tata consumer", "colgate palmolive india", "nifty fmcg",
    },
    "Energy": {
        "reliance industries", "ril", "ongc", "indian oil", "ioc",
        "bpcl", "hpcl", "gail india", "oil india", "petronet lng",
        "nifty energy", "crude oil", "brent crude",
    },
    "Metals": {
        "tata steel", "jsw steel", "hindalco", "vedanta", "coal india",
        "nmdc", "sail", "jindal steel", "nalco", "nifty metal",
    },
    "Realty": {
        "dlf", "godrej properties", "oberoi realty", "prestige estates",
        "macrotech", "lodha developers", "phoenix mills", "nifty realty",
    },
    "Telecom": {
        "bharti airtel", "vodafone idea", "vi india", "reliance jio",
        "jio platforms", "indus towers",
    },
    "Cement": {
        "ultratech cement", "shree cement", "ambuja cement", "acc cement",
        "dalmia bharat", "ramco cements", "jk cement",
    },
    "Power": {
        "ntpc", "power grid", "tata power", "adani power", "jsw energy",
        "torrent power", "nhpc", "sjvn", "nifty power",
    },
    # Macro reuses HARD_ANCHORS-style vocabulary; kept narrow on purpose.
    "Macro": {
        "rbi", "sebi", "fii", "dii", "rupee", "repo rate", "rate hike",
        "rate cut", "interest rate", "inflation", "cpi inflation",
        "wpi inflation", "gdp growth", "fiscal deficit", "current account",
        "bond yield", "fomc", "fed rate", "fed policy", "rbi policy",
        "monetary policy", "budget", "union budget",
    },
    "Geopolitical": {
        "war", "conflict", "sanction", "tariff", "trade war",
        "ukraine war", "russia ukraine", "israel gaza", "iran israel",
        "china tension", "taiwan tension", "red sea", "houthi",
    },
}

_RE_SECTORS = {name: _compile_word_set(words) for name, words in SECTOR_KEYWORDS.items()}

# Curated NSE tickers — Nifty 50 + Bank Nifty + Fin Nifty additions, with
# a couple of Sensex-only names. Each entry pre-tags sector and the indices
# it belongs to so we don't recompute at scoring time.
# Aliases are matched as whole-word phrases; the symbol itself is also
# matched (case-insensitive) so "INFY" or "HDFCBANK" in a tweet works too.
def _t(name, aliases, sector, indices):
    return {"name": name, "aliases": aliases, "sector": sector, "indices": indices}

_NIFTY_BANK_SENSEX  = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]
_NIFTY_FIN_SENSEX   = ["NIFTY", "FINNIFTY", "SENSEX"]
_NIFTY_SENSEX       = ["NIFTY", "SENSEX"]
_NIFTY_ONLY         = ["NIFTY"]

STOCK_TICKERS = {
    # ---- Banks (Bank Nifty + Fin Nifty + Nifty 50) ----
    "HDFCBANK":   _t("HDFC Bank",          ["hdfc bank"],                              "Banking", _NIFTY_BANK_SENSEX),
    "ICICIBANK":  _t("ICICI Bank",         ["icici bank"],                             "Banking", _NIFTY_BANK_SENSEX),
    "SBIN":       _t("State Bank of India",["sbi", "state bank of india"],             "Banking", _NIFTY_BANK_SENSEX),
    "AXISBANK":   _t("Axis Bank",          ["axis bank"],                              "Banking", _NIFTY_BANK_SENSEX),
    "KOTAKBANK":  _t("Kotak Mahindra Bank",["kotak bank", "kotak mahindra bank"],      "Banking", _NIFTY_BANK_SENSEX),
    "INDUSINDBK": _t("IndusInd Bank",      ["indusind bank"],                          "Banking", _NIFTY_BANK_SENSEX),
    "PNB":        _t("Punjab National Bank",["punjab national bank"],                  "Banking", ["NIFTY", "BANKNIFTY"]),
    "BANKBARODA": _t("Bank of Baroda",     ["bank of baroda"],                         "Banking", ["NIFTY", "BANKNIFTY"]),
    "CANBK":      _t("Canara Bank",        ["canara bank"],                            "Banking", ["BANKNIFTY"]),
    "FEDERALBNK": _t("Federal Bank",       ["federal bank"],                           "Banking", ["BANKNIFTY"]),
    "IDFCFIRSTB": _t("IDFC First Bank",    ["idfc first bank"],                        "Banking", ["BANKNIFTY"]),
    "AUBANK":     _t("AU Small Finance",   ["au small finance"],                       "Banking", ["BANKNIFTY"]),
    # ---- Financials / NBFC / Insurance (Fin Nifty) ----
    "BAJFINANCE": _t("Bajaj Finance",      ["bajaj finance"],                          "Financials", _NIFTY_FIN_SENSEX),
    "BAJAJFINSV": _t("Bajaj Finserv",      ["bajaj finserv"],                          "Financials", _NIFTY_FIN_SENSEX),
    "SHRIRAMFIN": _t("Shriram Finance",    ["shriram finance"],                        "Financials", _NIFTY_FIN_SENSEX),
    "CHOLAFIN":   _t("Cholamandalam",      ["cholamandalam", "chola finance"],         "Financials", ["FINNIFTY"]),
    "MUTHOOTFIN": _t("Muthoot Finance",    ["muthoot finance"],                        "Financials", ["FINNIFTY"]),
    "HDFCAMC":    _t("HDFC AMC",           ["hdfc amc"],                               "Financials", ["FINNIFTY"]),
    "HDFCLIFE":   _t("HDFC Life",          ["hdfc life"],                              "Financials", _NIFTY_FIN_SENSEX),
    "SBILIFE":    _t("SBI Life",           ["sbi life"],                               "Financials", _NIFTY_FIN_SENSEX),
    "ICICIPRULI": _t("ICICI Prudential",   ["icici prudential"],                       "Financials", ["FINNIFTY"]),
    "ICICIGI":    _t("ICICI Lombard",      ["icici lombard"],                          "Financials", ["FINNIFTY"]),
    "LICI":       _t("LIC of India",       ["lic of india", "lic india",
                                            "life insurance corporation"],             "Financials", ["FINNIFTY"]),
    # ---- IT (Nifty 50 + Sensex) ----
    "INFY":       _t("Infosys",            ["infosys"],                                "IT", _NIFTY_SENSEX),
    "TCS":        _t("Tata Consultancy",   ["tcs", "tata consultancy"],                "IT", _NIFTY_SENSEX),
    "WIPRO":      _t("Wipro",              ["wipro"],                                  "IT", _NIFTY_SENSEX),
    "HCLTECH":    _t("HCL Tech",           ["hcl tech", "hcltech"],                    "IT", _NIFTY_SENSEX),
    "TECHM":      _t("Tech Mahindra",      ["tech mahindra"],                          "IT", _NIFTY_SENSEX),
    "LTIM":       _t("LTIMindtree",        ["lti mindtree", "ltimindtree"],            "IT", _NIFTY_ONLY),
    # ---- Auto ----
    "MARUTI":     _t("Maruti Suzuki",      ["maruti suzuki"],                          "Auto", _NIFTY_SENSEX),
    "TATAMOTORS": _t("Tata Motors",        ["tata motors"],                            "Auto", _NIFTY_SENSEX),
    "M&M":        _t("Mahindra & Mahindra",["mahindra & mahindra", "m&m auto"],        "Auto", _NIFTY_SENSEX),
    "BAJAJ-AUTO": _t("Bajaj Auto",         ["bajaj auto"],                             "Auto", _NIFTY_ONLY),
    "HEROMOTOCO": _t("Hero MotoCorp",      ["hero motocorp"],                          "Auto", _NIFTY_ONLY),
    "EICHERMOT":  _t("Eicher Motors",      ["eicher motors"],                          "Auto", _NIFTY_ONLY),
    "TVSMOTOR":   _t("TVS Motor",          ["tvs motor"],                              "Auto", _NIFTY_ONLY),
    # ---- Pharma / Healthcare ----
    "SUNPHARMA":  _t("Sun Pharma",         ["sun pharma"],                             "Pharma", _NIFTY_SENSEX),
    "DRREDDY":    _t("Dr Reddy's",         ["dr reddy", "dr. reddy"],                  "Pharma", _NIFTY_SENSEX),
    "CIPLA":      _t("Cipla",              ["cipla"],                                  "Pharma", _NIFTY_ONLY),
    "DIVISLAB":   _t("Divi's Lab",         ["divi's lab", "divis lab"],                "Pharma", _NIFTY_ONLY),
    "APOLLOHOSP": _t("Apollo Hospitals",   ["apollo hospitals"],                       "Pharma", _NIFTY_ONLY),
    # ---- FMCG / Household ----
    "HINDUNILVR": _t("Hindustan Unilever", ["hindustan unilever", "hul india"],        "FMCG", _NIFTY_SENSEX),
    "ITC":        _t("ITC",                ["itc ltd"],                                "FMCG", _NIFTY_SENSEX),
    "NESTLEIND":  _t("Nestle India",       ["nestle india"],                           "FMCG", _NIFTY_SENSEX),
    "BRITANNIA":  _t("Britannia",          ["britannia"],                              "FMCG", _NIFTY_ONLY),
    "TATACONSUM": _t("Tata Consumer",      ["tata consumer"],                          "FMCG", _NIFTY_ONLY),
    # ---- Energy / Oil & Gas ----
    "RELIANCE":   _t("Reliance Industries",["reliance industries", "ril "],            "Energy", _NIFTY_SENSEX),
    "ONGC":       _t("ONGC",               ["ongc"],                                   "Energy", _NIFTY_ONLY),
    "BPCL":       _t("BPCL",               ["bpcl"],                                   "Energy", _NIFTY_ONLY),
    "IOC":        _t("Indian Oil",         ["indian oil corporation"],                 "Energy", _NIFTY_ONLY),
    "GAIL":       _t("GAIL India",         ["gail india"],                             "Energy", _NIFTY_ONLY),
    # ---- Metals ----
    "TATASTEEL":  _t("Tata Steel",         ["tata steel"],                             "Metals", _NIFTY_SENSEX),
    "JSWSTEEL":   _t("JSW Steel",          ["jsw steel"],                              "Metals", _NIFTY_ONLY),
    "HINDALCO":   _t("Hindalco",           ["hindalco"],                               "Metals", _NIFTY_ONLY),
    "VEDL":       _t("Vedanta",            ["vedanta"],                                "Metals", _NIFTY_ONLY),
    "COALINDIA":  _t("Coal India",         ["coal india"],                             "Metals", _NIFTY_ONLY),
    # ---- Realty ----
    "DLF":        _t("DLF",                ["dlf"],                                    "Realty", _NIFTY_ONLY),
    "GODREJPROP": _t("Godrej Properties",  ["godrej properties"],                      "Realty", _NIFTY_ONLY),
    "OBEROIRLTY": _t("Oberoi Realty",      ["oberoi realty"],                          "Realty", _NIFTY_ONLY),
    # ---- Telecom ----
    "BHARTIARTL": _t("Bharti Airtel",      ["bharti airtel"],                          "Telecom", _NIFTY_SENSEX),
    "IDEA":       _t("Vodafone Idea",      ["vodafone idea", "vi india"],              "Telecom", _NIFTY_ONLY),
    # ---- Cement ----
    "ULTRACEMCO": _t("UltraTech Cement",   ["ultratech cement"],                       "Cement", _NIFTY_SENSEX),
    "SHREECEM":   _t("Shree Cement",       ["shree cement"],                           "Cement", _NIFTY_ONLY),
    "GRASIM":     _t("Grasim Industries",  ["grasim industries"],                      "Cement", _NIFTY_SENSEX),
    # ---- Power ----
    "NTPC":       _t("NTPC",               ["ntpc"],                                   "Power", _NIFTY_SENSEX),
    "POWERGRID":  _t("Power Grid",         ["power grid"],                             "Power", _NIFTY_SENSEX),
    "TATAPOWER":  _t("Tata Power",         ["tata power"],                             "Power", _NIFTY_ONLY),
    "ADANIPOWER": _t("Adani Power",        ["adani power"],                            "Power", _NIFTY_ONLY),
    # ---- Misc Sensex / Nifty 50 ----
    "ASIANPAINT": _t("Asian Paints",       ["asian paints"],                           "FMCG", _NIFTY_SENSEX),
    "TITAN":      _t("Titan Company",      ["titan company"],                          "FMCG", _NIFTY_SENSEX),
    "LT":         _t("Larsen & Toubro",    ["larsen & toubro", "l&t"],                 "Power", _NIFTY_SENSEX),
    "ADANIENT":   _t("Adani Enterprises",  ["adani enterprises"],                      "Energy", _NIFTY_ONLY),
    "ADANIPORTS": _t("Adani Ports",        ["adani ports"],                            "Energy", _NIFTY_SENSEX),
}

# Build (lowercased phrase) → ticker symbol lookup. Each ticker is also
# matchable by its own symbol so "INFY" / "HDFCBANK" in a tweet works.
_TICKER_LOOKUP = {}
for _sym, _entry in STOCK_TICKERS.items():
    _TICKER_LOOKUP[_sym.lower()] = _sym
    for _alias in _entry["aliases"]:
        _TICKER_LOOKUP[_alias.lower()] = _sym

_RE_TICKERS = _compile_word_set(_TICKER_LOOKUP.keys())


# =========================
# SCORING
# =========================
def _capped(matches):
    """Cap a regex match list at MAX_HITS_PER_CATEGORY to defeat
    repetition-spam (e.g. 'war war war war war')."""
    return min(MAX_HITS_PER_CATEGORY, len(matches))


def get_news_score(text):
    """Score a news item by category-keyword matches.

    HIGH_IMPACT and GEOPOLITICAL only count when the text contains a
    HARD_ANCHOR — an unambiguous market term like 'nifty', 'rbi', 'rupee'.
    This prevents pure war / sanctions / fed-policy articles from scoring
    HIGH for an algo trader when they have no observable market hook,
    while still letting "war hits crude oil → nifty" trigger correctly.
    """
    if not text:
        return 0
    has_anchor = bool(_RE_HARD_ANCHOR.search(text))

    score  = 0
    score += 1 * _capped(_RE_KEYWORDS.findall(text))
    score += 1 * _capped(_RE_FINANCIAL.findall(text))
    score += 2 * _capped(_RE_BULLISH.findall(text))
    score += 2 * _capped(_RE_BEARISH.findall(text))
    if has_anchor:
        score += 5 * _capped(_RE_HIGH_IMPACT.findall(text))
        score += 2 * _capped(_RE_GEOPOLITICAL.findall(text))
    return score


def get_impact(score):
    if score >= 12:
        return "CRITICAL"
    if score >= 6:
        return "HIGH"
    if score >= 3:
        return "MEDIUM"
    return "LOW"


# =========================
# CATEGORIZATION / TICKER EXTRACTION / RELEVANCE FILTER
# =========================
# Sectors that are too broad to count as "specific" for the relevance
# multiplier. A pure RBI-policy story matches Macro and is kept, but it
# doesn't get the same boost as "Infosys raises FY guidance".
_BROAD_SECTORS = frozenset({"Macro", "Geopolitical"})


def categorize_item(text):
    """Return list of sectors whose vocabulary appears in `text`.

    Order is stable (SECTOR_KEYWORDS insertion order) so the persisted JSON
    is deterministic across runs.
    """
    if not text:
        return []
    return [name for name, pat in _RE_SECTORS.items() if pat.search(text)]


def extract_tickers(text):
    """Return deduped NSE symbols mentioned in `text`.

    Matches ticker aliases ("hdfc bank") and the bare symbol ("HDFCBANK")
    via _RE_TICKERS, then maps each match back to its canonical symbol via
    _TICKER_LOOKUP. Preserves first-seen order.
    """
    if not text:
        return []
    seen = []
    seen_set = set()
    for match in _RE_TICKERS.findall(text):
        sym = _TICKER_LOOKUP.get(match.lower())
        if sym and sym not in seen_set:
            seen.append(sym)
            seen_set.add(sym)
    return seen


def compute_affected_indices(sectors, tickers):
    """Union of indices touched by the matched sectors and tickers.

    Returned sorted for stable JSON output.
    """
    out = set()
    for s in sectors:
        out.update(SECTOR_INDICES.get(s, ()))
    for t in tickers:
        entry = STOCK_TICKERS.get(t)
        if entry:
            out.update(entry["indices"])
    return sorted(out)


def relevance_multiplier(sectors, tickers):
    """How specific is this item? Specific stock > named sector > pure macro.

    Both ticker-tagged and pure-macro items survive the filter; this just
    nudges the final score so concrete news outranks broad headlines.
    """
    if tickers:
        return 1.20
    specific = [s for s in sectors if s not in _BROAD_SECTORS]
    if specific:
        return 1.10
    if sectors:  # only Macro / Geopolitical matched
        return 0.90
    return 1.00


def is_target_relevant(affected_indices):
    """True iff this item touches one of NIFTY/BANKNIFTY/FINNIFTY/SENSEX."""
    return any(idx in TARGET_INDICES for idx in affected_indices)


_REDDIT_QUESTION_RE = re.compile(
    r"submitted by /u/|"
    r"\[link\]\s*\[comments\]|"
    r"open question for the community|"
    r"what do you think\??|"
    r"thoughts\??$|"
    r"how meaningful are these|"
    r"anyone else notice",
    re.IGNORECASE,
)


def _is_reddit_question(item):
    """True if this looks like a Reddit community discussion post rather than
    actual market news. Checks title, summary, and content."""
    if item.get("source", "").lower() != "reddit":
        return False
    blob = f"{item.get('title', '')} {item.get('summary', '')} {item.get('content', '')}"
    return bool(_REDDIT_QUESTION_RE.search(blob))


def filter_relevant_items(items):
    """Drop noise after enrichment. Keeps only items that are
    (a) at least MEDIUM impact, (b) tied to one of the four target
    indices, and (c) not a Reddit community question/discussion post.
    Logs per-reason drop counts so we can spot a vocabulary
    regression at a glance.
    """
    if not items:
        return items
    kept = []
    dropped_low = 0
    dropped_off_target = 0
    dropped_reddit_q = 0
    for it in items:
        if it.get("impact") == "LOW":
            dropped_low += 1
            continue
        if not is_target_relevant(it.get("affected_indices") or []):
            dropped_off_target += 1
            continue
        if _is_reddit_question(it):
            dropped_reddit_q += 1
            continue
        kept.append(it)
    log.info(
        "filter_relevant_items: kept %d / %d (dropped %d low-impact, %d off-target, %d reddit-question)",
        len(kept), len(items), dropped_low, dropped_off_target, dropped_reddit_q,
    )
    return kept


# =========================
# BUILD ITEM
# =========================
def _utc_iso_now():
    """Return current UTC time as an ISO-8601 string ending in Z."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_news_item(source, title, link, summary="", published=""):
    # Some RSS feeds (Google News in particular) wrap the title in HTML
    # tags as the "summary". Strip those so the stored summary is clean text.
    summary = _clean_summary_html(summary) if summary else ""
    combined = f"{title} {summary}".strip()
    compound, label = score_text(combined)
    score = get_news_score(combined)
    now_iso = _utc_iso_now()
    return {
        "source": source,
        "title": title,
        "summary": summary,
        "link": link,
        "published": published,
        "timestamp": now_iso,
        "ingested_at": now_iso,
        "score": score,
        "impact": get_impact(score),
        "sentiment": compound,
        "sentiment_label": label,
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
# Video pages under /news/videos/ are extraction-hostile (the body is just
# a video player + sidebar of unrelated headlines), so they pollute scoring.
_MC_VIDEO_RE = re.compile(
    r"^https?://(?:www\.)?moneycontrol\.com/news/videos?/",
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
            if _MC_VIDEO_RE.match(link):
                continue
            if link in seen_news:
                continue
            fp = _title_fingerprint(title)
            if fp and fp in seen_titles:
                continue
            news_list.append(build_news_item("Moneycontrol", title, link))
            seen_news.add(link)
            if fp:
                seen_titles.add(fp)
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
                title = entry.get("title", "")
                fp = _title_fingerprint(title)
                if fp and fp in seen_titles:
                    continue
                news_list.append(build_news_item(
                    "Reuters",
                    title,
                    link,
                    entry.get("summary", ""),
                    entry.get("published", ""),
                ))
                seen_news.add(link)
                if fp:
                    seen_titles.add(fp)
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
                # Match the guest path: drop retweets and replies.
                tweets = [
                    tw for tw in tweets
                    if not getattr(tw, "retweetedTweet", None)
                    and not getattr(tw, "inReplyToTweetId", None)
                ]
                for tw in tweets:
                    text = tw.rawContent or ""
                    link = f"https://x.com/{username}/status/{tw.id}"
                    if not text or link in seen_news:
                        continue
                    title = f"@{username}: {text[:120]}"
                    fp = _title_fingerprint(title)
                    if fp and fp in seen_titles:
                        continue
                    items.append(build_news_item(
                        "Twitter",
                        title,
                        link,
                        text,
                        str(tw.date) if tw.date else "",
                    ))
                    seen_news.add(link)
                    if fp:
                        seen_titles.add(fp)
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
_TW_BEARER = os.getenv("TW_GUEST_BEARER", "")
if not _TW_BEARER:
    log.warning(
        "TW_GUEST_BEARER not set — Twitter guest-token path will be skipped. "
        "Set TW_GUEST_BEARER (web app bearer) to enable it."
    )
_guest_token_cache = {"token": None, "ts": 0}
_guest_token_lock  = threading.Lock()


def _get_guest_token():
    if not _TW_BEARER:
        return None
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
            title = f"@{username}: {text[:120]}"
            fp = _title_fingerprint(title)
            if fp and fp in seen_titles:
                continue
            items.append(build_news_item(
                "Twitter",
                title,
                link,
                text,
                "",
            ))
            seen_news.add(link)
            if fp:
                seen_titles.add(fp)
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
            fp = _title_fingerprint(title)
            if fp and fp in seen_titles:
                continue
            items.append(build_news_item(
                source, title, link,
                entry.get("summary", ""),
                entry.get("published", ""),
            ))
            seen_news.add(link)
            if fp:
                seen_titles.add(fp)
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


# Lines / phrases that show up as boilerplate or navigation in extracted
# article bodies. We strip any line containing one of these substrings
# (case-insensitive). Keep this list short — match strings, not patterns,
# so adding a publisher takes one line and zero regex thinking.
_BOILERPLATE_NEEDLES = (
    "find the best of al news",
    "stay on top of the latest tech",
    "explore 230+ exclusive editorials",
    "uncover insights from world-renowned",
    "exclusive live webinars",
    "you are already a moneycontrol pro",
    "moneycontrol pro panorama",
    "see the top gainers, losers",
    "discover the secret world of unlisted",
    "recommended stories",
    "read more:",
    "also read:",
    "first published:",
    "subscribe to our newsletter",
    "follow us on",
    "sign up for our newsletter",
    "click here to",
    "watch the video",
    "a collection of the most-viewed",
    "track the latest updates",
    # Al Jazeera "list of N items" sidebar markers
    "list of 3 items",
    "list of 4 items",
    "list of 5 items",
    "list 1 of",
    "list 2 of",
    "list 3 of",
    "list 4 of",
)

# Trafilatura sometimes leaks Moneycontrol's PHP debug array dump.
_MC_PHP_DUMP_RE = re.compile(r"Array\s*\(\s*\[direction\].*?\)", re.DOTALL)


def _clean_extracted_body(text):
    """Strip boilerplate lines and dedupe paragraphs from an extracted body.

    Used for both trafilatura output and the BS4 fallback so the scoring
    text is article-only (no recommended-stories sidebar, no newsletter
    spam, no repeated promo paragraphs)."""
    if not text:
        return ""
    text = _MC_PHP_DUMP_RE.sub("", text)
    seen = set()
    kept = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        low = line.lower()
        if any(needle in low for needle in _BOILERPLATE_NEEDLES):
            continue
        # Dedupe paragraphs by normalized hash so a "more videos" block
        # listing the same headline 3× collapses to 1.
        key = re.sub(r"\s+", " ", low)
        if key in seen:
            continue
        seen.add(key)
        kept.append(line)
    return "\n".join(kept).strip()


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
                body = _clean_extracted_body(body.strip())
                if len(body.split()) >= MIN_BODY_WORDS:
                    return body[:MAX_BODY_CHARS]
        except Exception as e:
            log.debug("trafilatura failed for %s: %s", host, e)
        # Fallback: BS4 paragraph join (with same cleaning).
        soup = BeautifulSoup(res.text, "html.parser")
        paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        joined = "\n".join(p for p in paras if len(p) > 40)
        text = _clean_extracted_body(joined)
        if text and len(text.split()) >= MIN_BODY_WORDS:
            return text[:MAX_BODY_CHARS]
    except Exception as e:
        log.debug("extract_article_body %s: %s", host, e)
    return ""


def _clean_summary_html(text):
    """Strip HTML tags + extra whitespace from RSS summary fields, then
    NFKC-normalize so smart quotes / NBSPs / ligatures don't poison
    downstream tokenization."""
    if not text:
        return ""
    try:
        cleaned = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
        cleaned = unicodedata.normalize("NFKC", cleaned)
        return re.sub(r"\s+", " ", cleaned).strip()
    except Exception:
        return text


# =========================
# DATE / FRESHNESS FILTER
# =========================
def _as_utc(dt):
    """Force a datetime to tz-aware UTC. Naive datetimes are assumed UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_item_date(item):
    """Best-effort parse of an item's age, always returning tz-aware UTC.
    Falls back to its 'ingested_at'/'timestamp' field (set when we fetched
    it), so freshly-fetched items always survive."""
    from email.utils import parsedate_to_datetime
    pub = item.get("published") or ""
    if pub:
        try:
            dt = parsedate_to_datetime(pub)
            if dt is not None:
                return _as_utc(dt)
        except Exception:
            pass
        try:
            import dateparser
            dt = dateparser.parse(pub)
            if dt is not None:
                return _as_utc(dt)
        except Exception:
            pass
    for key in ("ingested_at", "timestamp"):
        ts = item.get(key) or ""
        if not ts:
            continue
        try:
            # ISO-8601 with trailing Z is the new format.
            return _as_utc(datetime.fromisoformat(ts.replace("Z", "+00:00")))
        except Exception:
            pass
    return None


def _is_recent(item, max_age_days=MAX_ITEM_AGE_DAYS):
    """Return True if the item's date is within max_age_days, OR if we
    cannot determine the date at all (don't drop unknowns)."""
    dt = _parse_item_date(item)
    if dt is None:
        return True
    return (datetime.now(timezone.utc) - dt) <= timedelta(days=max_age_days)


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
    now_utc = datetime.now(timezone.utc)
    for item in items:
        combined = f"{item.get('title','')} {item['content']}"
        raw_score = get_news_score(combined)

        # Source credibility multiplier (Reuters > ET > Reddit, etc.).
        weight = SOURCE_WEIGHT.get(item.get("source", ""), DEFAULT_SOURCE_WEIGHT)

        # Time decay anchored on ingested_at (falls back to timestamp).
        decay = 1.0
        ingested = item.get("ingested_at") or item.get("timestamp") or ""
        if ingested:
            try:
                dt = _as_utc(datetime.fromisoformat(ingested.replace("Z", "+00:00")))
                age_hours = max(0.0, (now_utc - dt).total_seconds() / 3600.0)
                decay = math.exp(-age_hours / DECAY_TAU_HOURS)
            except Exception:
                pass

        # Sector / ticker / index tagging — feeds the post-enrichment
        # relevance filter and gives the frontend something to render.
        sectors  = categorize_item(combined)
        tickers  = extract_tickers(combined)
        affected = compute_affected_indices(sectors, tickers)
        rel_mult = relevance_multiplier(sectors, tickers)

        final_score = round(raw_score * weight * decay * rel_mult, 2)
        item["score_raw"]            = raw_score
        item["score"]                = final_score
        item["impact"]               = get_impact(final_score)
        item["categories"]           = sectors
        item["tickers"]              = tickers
        item["affected_indices"]     = affected
        item["relevance_multiplier"] = rel_mult

        # Title-weighted sentiment: titles are clean and directional;
        # bodies often dilute with boilerplate / mixed signals. Blend the
        # two so a clearly-bullish headline isn't drowned by 800 words of
        # neutral filler.
        title = item.get("title", "")
        title_compound, title_label = score_text(title)
        body_compound,  body_label  = score_text(item.get("content", ""))
        if title and item.get("content"):
            compound = round(0.6 * title_compound + 0.4 * body_compound, 4)
        else:
            compound = title_compound or body_compound
        if compound > 0.15:
            label = "bullish"
        elif compound < -0.15:
            label = "bearish"
        else:
            label = "neutral"
        item["sentiment"]       = compound
        item["sentiment_label"] = label

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
        existing = []
        if os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE, "r") as f:
                    loaded = json.load(f)
                if isinstance(loaded, list):
                    existing = loaded
                else:
                    raise ValueError(f"OUTPUT_FILE root is {type(loaded).__name__}, expected list")
            except Exception as e:
                # Don't silently nuke history. Quarantine the bad file so an
                # operator can recover it, then start fresh.
                quarantine = (
                    OUTPUT_FILE + ".corrupt-"
                    + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                )
                try:
                    os.rename(OUTPUT_FILE, quarantine)
                    log.error(
                        "OUTPUT_FILE unreadable (%s) — quarantined to %s",
                        e, quarantine,
                    )
                except Exception as rename_err:
                    log.error(
                        "OUTPUT_FILE unreadable (%s) AND quarantine failed (%s) — "
                        "starting empty", e, rename_err,
                    )
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
# NODE.JS WEBSOCKET PUSH
# =========================
_ws_conn = None
_ws_lock = threading.Lock()


def _get_ws():
    """Lazy-connect to the Node uWebSockets.js server. Returns the live
    connection or None. Thread-safe; reuses the existing socket if alive."""
    global _ws_conn
    with _ws_lock:
        if _ws_conn and _ws_conn.connected:
            return _ws_conn
        try:
            _ws_conn = websocket.create_connection(NODE_WS_URL, timeout=10)
            log.info("WebSocket connected to Node at %s", NODE_WS_URL)
            return _ws_conn
        except Exception as e:
            log.warning("WebSocket connect to Node failed: %s", e)
            _ws_conn = None
            return None


def push_to_node(batch):
    """Send a batch of enriched news items to the Node backend via WebSocket.

    Retries up to NODE_PUSH_RETRIES times with exponential backoff.  Failures
    are logged but never raised — items are already safe in market_news.json
    and the Node app can backfill via GET /news on its next startup.
    """
    if not batch or not NODE_WS_URL:
        return

    # Shallow-copy so adding title_fingerprint doesn't mutate the originals
    # (they may still be used by the SocketIO emit that follows).
    payload = []
    for item in batch:
        doc = dict(item)
        fp = _title_fingerprint(doc.get("title", ""))
        if fp:
            doc["title_fingerprint"] = fp
        payload.append(doc)

    message = json.dumps({"type": "news_batch", "news": payload})

    for attempt in range(1, NODE_PUSH_RETRIES + 1):
        ws = _get_ws()
        if ws is None:
            if attempt < NODE_PUSH_RETRIES:
                time.sleep(2 ** (attempt - 1))
            continue
        try:
            ws.send(message)
            log.info("Pushed %d items to Node via WebSocket", len(payload))
            return
        except Exception as e:
            log.warning(
                "WebSocket send failed (attempt %d/%d): %s",
                attempt, NODE_PUSH_RETRIES, e,
            )
            # Close the dead socket so _get_ws() reconnects next time.
            with _ws_lock:
                global _ws_conn
                try:
                    _ws_conn.close()
                except Exception:
                    pass
                _ws_conn = None
            if attempt < NODE_PUSH_RETRIES:
                time.sleep(2 ** (attempt - 1))

    log.error(
        "Failed to push %d items to Node after %d attempts — items saved locally",
        len(payload), NODE_PUSH_RETRIES,
    )


# =========================
# POSTGRESQL PERSISTENCE
# =========================
_pg_conns = {}   # dsn -> connection
_pg_lock = threading.Lock()

_PG_UPSERT_SQL = """
    INSERT INTO market_news (
        source, title, summary, link, content,
        published, timestamp, ingested_at,
        score_raw, score, impact,
        sentiment, sentiment_label,
        categories, tickers, affected_indices,
        relevance_multiplier, title_fingerprint
    ) VALUES (
        %(source)s, %(title)s, %(summary)s, %(link)s, %(content)s,
        %(published)s, %(timestamp)s, %(ingested_at)s,
        %(score_raw)s, %(score)s, %(impact)s,
        %(sentiment)s, %(sentiment_label)s,
        %(categories)s, %(tickers)s, %(affected_indices)s,
        %(relevance_multiplier)s, %(title_fingerprint)s
    )
    ON CONFLICT (link) DO UPDATE SET
        score_raw           = EXCLUDED.score_raw,
        score               = EXCLUDED.score,
        impact              = EXCLUDED.impact,
        sentiment           = EXCLUDED.sentiment,
        sentiment_label     = EXCLUDED.sentiment_label,
        categories          = EXCLUDED.categories,
        tickers             = EXCLUDED.tickers,
        affected_indices    = EXCLUDED.affected_indices,
        relevance_multiplier = EXCLUDED.relevance_multiplier,
        content             = EXCLUDED.content
"""


def _get_pg(dsn):
    """Return a live PostgreSQL connection for *dsn*, reconnecting if needed."""
    with _pg_lock:
        conn = _pg_conns.get(dsn)
        if conn and not conn.closed:
            try:
                conn.cursor().execute("SELECT 1")
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                _pg_conns.pop(dsn, None)
        if not PSYCOPG2_AVAILABLE:
            return None
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            _pg_conns[dsn] = conn
            log.info("PostgreSQL connected via %s", dsn.split("@")[-1])
            return conn
        except Exception as e:
            log.warning("PostgreSQL connect failed (%s): %s", dsn.split("@")[-1], e)
            return None


def _parse_ts(val):
    """Best-effort parse a timestamp string to datetime, or None."""
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _build_pg_rows(news):
    """Convert enriched news dicts into rows suitable for the upsert SQL."""
    rows = []
    for item in news:
        fp = _title_fingerprint(item.get("title", ""))
        rows.append({
            "source":               item.get("source", ""),
            "title":                item.get("title", ""),
            "summary":              item.get("summary", ""),
            "link":                 item.get("link", ""),
            "content":              (item.get("content") or "")[:MAX_BODY_CHARS],
            "published":            _parse_ts(item.get("published")),
            "timestamp":            _parse_ts(item.get("timestamp")),
            "ingested_at":          _parse_ts(item.get("ingested_at")) or datetime.now(timezone.utc),
            "score_raw":            float(item.get("score_raw", 0)),
            "score":                float(item.get("score", 0)),
            "impact":               item.get("impact", "LOW"),
            "sentiment":            float(item.get("sentiment", 0)),
            "sentiment_label":      item.get("sentiment_label", "neutral"),
            "categories":           item.get("categories", []),
            "tickers":              item.get("tickers", []),
            "affected_indices":     item.get("affected_indices", []),
            "relevance_multiplier": float(item.get("relevance_multiplier", 1.0)),
            "title_fingerprint":    fp,
        })
    return rows


def _upsert_pg(dsn, rows, label):
    """Upsert rows into a single PG instance identified by *dsn*."""
    conn = _get_pg(dsn)
    if conn is None:
        return
    try:
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, _PG_UPSERT_SQL, rows, page_size=100)
        conn.commit()
        log.info("PostgreSQL [%s]: upserted %d items", label, len(rows))
    except Exception as e:
        log.error("PostgreSQL [%s] upsert failed: %s", label, e)
        try:
            conn.rollback()
        except Exception:
            pass


def save_news_to_pg(news):
    """Upsert enriched news items directly into PostgreSQL.

    Kept for the on-demand /scrape route and for admin/debug use. The
    normal real-time path goes through process_news_queue() instead so
    that fuzzy dedupe and pointer-based delta reads can run in one place.
    """
    if not news or not PSYCOPG2_AVAILABLE:
        return
    rows = _build_pg_rows(news)
    _upsert_pg(PG_DSN, rows, "local")


# =========================
# QUEUE PROCESSOR (JSON -> PG)
# =========================
_queue_lock = threading.Lock()
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _read_queue_pointer():
    """Return the high-water mark datetime of the last processed batch."""
    try:
        with open(QUEUE_POINTER_FILE, "r") as f:
            ts = f.read().strip()
        parsed = _parse_ts(ts)
        if parsed is None:
            return _EPOCH
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except FileNotFoundError:
        return _EPOCH
    except Exception as e:
        log.warning("Queue pointer read failed (%s) — restarting from epoch", e)
        return _EPOCH


def _write_queue_pointer(ts):
    """Persist the latest processed ingested_at atomically."""
    try:
        tmp = QUEUE_POINTER_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(ts.isoformat())
        os.replace(tmp, QUEUE_POINTER_FILE)
    except Exception as e:
        log.error("Queue pointer write failed: %s", e)


def _similarity(a, b):
    """Return a 0..1 ratio between two short strings with a length gate.

    SequenceMatcher is O(N*M); titles are short enough for this not to
    matter, but the length gate short-circuits mismatches instantly.
    """
    if not a or not b:
        return 0.0
    la, lb = len(a), len(b)
    m = max(la, lb)
    if m == 0:
        return 0.0
    if min(la, lb) / m < 0.80:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower(), autojunk=False).ratio()


def _fetch_existing_links(links):
    """Return the subset of *links* already present in market_news."""
    if not links or not PSYCOPG2_AVAILABLE:
        return set()
    conn = _get_pg(PG_DSN)
    if conn is None:
        return set()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT link FROM market_news WHERE link = ANY(%s)",
            (list(links),),
        )
        return {row[0] for row in cur.fetchall()}
    except Exception as e:
        log.error("Queue: fetch existing links failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return set()


def _fetch_recent_titles(source, days):
    """Return titles ingested within the last *days* for *source*."""
    if not source or not PSYCOPG2_AVAILABLE:
        return []
    conn = _get_pg(PG_DSN)
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT title FROM market_news "
            "WHERE source = %s "
            "  AND ingested_at > NOW() - (%s || ' days')::interval",
            (source, str(days)),
        )
        return [row[0] for row in cur.fetchall() if row[0]]
    except Exception as e:
        log.error("Queue: fetch recent titles failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def process_news_queue():
    """Process the JSON backlog into PostgreSQL.

    Pipeline:
      1. Read queue.pointer (high-water mark).
      2. Load market_news.json and take items with ingested_at > pointer.
      3. Bulk-check which links already exist in DB (exact match).
      4. Group by source; for each source, fetch recent titles once and
         drop incoming items whose title is >= SIMILARITY_THRESHOLD similar
         to any existing same-source title (fuzzy dedupe).
      5. Upsert the survivors.
      6. Advance the pointer to max(ingested_at) of the delta.

    Idempotent — a crashed run just re-reads the same delta next cycle.
    """
    if not PSYCOPG2_AVAILABLE:
        return
    with _queue_lock:
        pointer = _read_queue_pointer()
        try:
            with open(OUTPUT_FILE, "r") as f:
                items = json.load(f)
        except FileNotFoundError:
            return
        except Exception as e:
            log.warning("Queue: JSON read failed: %s", e)
            return
        if not isinstance(items, list):
            return

        delta = []
        max_ts = pointer
        for it in items:
            if not isinstance(it, dict):
                continue
            ts = _parse_ts(it.get("ingested_at"))
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts <= pointer:
                continue
            delta.append(it)
            if ts > max_ts:
                max_ts = ts

        if not delta:
            return

        log.info("Queue: %d new items since %s", len(delta), pointer.isoformat())

        # Bulk exact-match link check for the whole batch.
        existing_links = _fetch_existing_links(
            [i.get("link") for i in delta if i.get("link")]
        )

        # Group by source so same-source titles are fetched once per source.
        by_source = {}
        for it in delta:
            by_source.setdefault(it.get("source", ""), []).append(it)

        accepted = []
        exact_skipped = 0
        fuzzy_skipped = 0
        for source, src_items in by_source.items():
            recent_titles = _fetch_recent_titles(source, SIMILARITY_WINDOW_DAYS)
            for it in src_items:
                link = it.get("link")
                if link and link in existing_links:
                    exact_skipped += 1
                    continue
                title = (it.get("title") or "").strip()
                if not title:
                    continue
                dup = False
                for t in recent_titles:
                    if _similarity(title, t) >= SIMILARITY_THRESHOLD:
                        fuzzy_skipped += 1
                        dup = True
                        break
                if dup:
                    continue
                accepted.append(it)
                # Block near-duplicates inside the same batch too.
                recent_titles.append(title)

        if accepted:
            rows = _build_pg_rows(accepted)
            _upsert_pg(PG_DSN, rows, "local")

        log.info(
            "Queue: accepted=%d exact_dupes=%d fuzzy_dupes=%d",
            len(accepted), exact_skipped, fuzzy_skipped,
        )

        _write_queue_pointer(max_ts)


def background_queue_processor(interval=None):
    """Daemon loop: drain the JSON backlog into PostgreSQL periodically."""
    interval = interval or QUEUE_INTERVAL
    log.info("Queue processor started (interval=%ss, threshold=%.2f)...",
             interval, SIMILARITY_THRESHOLD)
    while True:
        try:
            process_news_queue()
        except Exception as e:
            log.error("Queue processor error: %s", e)
        time.sleep(interval)


# =========================
# RETENTION (10-day window)
# =========================
def prune_old_news():
    """Delete rows older than RETENTION_DAYS from market_news."""
    if not PSYCOPG2_AVAILABLE:
        return
    conn = _get_pg(PG_DSN)
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM market_news "
            "WHERE ingested_at < NOW() - (%s || ' days')::interval",
            (str(RETENTION_DAYS),),
        )
        deleted = cur.rowcount
        conn.commit()
        log.info("Retention: deleted %d rows older than %d days", deleted, RETENTION_DAYS)
    except Exception as e:
        log.error("Retention delete failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass


def background_retention(interval=None):
    """Daemon loop: periodically prune old rows from PostgreSQL."""
    interval = interval or RETENTION_INTERVAL
    log.info("Retention worker started (interval=%ss, keep=%d days)...",
             interval, RETENTION_DAYS)
    while True:
        try:
            prune_old_news()
        except Exception as e:
            log.error("Retention worker error: %s", e)
        time.sleep(interval)


# =========================
# BACKGROUND SCRAPER
# =========================
def background_scraper(socketio, interval=120):
    log.info("Real-Time Scraper Started (interval=%ss)...", interval)
    consecutive_failures = 0
    while True:
        try:
            news = fetch_news()
            if news:
                news = enrich_items(news)
                news = filter_relevant_items(news)
                if news:
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
    """Pre-populate seen_news + seen_titles with items already on disk so a
    restart doesn't re-fetch / re-emit articles we already have. Also
    warms the sentiment model so the first scrape cycle isn't penalized
    by FinBERT's ~440MB cold start."""
    try:
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            link_count = 0
            title_count = 0
            for it in data:
                if not isinstance(it, dict):
                    continue
                link = it.get("link")
                if link:
                    seen_news.add(link)
                    link_count += 1
                fp = _title_fingerprint(it.get("title", ""))
                if fp:
                    seen_titles.add(fp)
                    title_count += 1
            log.info(
                "Bootstrapped seen_news (%d links) and seen_titles (%d titles)",
                link_count, title_count,
            )
    except FileNotFoundError:
        pass
    except Exception as e:
        log.debug("seen_news bootstrap skipped: %s", e)

    # Eagerly load the FinBERT pipeline so the first scrape cycle is fast.
    try:
        warm_pipeline()
    except Exception as e:
        log.warning("FinBERT warm-up failed: %s", e)


_bootstrap_seen_from_disk()
