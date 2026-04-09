"""Financial sentiment scoring backed by FinBERT (ProsusAI/finbert).

Replaces VADER, which is trained on social media and misses financial
domain language ("rate cut", "FII selling", etc.).

Exposes:
    score_text(text) -> (compound in [-1, 1], label in {bullish, bearish, neutral})
    warm_pipeline()  -> force model load (call once at startup)
"""

import logging
import threading

log = logging.getLogger("scraper.sentiment")

_pipeline = None
_lock = threading.Lock()


def _get_pipeline():
    """Lazy singleton. First call loads ~440MB into memory."""
    global _pipeline
    with _lock:
        if _pipeline is not None:
            return _pipeline
        from transformers import pipeline
        _pipeline = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
            truncation=True,
            max_length=512,
        )
        log.info("FinBERT pipeline loaded")
        return _pipeline


def warm_pipeline():
    """Trigger the model load eagerly so the first scoring call is fast."""
    _get_pipeline()


def score_text(text):
    """Return (compound, label).

    compound: float in [-1, 1] — confidence-weighted polarity.
    label:    'bullish' | 'bearish' | 'neutral'.
    """
    if not text or not text.strip():
        return 0.0, "neutral"
    try:
        result = _get_pipeline()(text[:2000])[0]
    except Exception as e:
        log.debug("FinBERT score failed: %s", e)
        return 0.0, "neutral"

    label = (result.get("label") or "").lower()
    confidence = float(result.get("score", 0.0))
    if label == "positive":
        return confidence, "bullish"
    if label == "negative":
        return -confidence, "bearish"
    return 0.0, "neutral"
