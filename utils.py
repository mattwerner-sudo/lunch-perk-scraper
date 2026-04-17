"""
Shared utilities: HTTP fetching, text matching, deduplication.
"""
import re
import time
import random
import logging
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import FOOD_KEYWORDS, NYC_SIGNALS, REQUEST_TIMEOUT, DELAY_BETWEEN_REQUESTS, MAX_RETRIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── HTTP session with automatic retries ─────────────────────────────────────
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


SESSION = build_session()


def get(url: str, **kwargs) -> Optional[requests.Response]:
    """GET with polite delay and error handling."""
    time.sleep(DELAY_BETWEEN_REQUESTS + random.uniform(0, 0.5))
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f"GET failed for {url}: {e}")
        return None


# ── Text matching ────────────────────────────────────────────────────────────
def find_food_keywords(text: str) -> list[str]:
    """
    Return food-perk keywords found in text, with context validation.

    For ambiguous keywords (doordash, grubhub, ubereats, forkable, sharebite)
    we require them to appear near perk-context words — not just anywhere in the
    text. This eliminates false positives like "we serve clients like DoorDash".
    """
    text_lower = text.lower()

    # Markers that indicate we're inside a benefits/perks section
    PERK_CONTEXT = [
        "benefit", "perk", "we offer", "you'll enjoy", "you will enjoy",
        "what we offer", "what you get", "compensation", "total rewards",
        "lunch", "meal", "food", "snack", "kitchen", "stipend", "credit",
        "catered", "complimentary", "free", "daily", "office life",
    ]

    # Platform names that are ambiguous — need perk context nearby
    NEEDS_CONTEXT = {
        "doordash", "grubhub", "ubereats", "uber eats",
        "forkable", "sharebite",
    }

    matched = []
    for kw in FOOD_KEYWORDS:
        if kw not in text_lower:
            continue

        if kw not in NEEDS_CONTEXT:
            # Non-ambiguous keyword (e.g. "free lunch", "catered meals") — accept as-is
            matched.append(kw)
            continue

        # For ambiguous platform names, find all occurrences and check context
        idx = 0
        found_with_context = False
        while True:
            pos = text_lower.find(kw, idx)
            if pos == -1:
                break
            # Look at a window of ±300 chars around the keyword
            window_start = max(0, pos - 300)
            window_end = min(len(text_lower), pos + len(kw) + 300)
            window = text_lower[window_start:window_end]
            if any(ctx in window for ctx in PERK_CONTEXT):
                found_with_context = True
                break
            idx = pos + 1

        if found_with_context:
            matched.append(kw)

    return matched


def is_nyc(text: str) -> bool:
    """Return True if text contains any NYC location signal."""
    text_lower = text.lower()
    return any(sig in text_lower for sig in NYC_SIGNALS)


def excerpt(text: str, keyword: str, window: int = 120) -> str:
    """Return a short excerpt around the first occurrence of keyword."""
    idx = text.lower().find(keyword.lower())
    if idx == -1:
        return ""
    start = max(0, idx - window // 2)
    end = min(len(text), idx + len(keyword) + window // 2)
    snip = text[start:end].strip()
    return f"...{snip}..."


def clean_text(html_or_text: str) -> str:
    """Strip HTML tags, decode entities, and collapse whitespace."""
    import html as html_lib
    text = re.sub(r"<[^>]+>", " ", html_or_text)
    text = html_lib.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
