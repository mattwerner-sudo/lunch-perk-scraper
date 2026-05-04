"""
ATS tenant discovery — enumerates every active company on Greenhouse, Lever, and Ashby.

Uses the CommonCrawl index API to find all URLs crawled under each ATS job board domain,
extracts company slugs, and caches them in SQLite for 7 days.

Greenhouse:  boards.greenhouse.io/*      (~15,000 companies)
Lever:       jobs.lever.co/*             (~20,000 companies)
Ashby:       jobs.ashbyhq.com/*          (~5,000 companies)

CommonCrawl index API:
  https://index.commoncrawl.org/{INDEX}-index?url={domain}/*&output=json&limit=15000&from={offset}
  Returns NDJSON lines; paginate with from= until results < limit.
  We query multiple recent indexes to maximize coverage.
"""
import re
import json
import logging
import sqlite3
import requests
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

DB_PATH    = Path(__file__).parent.parent / "lunch_perks.db"
CACHE_DAYS = 7
TIMEOUT    = 60

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# Recent CommonCrawl indexes (newest first — stop early once we have enough slugs)
CC_INDEXES = [
    "CC-MAIN-2025-13",
    "CC-MAIN-2024-51",
    "CC-MAIN-2024-42",
    "CC-MAIN-2024-33",
]

CC_URL = "https://index.commoncrawl.org/{index}-index"
CC_LIMIT = 15000
MIN_SLUGS = 500   # stop querying more indexes once we hit this

# ATS domain patterns for CommonCrawl queries
CC_DOMAINS = {
    "greenhouse": "boards.greenhouse.io/*",
    "lever":      "jobs.lever.co/*",
    "ashby":      "jobs.ashbyhq.com/*",
}

# Slug extraction regexes
SLUG_RE = {
    "greenhouse": re.compile(r'(?:boards|job-boards)\.greenhouse\.io/([a-zA-Z0-9_-]+)'),
    "lever":      re.compile(r'jobs\.lever\.co/([a-zA-Z0-9_-]+)'),
    "ashby":      re.compile(r'jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)'),
}

# Path segments that are platform infrastructure, not company slugs
_SKIP = {
    "greenhouse", "lever", "ashby", "jobs", "careers", "boards",
    "job-boards", "en", "us", "external", "site", "embed", "api",
    "v1", "v2", "v3", "embed", "apply", "confirmation",
}


# ── SQLite helpers ────────────────────────────────────────────────────────────

def _init():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ats_tenants (
                ats_type      TEXT NOT NULL,
                slug          TEXT NOT NULL,
                discovered_at TEXT NOT NULL,
                PRIMARY KEY (ats_type, slug)
            )
        """)


def _cache_fresh(ats_type: str) -> bool:
    cutoff = (date.today() - timedelta(days=CACHE_DAYS)).isoformat()
    try:
        with sqlite3.connect(DB_PATH) as con:
            n = con.execute(
                "SELECT COUNT(*) FROM ats_tenants WHERE ats_type=? AND discovered_at>=?",
                (ats_type, cutoff)
            ).fetchone()[0]
        return n > 100
    except Exception:
        return False


def _write(ats_type: str, slugs: list[str]):
    today = date.today().isoformat()
    with sqlite3.connect(DB_PATH) as con:
        con.executemany(
            "INSERT OR REPLACE INTO ats_tenants VALUES (?,?,?)",
            [(ats_type, s, today) for s in slugs],
        )


def _read(ats_type: str) -> list[str]:
    try:
        with sqlite3.connect(DB_PATH) as con:
            return [
                r[0] for r in con.execute(
                    "SELECT slug FROM ats_tenants WHERE ats_type=?", (ats_type,)
                ).fetchall()
            ]
    except Exception:
        return []


# ── CommonCrawl fetching ──────────────────────────────────────────────────────

def _fetch_cc_index(cc_index: str, domain_pattern: str) -> list[str]:
    """
    Fetch all URLs for domain_pattern from one CommonCrawl index.
    Paginates with from= until results < CC_LIMIT.
    Returns raw URL strings (not slugs).
    """
    urls: list[str] = []
    offset = 0

    while True:
        try:
            r = requests.get(
                CC_URL.format(index=cc_index),
                params={
                    "url":    domain_pattern,
                    "output": "json",
                    "limit":  CC_LIMIT,
                    "from":   offset,
                },
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            if r.status_code == 404:
                break
            if r.status_code != 200:
                log.debug(f"CC {cc_index} HTTP {r.status_code} for {domain_pattern}")
                break

            lines = [ln for ln in r.text.splitlines() if ln.strip()]
            if not lines:
                break

            batch_urls = []
            for line in lines:
                try:
                    obj = json.loads(line)
                    u = obj.get("url", "")
                    if u:
                        batch_urls.append(u)
                except Exception:
                    continue

            urls.extend(batch_urls)
            log.debug(f"CC {cc_index} offset={offset}: {len(batch_urls)} URLs")

            if len(lines) < CC_LIMIT:
                break
            offset += CC_LIMIT

        except Exception as e:
            log.debug(f"CC fetch error ({cc_index}, {domain_pattern}): {e}")
            break

    return urls


def _discover(ats_type: str) -> list[str]:
    """Query CommonCrawl across multiple indexes; return deduplicated slugs."""
    pattern  = CC_DOMAINS[ats_type]
    slug_re  = SLUG_RE[ats_type]
    seen_slugs: set[str] = set()
    all_urls: list[str] = []

    for idx in CC_INDEXES:
        log.info(f"ATS discovery ({ats_type}): querying {idx}...")
        urls = _fetch_cc_index(idx, pattern)
        log.info(f"ATS discovery ({ats_type}): {idx} → {len(urls):,} URLs")
        all_urls.extend(urls)

        # Extract slugs so far
        for url in urls:
            m = slug_re.search(url)
            if not m:
                continue
            slug = m.group(1).lower().strip()
            if slug and slug not in _SKIP and len(slug) > 1:
                seen_slugs.add(slug)

        if len(seen_slugs) >= MIN_SLUGS:
            log.info(
                f"ATS discovery ({ats_type}): {len(seen_slugs):,} slugs after {idx} — stopping early"
            )
            break

    return sorted(seen_slugs)


# ── Public API ────────────────────────────────────────────────────────────────

def get_slugs(ats_type: str, fallback: list[str] | None = None) -> list[str]:
    """
    Return all discovered slugs for ats_type (greenhouse | lever | ashby).

    Flow:
      1. SQLite cache (7-day TTL) → instant return
      2. CommonCrawl index API → extract slugs → cache → return
      3. If CommonCrawl fails → return fallback list (hardcoded slugs)
    """
    _init()

    if _cache_fresh(ats_type):
        slugs = _read(ats_type)
        log.info(f"ATS discovery ({ats_type}): {len(slugs):,} tenants from cache")
        return slugs

    slugs = _discover(ats_type)

    if len(slugs) > 100:
        _write(ats_type, slugs)
        log.info(f"ATS discovery ({ats_type}): {len(slugs):,} tenants discovered + cached")
        return slugs

    fb = fallback or []
    log.warning(
        f"ATS discovery ({ats_type}): CommonCrawl returned {len(slugs)} slugs "
        f"— using hardcoded fallback ({len(fb)} slugs)"
    )
    return fb
