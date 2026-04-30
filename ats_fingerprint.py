"""
ATS fingerprinting — maps a company domain to its ATS type + slug.

Strategy (in order):
  1. SQLite cache — free if already known, valid for 90 days
  2. Direct ATS API probe — Greenhouse/Lever/Ashby have public JSON endpoints;
     try a few slug variants derived from the domain
  3. Exa search — finds the actual ATS URL regardless of slug naming
  4. None — company has no detectable ATS (custom page or no postings)

Results cached in SQLite ats_cache table.
"""
import re
import os
import sqlite3
import requests
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv
from utils import log

load_dotenv()

DB_PATH    = Path(__file__).parent / "lunch_perks.db"
CACHE_DAYS = 90
TIMEOUT    = 8
HEADERS    = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# Regex patterns to extract ATS type + slug from any URL or page content
ATS_URL_PATTERNS = [
    ("greenhouse", r'(?:boards|job-boards)\.greenhouse\.io/([a-zA-Z0-9_-]+)'),
    ("lever",      r'jobs\.lever\.co/([a-zA-Z0-9_-]+)'),
    ("ashby",      r'jobs\.ashby\.com/([a-zA-Z0-9_-]+)'),
    ("workday",    r'([a-zA-Z0-9]+)\.wd\d+\.myworkdayjobs\.com'),
    ("icims",      r'careers\.icims\.com'),
    ("smartrecruiters", r'careers\.smartrecruiters\.com/([a-zA-Z0-9_-]+)'),
]


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _init_cache():
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ats_cache (
                domain      TEXT PRIMARY KEY,
                ats_type    TEXT,
                ats_slug    TEXT,
                checked_at  TEXT NOT NULL
            )
        """)


# ── Slug derivation ───────────────────────────────────────────────────────────

def _slug_variants(domain: str) -> list[str]:
    """
    Generate likely ATS slug variants from a domain.
    e.g. 'klaviyo.com' → ['klaviyo']
         'marykay.com' → ['marykay', 'mary-kay', 'marykaycareers']
    """
    base = re.sub(r"\.(com|org|edu|net|io|co).*$", "", domain.lower())
    base = re.sub(r"^www\.", "", base)
    variants = [base]
    # hyphenated variant for multi-word domains
    if re.search(r"[a-z][A-Z]", domain):
        variants.append(re.sub(r"([a-z])([A-Z])", r"\1-\2", base).lower())
    return list(dict.fromkeys(variants))  # dedupe, preserve order


# ── Detection methods ─────────────────────────────────────────────────────────

def _extract_ats_from_text(text: str) -> tuple[str, str] | None:
    """Scan text/URL for ATS patterns, return (ats_type, slug) or None."""
    for ats_type, pattern in ATS_URL_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            slug = m.group(1) if m.lastindex else ""
            return ats_type, slug
    return None


def _probe_direct(domain: str) -> tuple[str, str] | None:
    """
    Try Greenhouse/Lever/Ashby JSON endpoints with slug variants.
    These are public APIs — no auth needed — so a 200 + job count > 0 is definitive.
    """
    slugs = _slug_variants(domain)

    for slug in slugs:
        # Greenhouse public JSON API
        for gh_url in [
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
            f"https://api.greenhouse.io/v1/boards/{slug}/jobs",
        ]:
            try:
                r = requests.get(gh_url, timeout=TIMEOUT, headers=HEADERS)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("jobs") is not None:  # key exists even if empty list
                        return "greenhouse", slug
            except Exception:
                pass

        # Lever public JSON API
        lever_url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            r = requests.get(lever_url, timeout=TIMEOUT, headers=HEADERS)
            if r.status_code == 200 and isinstance(r.json(), list):
                return "lever", slug
        except Exception:
            pass

        # Ashby public JSON API
        ashby_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        try:
            r = requests.get(ashby_url, timeout=TIMEOUT, headers=HEADERS)
            if r.status_code == 200 and r.json().get("jobPostings") is not None:
                return "ashby", slug
        except Exception:
            pass

    return None


def _probe_exa(domain: str, company_name: str = "") -> tuple[str, str] | None:
    """
    Use Exa to find the company's ATS URL.
    Falls back gracefully if EXA_API_KEY is not set.
    """
    api_key = os.getenv("EXA_API_KEY", "")
    if not api_key:
        return None

    query = f'"{company_name or domain}" jobs careers site:boards.greenhouse.io OR site:jobs.lever.co OR site:jobs.ashby.com'
    try:
        import exa_py
        exa = exa_py.Exa(api_key=api_key)
        results = exa.search(query, num_results=3, type="keyword")
        for result in results.results:
            match = _extract_ats_from_text(result.url)
            if match:
                return match
    except Exception as e:
        log.debug(f"Exa fingerprint failed for {domain}: {e}")

    return None


# ── Public API ────────────────────────────────────────────────────────────────

def get_ats(domain: str, company_name: str = "", force_refresh: bool = False) -> tuple[str, str]:
    """
    Return (ats_type, ats_slug) for a domain.
    ats_type: 'greenhouse' | 'lever' | 'ashby' | 'workday' | 'exa' | 'none'
    """
    _init_cache()
    stale_cutoff = (date.today() - timedelta(days=CACHE_DAYS)).isoformat()

    if not force_refresh:
        with _conn() as con:
            row = con.execute(
                "SELECT ats_type, ats_slug, checked_at FROM ats_cache WHERE domain = ?",
                (domain,)
            ).fetchone()
            if row and row["checked_at"] >= stale_cutoff:
                return row["ats_type"], row["ats_slug"] or ""

    log.debug(f"Fingerprinting ATS for {domain}")

    # 1. Direct JSON API probe (free, fast)
    result = _probe_direct(domain)

    # 2. Exa search fallback
    if not result:
        result = _probe_exa(domain, company_name)

    # 3. Default to 'exa' so targeted_scraper uses Exa search as fallback
    ats_type, ats_slug = result if result else ("exa", "")

    with _conn() as con:
        con.execute("""
            INSERT INTO ats_cache (domain, ats_type, ats_slug, checked_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
                ats_type   = excluded.ats_type,
                ats_slug   = excluded.ats_slug,
                checked_at = excluded.checked_at
        """, (domain, ats_type, ats_slug, date.today().isoformat()))

    return ats_type, ats_slug


def ats_distribution() -> dict:
    """Return ATS type counts from cache — for observability reporting."""
    _init_cache()
    with _conn() as con:
        rows = con.execute(
            "SELECT ats_type, COUNT(*) as n FROM ats_cache GROUP BY ats_type ORDER BY n DESC"
        ).fetchall()
    return {r["ats_type"]: r["n"] for r in rows}
