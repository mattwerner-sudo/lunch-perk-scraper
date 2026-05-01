"""
Domain-first ATS scraper — the most accurate source in the pipeline.

Starts from your managed + ICP unmanaged account domain lists.
Detects each domain's ATS via direct JSON API probe (SQLite-cached 90 days).
Queries Greenhouse / Lever / Ashby JSON APIs for job listings.
Filters for food perk keywords in job description text.

Key properties vs. the keyword-search scrapers:
  - Every result is a company from your account lists — no random prospects
  - _domain is verified (from CSV), so account matching is exact — no guessing
  - Company name = canonical CSV name → zero fuzzy matching needed
  - Same fast JSON APIs as greenhouse.py / lever.py / ashby.py (free, no credits)
  - Self-correcting: as ATS cache fills, each weekly run gets faster

Usage (via scrape.py):
  python3 scrape.py --sources da --no-notify
  python3 scrape.py --sources da gh lv ab --no-notify  # combine with other sources
"""
import logging
import time
import requests
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from utils import find_food_keywords, excerpt, clean_text
from ats_fingerprint import get_ats
from account_filter import get_all_tiers

log = logging.getLogger(__name__)

TIMEOUT    = 12
MAX_WORKERS = 8   # parallel ATS probes

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


# ── ATS fetchers (JSON API, no HTML scraping) ─────────────────────────────────

def _fetch_greenhouse(slug: str) -> list[dict]:
    try:
        r = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            headers=HEADERS, timeout=TIMEOUT,
        )
        if r.status_code == 200:
            return r.json().get("jobs", [])
    except Exception:
        pass
    return []


def _fetch_lever(slug: str) -> list[dict]:
    try:
        r = requests.get(
            f"https://api.lever.co/v0/postings/{slug}?mode=json",
            headers=HEADERS, timeout=TIMEOUT,
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def _fetch_ashby(slug: str) -> list[dict]:
    try:
        r = requests.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
            headers=HEADERS, timeout=TIMEOUT,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("jobs", data.get("jobPostings", []))
    except Exception:
        pass
    return []


# ── Per-ATS record parsers ────────────────────────────────────────────────────

def _parse_greenhouse(jobs: list[dict], company: str, domain: str) -> list[dict]:
    out = []
    for job in jobs:
        content = clean_text(job.get("content", "") or "")
        matched  = find_food_keywords(content)
        if not matched:
            continue
        out.append({
            "source":                "Domain-Greenhouse",
            "company":               company,
            "_domain":               domain,
            "title":                 job.get("title", ""),
            "location":              (job.get("location") or {}).get("name", ""),
            "remote":                "",
            "food_keywords_matched": ", ".join(matched),
            "keyword_count":         len(matched),
            "perk_excerpt":          excerpt(content, matched[0]),
            "date_posted":           (job.get("updated_at", "") or "")[:10],
            "url":                   job.get("absolute_url", ""),
        })
    return out


def _parse_lever(jobs: list[dict], company: str, domain: str) -> list[dict]:
    out = []
    for job in jobs:
        plain       = job.get("descriptionPlain", "") or ""
        list_text   = " ".join(lst.get("content", "") for lst in (job.get("lists") or []))
        additional  = job.get("additional", "") or ""
        full_text   = clean_text(f"{plain} {list_text} {additional}")
        matched     = find_food_keywords(full_text)
        if not matched:
            continue
        out.append({
            "source":                "Domain-Lever",
            "company":               company,
            "_domain":               domain,
            "title":                 job.get("text", ""),
            "location":              (job.get("categories") or {}).get("location", ""),
            "remote":                "",
            "food_keywords_matched": ", ".join(matched),
            "keyword_count":         len(matched),
            "perk_excerpt":          excerpt(full_text, matched[0]),
            "date_posted":           _ts_to_date(job.get("createdAt")),
            "url":                   job.get("hostedUrl", ""),
        })
    return out


def _parse_ashby(jobs: list[dict], company: str, domain: str) -> list[dict]:
    out = []
    for job in jobs:
        desc_html    = job.get("descriptionHtml", "") or job.get("descriptionPlain", "") or ""
        section_text = " ".join(
            s.get("content", "") for s in (job.get("descriptionSections") or [])
        )
        full_text = clean_text(f"{desc_html} {section_text}")
        matched   = find_food_keywords(full_text)
        if not matched:
            continue
        out.append({
            "source":                "Domain-Ashby",
            "company":               company,
            "_domain":               domain,
            "title":                 job.get("title", ""),
            "location":              job.get("location", ""),
            "remote":                "Remote" if job.get("isRemote") else "On-site",
            "food_keywords_matched": ", ".join(matched),
            "keyword_count":         len(matched),
            "perk_excerpt":          excerpt(full_text, matched[0]),
            "date_posted":           (job.get("publishedAt", "") or "")[:10],
            "url":                   job.get("jobUrl", ""),
        })
    return out


def _ts_to_date(ts) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


_FETCHERS = {
    "greenhouse": _fetch_greenhouse,
    "lever":      _fetch_lever,
    "ashby":      _fetch_ashby,
}
_PARSERS = {
    "greenhouse": _parse_greenhouse,
    "lever":      _parse_lever,
    "ashby":      _parse_ashby,
}


# ── Per-account worker ────────────────────────────────────────────────────────

def _scrape_account(account: dict) -> list[dict]:
    """
    Probe one account domain, fetch its ATS job list, filter for food keywords.
    Returns list of records (empty if no food perks found or ATS not supported).
    """
    domain  = account.get("_domain", "").strip()
    company = (account.get("Account Name") or domain).strip()
    if not domain:
        return []

    # get_ats: checks SQLite cache (90-day TTL) → probes GH/Lever/Ashby → caches result
    ats_type, slug = get_ats(domain, company_name=company)

    fetcher = _FETCHERS.get(ats_type)
    parser  = _PARSERS.get(ats_type)
    if not fetcher or not parser or not slug:
        return []   # workday / icims / none / exa-only — not supported here

    jobs    = fetcher(slug)
    records = parser(jobs, company, domain)

    if records:
        log.info(f"  ✓ {company} ({domain}) [{ats_type}]: {len(records)} food-perk matches")

    return records


# ── Public entry point ────────────────────────────────────────────────────────

def scrape(tier2_sample: int = 1000) -> Iterator[dict]:
    """
    Load managed + ICP unmanaged accounts, detect their ATS, yield food-perk records.

    tier2_sample: unmanaged ICP accounts per run (default 1000).
    Full 19k unmanaged list rotates over ~19 weekly runs.

    First run is slower (probing uncached domains); subsequent runs are fast
    (ATS type + slug are cached 90 days in SQLite).
    """
    accounts = get_all_tiers(tier2_sample=tier2_sample)
    log.info(f"Domain ATS: probing {len(accounts)} accounts")

    seen_urls: set[str] = set()
    hits = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_scrape_account, acc): acc for acc in accounts}
        for future in as_completed(futures):
            try:
                records = future.result()
            except Exception as e:
                log.warning(f"Domain ATS worker failed: {e}")
                continue

            for rec in records:
                url = rec.get("url", "")
                if url and url in seen_urls:
                    continue
                if url:
                    seen_urls.add(url)
                hits += 1
                yield rec

    log.info(f"Domain ATS: {hits} total food-perk records from account list")
