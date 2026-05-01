"""
TheirStack Job Search scraper.

Two modes:
  account_monitor  — query TheirStack with our managed/unmanaged domain list.
                     Returns which of our known accounts posted food-perk JDs
                     this week. Collapses the 18-week rotation cycle.
  discovery        — keyword-only search, no domain filter.
                     Finds net-new companies outside our account universe.

API: POST https://api.theirstack.com/v1/jobs/search
Cost: 1 credit per job returned. Filter aggressively on the API side.
Docs: https://theirstack.com/en/docs/api-reference/jobs/search_jobs_v1

Requires: THEIRSTACK_API_KEY in .env
"""
import logging
import os
import time
import math
from datetime import date, timedelta
from typing import Iterator

import requests
from dotenv import load_dotenv

from utils import find_food_keywords, excerpt, clean_text

load_dotenv()
log = logging.getLogger(__name__)

API_KEY  = os.getenv("THEIRSTACK_API_KEY", "")
BASE_URL = "https://api.theirstack.com/v1/jobs/search"

# ── Tuning constants ──────────────────────────────────────────────────────────
DOMAIN_BATCH_SIZE   = 100    # domains per request (safe payload limit)
RESULTS_PER_PAGE    = 25     # jobs per page (25 = 25 credits/request)
MAX_PAGES_PER_BATCH = 4      # max pages to fetch per domain batch (100 credits cap)
REQUEST_DELAY       = 0.26   # seconds between requests — stays under 4 req/sec limit
MAX_AGE_DAYS        = 7      # only jobs posted in the last N days

# ── Food keyword patterns sent to TheirStack API ──────────────────────────────
# These are the high-value keywords only (score >= 5 in our rubric).
# Keeping this list tight minimises false-positive credits consumed.
# "stocked kitchen" intentionally excluded — too noisy at $0.001/result.
FOOD_PATTERNS = [
    "free lunch",
    "catered lunch",
    "catered meals",
    "catered breakfast",
    "daily lunch",
    "free meals",
    "free food",
    "doordash",
    "grubhub",
    "ubereats",
    "uber eats",
    "forkable",
    "sharebite",
    "seamless corporate",
    "meal stipend",
    "food stipend",
    "lunch stipend",
    "meal credit",
    "food credit",
    "lunch credit",
    "lunch benefit",
]

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}


# ── Core request ──────────────────────────────────────────────────────────────

def _post(payload: dict) -> dict | None:
    """Single TheirStack API call. Returns parsed JSON or None on failure."""
    try:
        resp = requests.post(BASE_URL, json=payload, headers=HEADERS, timeout=30)
        if resp.status_code == 402:
            log.error("TheirStack: out of credits — stopping")
            return None
        if resp.status_code == 422:
            log.error(f"TheirStack: invalid request — {resp.text[:200]}")
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        log.warning("TheirStack: request timed out")
        return None
    except Exception as e:
        log.warning(f"TheirStack: request failed — {e}")
        return None


def _count(payload: dict) -> int:
    """
    Free count mode — returns total matching jobs without consuming credits.
    Append ?free_count=true to preview how many results a query would return.
    """
    try:
        resp = requests.post(
            f"{BASE_URL}?free_count=true",
            json=payload,
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("total", 0)
    except Exception as e:
        log.warning(f"TheirStack: count failed — {e}")
        return 0


# ── Record builder ────────────────────────────────────────────────────────────

def _build_record(job: dict) -> dict | None:
    """
    Map a TheirStack job object to the standard scraper record format.
    Returns None if the job doesn't pass local keyword validation.
    """
    description = clean_text(job.get("description") or job.get("short_description") or "")
    title       = (job.get("job_title") or job.get("title") or "").strip()
    company     = (job.get("company_name") or job.get("company", {}).get("name") or "").strip()
    location    = (job.get("location") or job.get("city") or "").strip()
    country     = (job.get("country_code") or "").strip()
    url         = (job.get("url") or job.get("job_url") or "").strip()
    date_posted = (job.get("date_posted") or job.get("discovered_at") or "")[:10]
    domain      = (job.get("company_domain") or
                   job.get("company", {}).get("domain") or "").strip().lower()

    if not company or not url:
        return None

    # Full location string
    loc_parts = [p for p in [location, country] if p]
    loc_str   = ", ".join(loc_parts)

    # Local keyword validation — TheirStack pattern matching is substring-based
    # but our find_food_keywords() is more precise (word boundaries, de-dup).
    # Only emit records that pass our own classifier.
    search_text = f"{title} {description}"
    matched = find_food_keywords(search_text)
    if not matched:
        # Fallback: check against our pattern list directly (catches multi-word)
        low = search_text.lower()
        matched = [p for p in FOOD_PATTERNS if p in low]
    if not matched:
        return None

    return {
        "source":                "TheirStack",
        "company":               company,
        "title":                 title,
        "location":              loc_str,
        "remote":                "Remote" if job.get("remote") else "",
        "food_keywords_matched": ", ".join(matched),
        "keyword_count":         len(matched),
        "perk_excerpt":          excerpt(description, matched[0]) if description else "",
        "date_posted":           date_posted,
        "url":                   url,
        "_domain":               domain,   # internal — used for dedup, not in CSV
    }


# ── Batch fetcher ─────────────────────────────────────────────────────────────

def _fetch_batch(
    domain_batch: list[str],
    posted_at_max_age_days: int,
    seen_urls: set[str],
    credit_counter: list[int],
) -> list[dict]:
    """
    Fetch all food-perk jobs for a batch of domains.
    Pages through results up to MAX_PAGES_PER_BATCH.
    Mutates seen_urls (dedup) and credit_counter (tracking).
    """
    base_payload = {
        "company_domain_or":          domain_batch,
        "job_description_pattern_or": FOOD_PATTERNS,
        "posted_at_max_age_days":     posted_at_max_age_days,
        "country_code_or":            ["US"],
        "limit":                      RESULTS_PER_PAGE,
        "order_by":                   [{"field": "date_posted", "desc": True}],
    }

    records = []
    for page in range(MAX_PAGES_PER_BATCH):
        payload = {**base_payload, "offset": page * RESULTS_PER_PAGE}
        data = _post(payload)
        time.sleep(REQUEST_DELAY)

        if not data:
            break

        jobs = data.get("data", []) or data.get("jobs", [])
        if not jobs:
            break

        credits_used = len(jobs)
        credit_counter[0] += credits_used

        for job in jobs:
            url = (job.get("url") or job.get("job_url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            record = _build_record(job)
            if record:
                records.append(record)

        # If we got fewer results than the page size, we've exhausted this batch
        if len(jobs) < RESULTS_PER_PAGE:
            break

    return records


# ── Public entry points ───────────────────────────────────────────────────────

def scrape(
    mode: str = "account_monitor",
    max_age_days: int = MAX_AGE_DAYS,
    domain_limit: int | None = None,
    dry_run: bool = False,
) -> Iterator[dict]:
    """
    Main scrape function, called by scrape.py.

    mode:
      "account_monitor" — query against managed + ICP unmanaged domain list.
                          Use this for weekly account monitoring runs.
      "discovery"       — keyword-only, no domain filter.
                          Use this for finding net-new companies.

    domain_limit: cap for testing (e.g. domain_limit=500 for a smoke test).
    dry_run: use free_count mode — logs expected credit spend, yields nothing.
    """
    if not API_KEY:
        log.warning("TheirStack: THEIRSTACK_API_KEY not set — skipping")
        return

    seen_urls: set[str]  = set()
    credit_counter       = [0]   # mutable so _fetch_batch can mutate it

    if mode == "discovery":
        yield from _scrape_discovery(max_age_days, seen_urls, credit_counter, dry_run)
        log.info(f"TheirStack discovery: ~{credit_counter[0]} credits consumed")
        return

    # ── Account monitor mode ──────────────────────────────────────────────────
    # Load domain universe: managed + ICP unmanaged, deduplicated
    from account_filter import _managed_accounts, _unmanaged_accounts, _is_suppressed, _is_icp

    managed_domains   = {r["_domain"] for r in _managed_accounts() if r["_domain"]}
    unmanaged_domains = {
        r["_domain"] for r in _unmanaged_accounts()
        if r["_domain"] and not _is_suppressed(r) and _is_icp(r)
    }
    all_domains = list(managed_domains | unmanaged_domains)

    if domain_limit:
        all_domains = all_domains[:domain_limit]

    total_batches = math.ceil(len(all_domains) / DOMAIN_BATCH_SIZE)
    log.info(
        f"TheirStack: {len(all_domains)} domains → "
        f"{total_batches} batches of {DOMAIN_BATCH_SIZE}"
    )

    if dry_run:
        # Preview: count matching jobs without burning credits
        sample_batch = all_domains[:DOMAIN_BATCH_SIZE]
        count = _count({
            "company_domain_or":          sample_batch,
            "job_description_pattern_or": FOOD_PATTERNS,
            "posted_at_max_age_days":     max_age_days,
            "country_code_or":            ["US"],
            "limit":                      RESULTS_PER_PAGE,
        })
        projected = round(count * (len(all_domains) / len(sample_batch)))
        log.info(
            f"TheirStack DRY RUN — sample batch ({len(sample_batch)} domains): "
            f"{count} jobs found. Projected full run: ~{projected} jobs / ~{projected} credits."
        )
        return

    for batch_num, start in enumerate(range(0, len(all_domains), DOMAIN_BATCH_SIZE), 1):
        batch = all_domains[start : start + DOMAIN_BATCH_SIZE]
        log.info(f"TheirStack: batch {batch_num}/{total_batches} ({len(batch)} domains)")

        records = _fetch_batch(batch, max_age_days, seen_urls, credit_counter)

        for record in records:
            log.debug(f"  ✓ {record['company']} — {record['title']} [{record['location']}]")
            yield record

        if records:
            log.info(f"  → {len(records)} food-perk matches | "
                     f"total credits used: {credit_counter[0]}")


def _scrape_discovery(
    max_age_days: int,
    seen_urls: set[str],
    credit_counter: list[int],
    dry_run: bool,
) -> Iterator[dict]:
    """
    Discovery mode: no domain filter, keyword-only.
    Finds companies with food perks outside our known account universe.
    Caps at 5 pages (125 results / 125 credits) by default.
    """
    payload = {
        "job_description_pattern_or": FOOD_PATTERNS,
        "posted_at_max_age_days":     max_age_days,
        "country_code_or":            ["US"],
        "limit":                      RESULTS_PER_PAGE,
        "order_by":                   [{"field": "date_posted", "desc": True}],
    }

    if dry_run:
        count = _count(payload)
        log.info(f"TheirStack discovery DRY RUN: ~{count} jobs would be returned (~{count} credits)")
        return

    MAX_DISCOVERY_PAGES = 5
    for page in range(MAX_DISCOVERY_PAGES):
        data = _post({**payload, "offset": page * RESULTS_PER_PAGE})
        time.sleep(REQUEST_DELAY)

        if not data:
            break

        jobs = data.get("data", []) or data.get("jobs", [])
        if not jobs:
            break

        credit_counter[0] += len(jobs)

        for job in jobs:
            url = (job.get("url") or job.get("job_url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            record = _build_record(job)
            if record:
                record["source"] = "TheirStack-Discovery"
                yield record

        if len(jobs) < RESULTS_PER_PAGE:
            break
