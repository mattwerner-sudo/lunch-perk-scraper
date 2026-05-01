"""
Sumble Job Search scraper.

Two modes:
  discovery       — free-text query against Sumble's food keyword index.
                    Finds companies with food perks across all of Sumble's corpus.
                    Matches results against our account list for segmentation.
  account_monitor — per-domain calls against managed accounts only (876 domains).
                    Confirms active food-perk hiring at known accounts.
                    (Avoid running against all 18k domains — 1 API call per domain.)

API: POST https://api.sumble.com/v6/jobs/find
Cost: 2 credits/job without descriptions, 3 credits/job with descriptions.
Docs: https://docs.sumble.com/api/jobs.md

Requires: SUMBLE_API_KEY in .env
"""
import logging
import os
import time
from typing import Iterator

import requests
from dotenv import load_dotenv

from utils import find_food_keywords, excerpt, clean_text

load_dotenv()
log = logging.getLogger(__name__)

API_KEY  = os.getenv("SUMBLE_API_KEY", "")
BASE_URL = "https://api.sumble.com/v6/jobs/find"

# ── Tuning constants ──────────────────────────────────────────────────────────
RESULTS_PER_PAGE        = 100   # max allowed by Sumble API
MAX_DISCOVERY_PAGES     = 10    # 10 × 100 × 3 credits = 3,000 credits max for discovery
MAX_ACCOUNT_JOBS        = 10    # jobs per domain in account_monitor mode (cost control)
REQUEST_DELAY           = 0.12  # seconds between requests — stays under 10 req/sec limit
MAX_AGE_DAYS            = 7

# Free-text query for discovery mode — Sumble's query field actually filters on description
FOOD_QUERY = (
    "free lunch OR catered lunch OR catered meals OR meal stipend OR food stipend "
    "OR lunch stipend OR meal credit OR food credit OR doordash OR grubhub OR ubereats "
    "OR forkable OR sharebite OR free food OR free meals"
)

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}


# ── Core request ──────────────────────────────────────────────────────────────

def _post(payload: dict) -> dict | None:
    """Single Sumble API call. Returns parsed JSON or None on failure."""
    try:
        resp = requests.post(BASE_URL, json=payload, headers=HEADERS, timeout=30)
        if resp.status_code == 402:
            log.error("Sumble: out of credits — stopping")
            return None
        if resp.status_code == 422:
            log.error(f"Sumble: invalid request — {resp.text[:200]}")
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        log.warning("Sumble: request timed out")
        return None
    except Exception as e:
        log.warning(f"Sumble: request failed — {e}")
        return None


# ── Record builder ────────────────────────────────────────────────────────────

def _build_record(job: dict, source_label: str = "Sumble") -> dict | None:
    """
    Map a Sumble job object to the standard scraper record format.
    Always runs local keyword check — Sumble's query filter improves precision
    but we still validate every record.
    """
    title       = (job.get("job_title") or "").strip()
    url         = (job.get("url") or "").strip()
    date_posted = (job.get("datetime_pulled") or "")[:10]
    domain      = (job.get("organization_domain") or "").strip().lower()
    company     = (job.get("organization_name") or domain).strip()
    location    = (job.get("location") or "").strip()
    description = clean_text(job.get("description") or "")

    if not url:
        return None

    search_text = f"{title} {description}"
    matched = find_food_keywords(search_text)
    if not matched:
        return None

    return {
        "source":                source_label,
        "company":               company,
        "title":                 title,
        "location":              location,
        "remote":                "",
        "food_keywords_matched": ", ".join(matched),
        "keyword_count":         len(matched),
        "perk_excerpt":          excerpt(description, matched[0]) if description else "",
        "date_posted":           date_posted,
        "url":                   url,
        "_domain":               domain,
    }


# ── Discovery mode ────────────────────────────────────────────────────────────

def _scrape_discovery(
    max_age_days: int,
    seen_urls: set[str],
    credit_counter: list[int],
    dry_run: bool,
) -> Iterator[dict]:
    """
    Discovery mode: free-text query, no domain scoping.
    Finds food-perk companies across Sumble's full corpus, then matches
    results against our account list for managed/unmanaged/prospect segmentation.
    """
    base_payload = {
        "filters": {"query": FOOD_QUERY, "since": _since_date(max_age_days), "countries": ["US"]},
        "include_descriptions": True,
        "limit": RESULTS_PER_PAGE,
    }

    if dry_run:
        probe = _post({**base_payload, "limit": 5})
        if probe is None:
            log.info("Sumble DRY RUN: API call failed (check SUMBLE_API_KEY)")
            return
        jobs = probe.get("jobs", [])
        food = sum(1 for j in jobs if _build_record(j))
        total = probe.get("total", "?")
        credits_used = probe.get("credits_used", "?")
        log.info(
            f"Sumble DRY RUN: {food}/{len(jobs)} of 5 probed passed food keyword filter. "
            f"Total matching: {total}. Credits used for probe: {credits_used}. "
            f"Full discovery run: up to {MAX_DISCOVERY_PAGES} pages × {RESULTS_PER_PAGE} jobs × 3 credits "
            f"= up to {MAX_DISCOVERY_PAGES * RESULTS_PER_PAGE * 3} credits max."
        )
        if jobs:
            for j in jobs[:3]:
                log.info(f"  sample: [{j.get('organization_domain','')}] {j.get('job_title','')} — {j.get('location','')}")
        return

    for page in range(MAX_DISCOVERY_PAGES):
        data = _post({**base_payload, "offset": page * RESULTS_PER_PAGE})
        time.sleep(REQUEST_DELAY)

        if not data:
            break

        jobs = data.get("jobs", [])
        if not jobs:
            break

        credit_counter[0] += data.get("credits_used", len(jobs) * 3)

        for job in jobs:
            url = (job.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            record = _build_record(job, source_label="Sumble-Discovery")
            if record:
                yield record

        log.info(f"Sumble discovery: page {page + 1}/{MAX_DISCOVERY_PAGES} — "
                 f"{len(jobs)} jobs pulled | total credits: {credit_counter[0]}")

        if len(jobs) < RESULTS_PER_PAGE:
            break


# ── Account monitor mode ──────────────────────────────────────────────────────

def _scrape_account_monitor(
    max_age_days: int,
    domain_limit: int | None,
    seen_urls: set[str],
    credit_counter: list[int],
    dry_run: bool,
) -> Iterator[dict]:
    """
    Account monitor mode: per-domain calls against managed accounts only.
    Confirms which rep-owned accounts have active food-perk hiring.
    Avoids the 18k-domain loop — scoped to managed (876 domains) only.
    """
    from account_filter import _managed_accounts

    domains = list({r["_domain"] for r in _managed_accounts() if r["_domain"]})
    if domain_limit:
        domains = domains[:domain_limit]

    log.info(f"Sumble account_monitor: {len(domains)} managed domains")

    if dry_run:
        sample = domains[:3]
        total_food = 0
        for domain in sample:
            probe = _post({
                "organization": {"domain": domain},
                "filters": {"since": _since_date(max_age_days), "countries": ["US"]},
                "include_descriptions": True,
                "limit": 3,
            })
            if probe is None:
                log.info(f"Sumble DRY RUN [{domain}]: API call failed")
                continue
            jobs = probe.get("jobs", [])
            food = sum(1 for j in jobs if _build_record(j))
            total_food += food
            log.info(f"Sumble DRY RUN [{domain}]: {food}/{len(jobs)} food-perk matches")
            time.sleep(REQUEST_DELAY)
        est_credits = len(domains) * MAX_ACCOUNT_JOBS * 3
        log.info(f"Sumble DRY RUN: full account_monitor run ~{est_credits} credits max "
                 f"({len(domains)} domains × {MAX_ACCOUNT_JOBS} jobs × 3 credits)")
        return

    for i, domain in enumerate(domains, 1):
        data = _post({
            "organization": {"domain": domain},
            "filters": {"since": _since_date(max_age_days), "countries": ["US"]},
            "include_descriptions": True,
            "limit": MAX_ACCOUNT_JOBS,
        })
        time.sleep(REQUEST_DELAY)

        if data is None:
            break

        jobs = data.get("jobs", [])
        credit_counter[0] += data.get("credits_used", len(jobs) * 3)

        for job in jobs:
            url = (job.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            record = _build_record(job, source_label="Sumble")
            if record:
                log.debug(f"  ✓ {record['company']} — {record['title']} [{record['location']}]")
                yield record

        if i % 50 == 0:
            log.info(f"Sumble account_monitor: {i}/{len(domains)} domains | "
                     f"credits used: {credit_counter[0]}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _since_date(max_age_days: int) -> str:
    from datetime import date, timedelta
    return (date.today() - timedelta(days=max_age_days)).strftime("%Y-%m-%d")


# ── Public entry point ────────────────────────────────────────────────────────

def scrape(
    mode: str = "discovery",
    max_age_days: int = MAX_AGE_DAYS,
    domain_limit: int | None = None,
    dry_run: bool = False,
) -> Iterator[dict]:
    """
    Main scrape function, called by scrape.py.

    mode:
      "discovery"       — free-text query, finds food-perk companies across all of Sumble.
                          Best for net-new prospects and broad account signal.
      "account_monitor" — per-domain calls against managed accounts only (876 domains).
                          Confirms active hiring at rep-owned accounts.

    domain_limit: cap for testing in account_monitor mode.
    dry_run: probe only — logs expected credit spend, yields nothing.
    """
    if not API_KEY:
        log.warning("Sumble: SUMBLE_API_KEY not set — skipping")
        return

    seen_urls: set[str] = set()
    credit_counter = [0]

    if mode == "discovery":
        yield from _scrape_discovery(max_age_days, seen_urls, credit_counter, dry_run)
        if not dry_run:
            log.info(f"Sumble discovery complete: ~{credit_counter[0]} credits consumed")
    elif mode == "account_monitor":
        yield from _scrape_account_monitor(max_age_days, domain_limit, seen_urls, credit_counter, dry_run)
        if not dry_run:
            log.info(f"Sumble account_monitor complete: ~{credit_counter[0]} credits consumed")
    else:
        log.error(f"Sumble: unknown mode '{mode}' — use 'discovery' or 'account_monitor'")
