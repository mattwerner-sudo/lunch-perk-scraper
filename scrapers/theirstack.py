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

_out_of_credits = False  # set on first 402 — stops all subsequent batches

# ── Tuning constants ──────────────────────────────────────────────────────────
DOMAIN_BATCH_SIZE   = 100    # domains per request (safe payload limit)
RESULTS_PER_PAGE    = 25     # jobs per page (25 = 25 credits/request)
MAX_PAGES_PER_BATCH = 1      # 1 page per batch = 25 credits max per 100 domains
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
    global _out_of_credits
    if _out_of_credits:
        return None
    try:
        resp = requests.post(BASE_URL, json=payload, headers=HEADERS, timeout=30)
        if resp.status_code == 402:
            log.error("TheirStack: out of credits — stopping all batches")
            _out_of_credits = True
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


def debug():
    """
    Progressive diagnostic — run with: python3 -c "from scrapers.theirstack import debug; debug()"
    Prints raw response structure so we can identify exact field names.
    Tries multiple description filter parameter names to find the working one.
    """
    import json

    def _safe_post(payload, label=""):
        try:
            resp = requests.post(BASE_URL, json=payload, headers=HEADERS, timeout=30)
            try:
                body = resp.json()
            except Exception:
                body = {"_raw": resp.text[:300]}
            return resp.status_code, body
        except Exception as e:
            print(f"  REQUEST FAILED: {e}")
            return 0, {}

    known_domains = ["google.com", "microsoft.com", "salesforce.com", "stripe.com", "airbnb.com"]

    # ── Step 1: Show raw response structure ───────────────────────────────────
    print("\n── Step 1: Raw response structure (known domains, no filters) ──")
    status, body = _safe_post({
        "company_domain_or":      known_domains,
        "posted_at_max_age_days": 30,
        "job_country_code_or":    ["US"],
        "limit": 2,
    })
    print(f"  status={status}")
    print(f"  top-level keys: {list(body.keys())}")
    jobs = body.get("data") or body.get("jobs") or []
    if jobs:
        print(f"  job[0] keys: {list(jobs[0].keys())}")
        print(f"  job[0] title: {jobs[0].get('job_title') or jobs[0].get('title')}")
        print(f"  job[0] company: {jobs[0].get('company_name') or jobs[0].get('company_object', {}).get('name')}")
    # Look for total in various places
    for key in ["total", "total_results", "count", "num_results"]:
        if key in body:
            print(f"  total field '{key}': {body[key]}")
    if "metadata" in body:
        print(f"  metadata: {body['metadata']}")
    time.sleep(0.3)

    # ── Step 2: free_count response structure ─────────────────────────────────
    print("\n── Step 2: free_count response structure ──")
    try:
        resp = requests.post(
            f"{BASE_URL}?free_count=true",
            json={"company_domain_or": known_domains, "posted_at_max_age_days": 30,
                  "job_country_code_or": ["US"], "limit": 2},
            headers=HEADERS, timeout=30,
        )
        body_fc = resp.json()
        print(f"  status={resp.status_code}")
        print(f"  free_count response keys: {list(body_fc.keys())}")
        print(f"  full response: {json.dumps(body_fc)[:300]}")
    except Exception as e:
        print(f"  FAILED: {e}")
    time.sleep(0.3)

    # ── Step 3: Try each description filter param name ────────────────────────
    print("\n── Step 3: Description filter param names ──")
    food_terms = ["free lunch", "catered meals", "doordash"]
    for param in [
        "job_description_pattern_or",
        "job_description_contains_or",
        "description_pattern_or",
        "description_contains_or",
        "job_description_or",
        "query",
    ]:
        status, body = _safe_post({
            param:                    food_terms,
            "posted_at_max_age_days": 30,
            "job_country_code_or":    ["US"],
            "limit": 1,
        })
        jobs = body.get("data") or body.get("jobs") or []
        title = (jobs[0].get("job_title") or jobs[0].get("title") or "—") if jobs else "—"
        err   = body.get("detail") or body.get("error") or ""
        # A food-relevant title means the filter worked
        food_hit = any(w in title.lower() for w in ["food", "lunch", "catering", "facility", "facilities", "office", "people", "hr", "chef"])
        marker = "✓ FOOD HIT" if food_hit else ("✗ wrong results" if jobs else "✗ no results")
        print(f"  {param:40s} status={status} title='{title[:50]}' {marker}")
        if status == 422:
            print(f"    422: {err[:150]}")
        time.sleep(0.3)


def _total(body: dict) -> int | None:
    """Extract total_results from TheirStack response metadata. Returns None if not set."""
    return (body.get("metadata") or {}).get("total_results")


# ── Record builder ────────────────────────────────────────────────────────────

def _build_record(job: dict) -> dict | None:
    """
    Map a TheirStack job object to the standard scraper record format.
    Field names confirmed from real API response (debug step 1).
    Returns None if local keyword check fails — description filter is done
    here because TheirStack's API-side description filter is plan-restricted.
    """
    # Confirmed field names from debug output
    title       = (job.get("job_title") or "").strip()
    url         = (job.get("url") or "").strip()
    date_posted = (job.get("date_posted") or job.get("discovered_at") or "")[:10]
    domain      = (job.get("company_domain") or "").strip().lower()
    description = clean_text(job.get("description") or "")

    # Company is nested under company_object
    co_obj  = job.get("company_object") or {}
    company = (co_obj.get("name") or domain).strip()

    # Location: short_location is cleaner than location for display
    location = (job.get("short_location") or job.get("long_location") or "").strip()

    if not url:
        return None

    # ── Local description filter (API-side filter not available on this plan) ──
    # Run our keyword matcher on the full description text returned by TheirStack.
    search_text = f"{title} {description}"
    matched = find_food_keywords(search_text)
    if not matched:
        low = search_text.lower()
        matched = [p for p in FOOD_PATTERNS if p in low]
    if not matched:
        return None

    return {
        "source":                "TheirStack",
        "company":               company,
        "title":                 title,
        "location":              location,
        "remote":                "Remote" if job.get("remote") else "",
        "food_keywords_matched": ", ".join(matched),
        "keyword_count":         len(matched),
        "perk_excerpt":          excerpt(description, matched[0]) if description else "",
        "date_posted":           date_posted,
        "url":                   url,
        "_domain":               domain,
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
    # No title filter — food perks appear in any role (engineer, analyst, PM)
    # not just HR/facilities. Credit spend is controlled by MAX_PAGES_PER_BATCH=1:
    # 25 jobs × 189 batches = ~4,700 credits max for a full run (~$4.70).
    # find_food_keywords() runs locally on the returned description field.
    base_payload = {
        "company_domain_or":      domain_batch,
        "posted_at_max_age_days": posted_at_max_age_days,
        "job_country_code_or":    ["US"],
        "limit":                  RESULTS_PER_PAGE,
        "order_by":               [{"field": "date_posted", "desc": True}],
    }

    records = []
    for page in range(MAX_PAGES_PER_BATCH):
        payload = {**base_payload, "offset": page * RESULTS_PER_PAGE}
        data = _post(payload)
        time.sleep(REQUEST_DELAY)

        if not data:
            break

        jobs = data.get("data", [])
        if not jobs:
            break

        credit_counter[0] += len(jobs)

        for job in jobs:
            url = (job.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            record = _build_record(job)
            if record:
                records.append(record)

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
        # Probe first batch with limit=1 (1 credit) to confirm filters work,
        # then project full run volume. free_count returns null on this plan.
        sample_batch = all_domains[:DOMAIN_BATCH_SIZE]
        probe = _post({
            "company_domain_or":      sample_batch,
            "posted_at_max_age_days": max_age_days,
            "job_country_code_or":    ["US"],
            "limit":                  5,
        })
        sample_jobs = (probe or {}).get("data", [])
        sample_food = sum(1 for j in sample_jobs if _build_record(j))
        log.info(
            f"TheirStack DRY RUN — sample batch ({len(sample_batch)} domains, 5 jobs pulled): "
            f"{sample_food}/{len(sample_jobs)} passed local food keyword filter. "
            f"Full run: {total_batches} batches × up to {RESULTS_PER_PAGE * MAX_PAGES_PER_BATCH} "
            f"jobs/batch = up to {total_batches * RESULTS_PER_PAGE * MAX_PAGES_PER_BATCH} credits max."
        )
        if sample_jobs:
            for j in sample_jobs[:2]:
                log.info(f"  sample: [{j.get('company_domain','')}] {j.get('job_title','')} — {j.get('short_location','')}")
        return

    for batch_num, start in enumerate(range(0, len(all_domains), DOMAIN_BATCH_SIZE), 1):
        if _out_of_credits:
            break
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
        "posted_at_max_age_days": max_age_days,
        "job_country_code_or":    ["US"],
        "limit":                  RESULTS_PER_PAGE,
        "order_by":               [{"field": "date_posted", "desc": True}],
    }

    if dry_run:
        probe = _post({**payload, "limit": 3})
        jobs  = (probe or {}).get("data", [])
        food  = sum(1 for j in jobs if _build_record(j))
        log.info(f"TheirStack discovery DRY RUN: {food}/{len(jobs)} of 3 probed passed food keyword filter")
        return

    MAX_DISCOVERY_PAGES = 5
    for page in range(MAX_DISCOVERY_PAGES):
        data = _post({**payload, "offset": page * RESULTS_PER_PAGE})
        time.sleep(REQUEST_DELAY)

        if not data:
            break

        jobs = data.get("data", [])
        if not jobs:
            break

        credit_counter[0] += len(jobs)

        for job in jobs:
            url = (job.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            record = _build_record(job)
            if record:
                record["source"] = "TheirStack-Discovery"
                yield record

        if len(jobs) < RESULTS_PER_PAGE:
            break
