"""
Targeted account scraper.

Takes a list of accounts (from account_filter.py), fingerprints each
domain's ATS type, then routes to the appropriate scraper.

Google Infra principles applied:
  - Per-target worker pools with independent rate limits
  - Write queue: scraper threads → queue → single batch writer
  - Observability: per-run metrics report
  - Vectorized keyword matching (compile once, apply many)
"""
import re
import time
import queue
import threading
import importlib
from datetime import date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from utils import find_food_keywords, excerpt, clean_text, log
from ats_fingerprint import get_ats
from account_filter import get_all_tiers, coverage_stats

# ── Per-ATS worker concurrency limits ────────────────────────────────────────
# Respects each platform's rate tolerance (Google Infra: per-target pools)
ATS_WORKERS = {
    "greenhouse": 8,
    "lever":      6,
    "ashby":      6,
    "workday":    3,
    "exa":        4,
    "other":      2,
}

# ── ATS job list URL templates ────────────────────────────────────────────────
ATS_JOB_LIST_URLS = {
    "greenhouse": [
        "https://boards.greenhouse.io/{slug}/jobs",
        "https://job-boards.greenhouse.io/{slug}/jobs",
    ],
    "lever":  ["https://jobs.lever.co/{slug}"],
    "ashby":  ["https://jobs.ashby.com/{slug}"],
}

TIMEOUT = 10
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# ── Pre-compiled food keyword pattern (vectorized, compiled once) ─────────────
from config import FOOD_KEYWORDS
_KW_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in sorted(FOOD_KEYWORDS, key=len, reverse=True)),
    re.IGNORECASE
)


def _fetch(url: str) -> str | None:
    try:
        r = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


def _scrape_greenhouse(slug: str, company: str) -> list[dict]:
    """Fetch Greenhouse job list and check descriptions for food keywords."""
    records = []
    for url_tpl in ATS_JOB_LIST_URLS["greenhouse"]:
        url = url_tpl.format(slug=slug)
        html = _fetch(url)
        if not html:
            continue
        # Extract job links from the board
        job_links = re.findall(
            rf'href="(https://(?:boards|job-boards)\.greenhouse\.io/{re.escape(slug)}/jobs/\d+)"',
            html
        )
        for job_url in set(job_links[:30]):  # cap at 30 per company
            time.sleep(0.3)
            jhtml = _fetch(job_url)
            if not jhtml:
                continue
            text = clean_text(jhtml)
            kws  = find_food_keywords(text)
            if kws:
                title_m = re.search(r'<title>([^<]+)</title>', jhtml, re.IGNORECASE)
                title = title_m.group(1).strip() if title_m else "Unknown Role"
                records.append({
                    "source":               "Targeted-Greenhouse",
                    "company":              company,
                    "title":                title,
                    "location":             "",
                    "remote":               "",
                    "food_keywords_matched": ", ".join(kws),
                    "keyword_count":        len(kws),
                    "perk_excerpt":         excerpt(text, kws[0]),
                    "date_posted":          date.today().isoformat(),
                    "url":                  job_url,
                })
        if records:
            break  # found results on first working URL
    return records


def _scrape_lever(slug: str, company: str) -> list[dict]:
    records = []
    url = f"https://jobs.lever.co/{slug}"
    html = _fetch(url)
    if not html:
        return records
    job_links = re.findall(
        rf'href="(https://jobs\.lever\.co/{re.escape(slug)}/[a-f0-9-]{{36}})"',
        html
    )
    for job_url in set(job_links[:30]):
        time.sleep(0.3)
        jhtml = _fetch(job_url)
        if not jhtml:
            continue
        text = clean_text(jhtml)
        kws  = find_food_keywords(text)
        if kws:
            title_m = re.search(r'<title>([^<]+)</title>', jhtml, re.IGNORECASE)
            title = title_m.group(1).strip() if title_m else "Unknown Role"
            records.append({
                "source":               "Targeted-Lever",
                "company":              company,
                "title":                title,
                "location":             "",
                "remote":               "",
                "food_keywords_matched": ", ".join(kws),
                "keyword_count":        len(kws),
                "perk_excerpt":         excerpt(text, kws[0]),
                "date_posted":          date.today().isoformat(),
                "url":                  job_url,
            })
    return records


def _scrape_ashby(slug: str, company: str) -> list[dict]:
    records = []
    url = f"https://jobs.ashby.com/{slug}"
    html = _fetch(url)
    if not html:
        return records
    job_links = re.findall(
        rf'href="(/jobs/{re.escape(slug)}/[^"]+)"',
        html
    )
    for path in set(job_links[:30]):
        job_url = f"https://jobs.ashby.com{path}"
        time.sleep(0.3)
        jhtml = _fetch(job_url)
        if not jhtml:
            continue
        text = clean_text(jhtml)
        kws  = find_food_keywords(text)
        if kws:
            title_m = re.search(r'<title>([^<]+)</title>', jhtml, re.IGNORECASE)
            title = title_m.group(1).strip() if title_m else "Unknown Role"
            records.append({
                "source":               "Targeted-Ashby",
                "company":              company,
                "title":                title,
                "location":             "",
                "remote":               "",
                "food_keywords_matched": ", ".join(kws),
                "keyword_count":        len(kws),
                "perk_excerpt":         excerpt(text, kws[0]),
                "date_posted":          date.today().isoformat(),
                "url":                  job_url,
            })
    return records


def _scrape_exa_targeted(domain: str, company: str) -> list[dict]:
    """Exa fallback for custom career pages."""
    try:
        from scrapers.exa_scraper import _search_exa, _extract_records
        query = f'site:{domain} careers jobs "free lunch" OR "catered meals" OR "meal stipend" OR "food stipend"'
        return _extract_records(query, company_hint=company)
    except Exception as e:
        log.debug(f"Exa targeted fallback failed for {domain}: {e}")
        return []


def _scrape_account(account: dict) -> list[dict]:
    """Route one account to the right scraper. Never raises."""
    domain  = account.get("_domain", "")
    company = account.get("Account Name", domain)
    if not domain:
        return []
    try:
        ats_type, slug = get_ats(domain)
        if ats_type == "greenhouse":
            return _scrape_greenhouse(slug, company)
        elif ats_type == "lever":
            return _scrape_lever(slug, company)
        elif ats_type == "ashby":
            return _scrape_ashby(slug, company)
        elif ats_type == "exa":
            return _scrape_exa_targeted(domain, company)
        else:
            return []  # unsupported ATS (Workday, iCIMS, Taleo) — future phases
    except Exception as e:
        log.error(f"  Targeted scrape failed for {company} ({domain}): {e}")
        return []


def run_targeted(
    tier2_sample: int = 1000,
    include_tier3: bool = False,
    dry_run: bool = False,
) -> list[dict]:
    """
    Main entry point for targeted account scraping.
    Returns list of job records with food perk keywords.
    """
    accounts = get_all_tiers(tier2_sample=tier2_sample, include_tier3=include_tier3)
    log.info(f"Targeted scrape: {len(accounts)} accounts (T1={875}, T2≤{tier2_sample})")

    if dry_run:
        log.info("DRY RUN — showing first 5 accounts + ATS fingerprints")
        for acc in accounts[:5]:
            domain = acc["_domain"]
            ats, slug = get_ats(domain)
            print(f"  {acc['Account Name']:40s}  {domain:30s}  ats={ats}  slug={slug}")
        return []

    # ── Write queue: scraper threads push here, one writer thread flushes ─────
    result_queue: queue.Queue = queue.Queue()
    all_records: list[dict]   = []
    writer_done = threading.Event()

    def writer_thread():
        while not writer_done.is_set() or not result_queue.empty():
            try:
                records = result_queue.get(timeout=0.5)
                all_records.extend(records)
                result_queue.task_done()
            except queue.Empty:
                continue

    writer = threading.Thread(target=writer_thread, daemon=True)
    writer.start()

    # ── Per-ATS worker pools ──────────────────────────────────────────────────
    # Group accounts by ATS type for targeted concurrency control
    ats_groups: dict[str, list[dict]] = defaultdict(list)
    log.info("Fingerprinting ATS types...")

    with ThreadPoolExecutor(max_workers=10) as fp_pool:
        futures = {fp_pool.submit(get_ats, acc["_domain"]): acc for acc in accounts}
        for future in as_completed(futures):
            acc = futures[future]
            try:
                ats_type, _ = future.result()
                ats_groups[ats_type].append(acc)
            except Exception:
                ats_groups["exa"].append(acc)

    # Log ATS distribution
    for ats, accs in sorted(ats_groups.items(), key=lambda x: -len(x[1])):
        log.info(f"  {ats:15s}: {len(accs):4d} accounts")

    # ── Scrape each ATS group with its own concurrency limit ─────────────────
    for ats_type, accs in ats_groups.items():
        workers = ATS_WORKERS.get(ats_type, 2)
        log.info(f"Scraping {ats_type} ({len(accs)} accounts, {workers} workers)")
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_scrape_account, acc): acc for acc in accs}
            for future in as_completed(futures):
                records = future.result()
                if records:
                    result_queue.put(records)
                    acc = futures[future]
                    log.info(f"  ✓ {acc['Account Name']}: {len(records)} perk matches")

    writer_done.set()
    writer.join()

    # ── Observability report ──────────────────────────────────────────────────
    stats = coverage_stats()
    total_checked = len(accounts)
    companies_with_signal = len({r["company"] for r in all_records})
    match_rate = companies_with_signal / total_checked * 100 if total_checked else 0

    print(f"""
Targeted Scrape Report
  Accounts checked     : {total_checked}
  Companies with signal: {companies_with_signal}
  Match rate           : {match_rate:.1f}%
  Raw job matches      : {len(all_records)}
  ATS distribution     : {dict(sorted(ats_groups.items(), key=lambda x: -len(x[1])))}
  Full cycle ETA       : ~{stats['weeks_to_full_cycle']} weeks at {tier2_sample}/run
""")

    return all_records
