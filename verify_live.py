"""
Live job verification.

After scraping, every job URL is checked to confirm the posting is still
active. Stale/closed listings are removed before the CSV is written.

Each ATS has a different "job closed" pattern — we handle them explicitly
rather than relying on HTTP status alone (most ATSs return 200 even for
closed jobs, just with different page content).
"""
import re
import logging
import time
import random
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ── Closed-job signals by ATS domain ────────────────────────────────────────
# Each entry: (domain_substring, list_of_closed_signals_in_page_text)
# Signals are case-insensitive substrings searched in the page text/HTML.

CLOSED_SIGNALS: list[tuple[str, list[str]]] = [
    # Greenhouse
    ("boards.greenhouse.io", [
        "this job is no longer available",
        "this position has been filled",
        "job listing is no longer active",
    ]),
    # Lever
    ("jobs.lever.co", [
        "this job listing is no longer accepting applications",
        "position has been filled",
        "this role is no longer available",
    ]),
    # Ashby
    ("jobs.ashbyhq.com", [
        "this job is no longer available",
        "position is no longer open",
    ]),
    # Workday
    ("myworkdayjobs.com", [
        "this job is no longer available",
        "job req status: closed",
        "no longer accepting applications",
    ]),
    # Built In
    ("builtin.com", [
        "this job is no longer available",
        "job listing is no longer available",
        "position has been filled",
    ]),
    # Glassdoor
    ("glassdoor.com", [
        "this job is no longer available",
        "job listing is no longer available",
        "this position has been filled",
    ]),
    # LinkedIn
    ("linkedin.com", [
        "no longer accepting applications",
        "this job is closed",
        "this job is no longer available",
    ]),
    # Indeed
    ("indeed.com", [
        "this job has expired",
        "this job is no longer available",
        "the job you are looking for has expired",
    ]),
    # Generic fallback — applies to all URLs
    ("", [
        "this position has been filled",
        "this job is no longer available",
        "this listing has expired",
        "posting has been removed",
        "job has been filled",
        "no longer accepting applications",
        "position is closed",
        "job is closed",
        "role is no longer available",
        "this req is no longer open",
    ]),
]

# HTTP status codes that definitively mean "gone"
DEAD_STATUS_CODES = {404, 410}

# Status codes that mean "alive"
ALIVE_STATUS_CODES = {200, 201, 301, 302}

REQUEST_TIMEOUT = 12
DELAY = 0.8  # seconds between verification requests


def _get_closed_signals_for_url(url: str) -> list[str]:
    """Return the combined closed signals for a given URL's domain."""
    domain = urlparse(url).netloc.lower()
    signals = []
    for domain_sub, sigs in CLOSED_SIGNALS:
        if not domain_sub or domain_sub in domain:
            signals.extend(sigs)
    return signals


def check_url_live(url: str, session: Optional[requests.Session] = None) -> tuple[bool, str]:
    """
    Check if a job posting URL is still live.

    Returns:
        (is_live: bool, reason: str)
    """
    if not url or not url.startswith("http"):
        return False, "invalid URL"

    sess = session or requests.Session()
    time.sleep(DELAY + random.uniform(0, 0.3))

    try:
        resp = sess.get(
            url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
    except requests.exceptions.Timeout:
        # Timeout ≠ dead — the server is just slow; keep it
        return True, "timeout (assumed live)"
    except requests.exceptions.ConnectionError:
        return False, "connection error"
    except Exception as e:
        return True, f"error ({e}) — assumed live"

    # Definitive HTTP death
    if resp.status_code in DEAD_STATUS_CODES:
        return False, f"HTTP {resp.status_code}"

    # Non-200 that isn't a redirect — treat as dead
    if resp.status_code not in ALIVE_STATUS_CODES:
        return False, f"HTTP {resp.status_code}"

    # Check page content for closed signals
    page_text = resp.text.lower()
    closed_signals = _get_closed_signals_for_url(url)
    for signal in closed_signals:
        if signal in page_text:
            return False, f"page says: '{signal}'"

    # ATS-specific: Greenhouse JSON API returns is_live flag
    domain = urlparse(url).netloc.lower()
    if "boards-api.greenhouse.io" in domain:
        try:
            data = resp.json()
            if not data.get("is_listed", True):
                return False, "greenhouse: is_listed=false"
        except Exception:
            pass

    # Ashby: job detail JSON has a status field
    if "ashbyhq.com" in domain:
        try:
            data = resp.json()
            status = data.get("status", "").lower()
            if status in {"closed", "draft", "archived"}:
                return False, f"ashby status: {status}"
        except Exception:
            pass

    return True, "live"


def verify_jobs(records: list[dict], max_workers: int = 8) -> list[dict]:
    """
    Filter a list of job records to only those with live postings.

    Uses a thread pool for parallel verification.
    Returns only records where the URL is confirmed live.
    """
    import concurrent.futures

    if not records:
        return []

    log.info(f"Verifying {len(records)} job URLs for liveness...")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    results = []
    dead_count = 0

    def check(record):
        url = record.get("url", "")
        is_live, reason = check_url_live(url, session)
        return record, is_live, reason

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(check, r): r for r in records}
        for future in concurrent.futures.as_completed(futures):
            try:
                record, is_live, reason = future.result()
                if is_live:
                    results.append(record)
                else:
                    dead_count += 1
                    log.info(
                        f"DEAD [{reason}] {record.get('company')} — "
                        f"{record.get('title')} ({record.get('url', '')[:60]})"
                    )
            except Exception as e:
                # On error, keep the record (fail open)
                record = futures[future]
                results.append(record)
                log.warning(f"Verification error for {record.get('url')}: {e}")

    log.info(
        f"Liveness check complete: {len(results)} live, {dead_count} removed."
    )
    return results
