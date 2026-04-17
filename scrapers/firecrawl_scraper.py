"""
Firecrawl careers-page scraper.

Two modes:
  1. DISCOVERY — Firecrawl's /search endpoint finds URLs across the web
     matching a query, then scrapes each page for perk keywords.
  2. VERIFICATION — given a company domain, scrape their /careers or
     /benefits page directly and check for food perks.

Mode 1 runs as part of the weekly scrape pipeline.
Mode 2 is called by enrich.py to verify companies found by other scrapers.

Requires: FIRECRAWL_API_KEY secret in GitHub Actions / .env
API docs: https://docs.firecrawl.dev/
"""
import logging
import os
import time
from typing import Iterator

import requests

from utils import find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

API_KEY  = os.getenv("FIRECRAWL_API_KEY", "")
BASE_URL = "https://api.firecrawl.dev/v1"

SEARCH_RESULTS_PER_QUERY = 5   # Firecrawl search is expensive — keep low
SCRAPE_TIMEOUT           = 45  # seconds; JS-heavy pages take longer

# Discovery queries — Firecrawl searches AND scrapes in one call.
# Focused on finding careers/benefits pages, not blog posts.
FIRECRAWL_QUERIES = [
    "site:greenhouse.io OR site:lever.co OR site:ashbyhq.com \"catered lunch\" OR \"free meals\" New York",
    "\"meal stipend\" OR \"food stipend\" OR \"catered meals\" jobs New York City site:careers",
    "\"DoorDash\" OR \"Forkable\" OR \"Sharebite\" employee benefit New York office careers",
    "\"stocked kitchen\" OR \"free lunch\" OR \"daily lunch\" New York company benefits",
    "\"lunch provided\" OR \"meals provided\" New York office hiring",
]

# Career page URL suffixes to try when verifying a domain
CAREERS_PATHS = [
    "/careers",
    "/jobs",
    "/join-us",
    "/work-with-us",
    "/about/careers",
    "/company/careers",
    "/en/careers",
    "/benefits",
    "/perks",
    "/life-at",
]


def _firecrawl_search(query: str) -> list[dict]:
    """Use Firecrawl /search to find and scrape matching pages."""
    if not API_KEY:
        return []
    try:
        resp = requests.post(
            f"{BASE_URL}/search",
            json={
                "query":   query,
                "limit":   SEARCH_RESULTS_PER_QUERY,
                "scrapeOptions": {
                    "formats": ["markdown"],
                    "onlyMainContent": True,
                },
            },
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        log.warning(f"Firecrawl search failed — '{query[:60]}': {e}")
        return []


def _firecrawl_scrape(url: str) -> str:
    """Scrape a single URL via Firecrawl. Returns clean markdown text."""
    if not API_KEY:
        return ""
    try:
        resp = requests.post(
            f"{BASE_URL}/scrape",
            json={
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": True,
                "waitFor": 2000,   # ms — let JS render
            },
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=SCRAPE_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
        return data.get("markdown", "") or ""
    except Exception as e:
        log.warning(f"Firecrawl scrape failed — {url}: {e}")
        return ""


def verify_domain(domain: str) -> dict | None:
    """
    Verify a company domain has food perks by scraping their careers/benefits page.
    Returns a record dict if perks found, None otherwise.
    Called by enrich.py for new companies discovered by other scrapers.
    """
    if not API_KEY:
        return None

    domain = domain.rstrip("/")
    if not domain.startswith("http"):
        domain = f"https://{domain}"

    for path in CAREERS_PATHS:
        url = f"{domain}{path}"
        log.info(f"Firecrawl: verifying {url}")
        text = _firecrawl_scrape(url)
        if not text:
            continue

        full_text = clean_text(text)
        matched = find_food_keywords(full_text)
        if matched:
            snip = excerpt(full_text, matched[0])
            return {
                "source":                "Firecrawl/Benefits",
                "url":                   url,
                "food_keywords_matched": ", ".join(matched),
                "keyword_count":         len(matched),
                "perk_excerpt":          snip,
            }
        time.sleep(1.0)

    return None


def _extract_company(result: dict) -> str:
    """Best-effort company name from a Firecrawl search result."""
    import re
    metadata = result.get("metadata", {}) or {}
    title    = metadata.get("title", "") or result.get("title", "") or ""
    url      = result.get("url", "")

    for sep in [" | ", " — ", " - ", " at ", " @ ", " · "]:
        if sep in title:
            parts = title.split(sep)
            company = min(parts, key=len).strip()
            if 2 < len(company) < 60:
                return company

    domain_match = re.search(r"(?:https?://)?(?:www\.)?([^/]+)", url)
    if domain_match:
        domain = domain_match.group(1)
        company = re.sub(r"\.(com|io|co|ai|net|org|jobs).*$", "", domain)
        company = company.replace("-", " ").replace("_", " ").title()
        if len(company) > 2:
            return company

    return ""


def scrape() -> Iterator[dict]:
    """
    Discovery mode: run Firecrawl search queries and yield perk-matched records.
    """
    if not API_KEY:
        log.warning("Firecrawl: FIRECRAWL_API_KEY not set — skipping")
        return

    seen_urls: set[str] = set()

    for query in FIRECRAWL_QUERIES:
        log.info(f"Firecrawl: searching — '{query[:70]}'")
        results = _firecrawl_search(query)
        log.info(f"Firecrawl: {len(results)} results")

        for result in results:
            url = result.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            markdown  = result.get("markdown", "") or ""
            full_text = clean_text(markdown)

            if not full_text:
                continue

            metadata  = result.get("metadata", {}) or {}
            title_str = metadata.get("title", "") or ""

            if not is_nyc(f"{full_text} {title_str} {url}"):
                continue

            matched = find_food_keywords(full_text)
            if not matched:
                continue

            company = _extract_company(result)
            if not company:
                continue

            snip = excerpt(full_text, matched[0])

            yield {
                "source":                "Firecrawl",
                "company":               company,
                "title":                 title_str,
                "location":              "New York, NY",
                "remote":                "Unknown",
                "food_keywords_matched": ", ".join(matched),
                "keyword_count":         len(matched),
                "perk_excerpt":          snip,
                "date_posted":           "",
                "url":                   url,
            }

        time.sleep(2.0)  # Firecrawl search is heavier — be conservative
