"""
Firecrawl ATS job posting scraper.

Searches within ATS domains only — every result is a real job posting
from a real employer. No articles, blogs, or catering vendor sites.

Requires: FIRECRAWL_API_KEY secret in GitHub Actions / .env
API docs: https://docs.firecrawl.dev/
"""
import logging
import os
import re
import time
from typing import Iterator

import requests

from utils import find_food_keywords, is_in_target_location, excerpt, clean_text

log = logging.getLogger(__name__)

API_KEY  = os.getenv("FIRECRAWL_API_KEY", "")
BASE_URL = "https://api.firecrawl.dev/v1"

SEARCH_RESULTS_PER_QUERY = 5
SCRAPE_TIMEOUT           = 45

# Queries scoped tightly to ATS domains — finds job postings we missed
# in our hardcoded slug lists
FIRECRAWL_QUERIES = [
    'site:greenhouse.io "catered lunch" OR "catered meals"',
    'site:greenhouse.io "meal stipend" OR "food stipend"',
    'site:greenhouse.io "DoorDash" OR "Sharebite" OR "Forkable"',
    'site:lever.co "catered lunch" OR "catered meals"',
    'site:lever.co "meal stipend" OR "food stipend"',
    'site:ashbyhq.com "catered lunch" OR "meal stipend"',
    'site:ashbyhq.com "DoorDash" OR "Sharebite"',
    'site:myworkdayjobs.com "catered lunch" OR "meal stipend"',
    'site:smartrecruiters.com "catered lunch" OR "free lunch"',
    'site:bamboohr.com "catered lunch" OR "meal stipend"',
]

# ATS URL → employer name extraction
ATS_PATTERNS = [
    (re.compile(r"greenhouse\.io/(?:v1/boards/)?([^/]+)/jobs"), "Greenhouse"),
    (re.compile(r"lever\.co/([^/]+)"), "Lever"),
    (re.compile(r"ashbyhq\.com/([^/]+)"), "Ashby"),
    (re.compile(r"([^.]+)\.myworkdayjobs\.com"), "Workday"),
    (re.compile(r"smartrecruiters\.com/([^/]+)"), "SmartRecruiters"),
    (re.compile(r"([^.]+)\.bamboohr\.com"), "BambooHR"),
    (re.compile(r"([^.]+)\.icims\.com"), "iCIMS"),
]

PLATFORM_SLUGS = {
    "greenhouse", "lever", "ashby", "workday", "smartrecruiters",
    "bamboohr", "icims", "jobvite", "breezy", "careers", "jobs",
    "en", "us", "external", "career", "site", "boards", "job-boards",
}


def _extract_company_from_url(url: str) -> str:
    for pattern, _ in ATS_PATTERNS:
        m = pattern.search(url)
        if m:
            slug = m.group(1).lower().strip()
            if slug and slug not in PLATFORM_SLUGS and len(slug) > 1:
                return slug.replace("-", " ").replace("_", " ").title()
    return ""


def _firecrawl_search(query: str) -> list[dict]:
    if not API_KEY:
        return []
    try:
        resp = requests.post(
            f"{BASE_URL}/search",
            json={
                "query": query,
                "limit": SEARCH_RESULTS_PER_QUERY,
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


def scrape() -> Iterator[dict]:
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

            # Require ATS URL — if we can't extract an employer from the URL,
            # it's not a job posting and we skip it
            company = _extract_company_from_url(url)
            if not company:
                log.debug(f"Firecrawl: skipping non-ATS URL {url}")
                continue

            markdown  = result.get("markdown", "") or ""
            full_text = clean_text(markdown)
            if not full_text:
                continue

            metadata  = result.get("metadata", {}) or {}
            title_str = metadata.get("title", "") or ""

            if not is_in_target_location(f"{full_text} {title_str} {url}"):
                continue

            matched = find_food_keywords(full_text)
            if not matched:
                continue

            snip = excerpt(full_text, matched[0])

            yield {
                "source":                "Firecrawl",
                "company":               company,
                "title":                 title_str,
                "location":              "",
                "remote":                "Unknown",
                "food_keywords_matched": ", ".join(matched),
                "keyword_count":         len(matched),
                "perk_excerpt":          snip,
                "date_posted":           "",
                "url":                   url,
            }

        time.sleep(2.0)
