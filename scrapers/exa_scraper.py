"""
Exa semantic search scraper.

Restricted to ATS and job board domains only — no articles, no blogs,
no catering vendors. Every result must be a real job posting from a
real employer.

Requires: EXA_API_KEY secret in GitHub Actions / .env
API docs: https://docs.exa.ai/
"""
import logging
import os
import re
import time
from typing import Iterator

import requests

from utils import find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

API_KEY   = os.getenv("EXA_API_KEY", "")
BASE_URL  = "https://api.exa.ai"
RESULTS_PER_QUERY = 20

# Only search within actual job posting and employer career page domains.
# This is the critical filter — prevents articles, blogs, and vendors.
INCLUDE_DOMAINS = [
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "myworkdayjobs.com",
    "icims.com",
    "smartrecruiters.com",
    "bamboohr.com",
    "breezy.hr",
    "jobvite.com",
    "jobs.lever.co",
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.ashbyhq.com",
    "careers.smartrecruiters.com",
]

EXA_QUERIES = [
    "catered lunch free meals employee benefit New York",
    "DoorDash GrubHub Sharebite Forkable employee perk office New York",
    "meal stipend food stipend lunch credit New York",
    "fully stocked kitchen catered breakfast lunch New York",
    "daily lunch catered meals office New York",
]

# ATS URL patterns → extract employer slug from URL
ATS_PATTERNS = [
    # greenhouse: boards.greenhouse.io/{slug}/jobs/...
    (re.compile(r"greenhouse\.io/(?:v1/boards/)?([^/]+)/jobs"), "Greenhouse"),
    # lever: jobs.lever.co/{slug}/...
    (re.compile(r"lever\.co/([^/]+)"), "Lever"),
    # ashby: jobs.ashbyhq.com/{slug}/...
    (re.compile(r"ashbyhq\.com/([^/]+)"), "Ashby"),
    # workday: {tenant}.myworkdayjobs.com/...
    (re.compile(r"([^.]+)\.myworkdayjobs\.com"), "Workday"),
    # smartrecruiters: careers.smartrecruiters.com/{slug}/...
    (re.compile(r"smartrecruiters\.com/([^/]+)"), "SmartRecruiters"),
    # bamboohr: {tenant}.bamboohr.com/...
    (re.compile(r"([^.]+)\.bamboohr\.com"), "BambooHR"),
    # icims: {tenant}.icims.com/...
    (re.compile(r"([^.]+)\.icims\.com"), "iCIMS"),
]

# Slugs that are platform names, not companies — skip them
PLATFORM_SLUGS = {
    "greenhouse", "lever", "ashby", "workday", "smartrecruiters",
    "bamboohr", "icims", "jobvite", "breezy", "careers", "jobs",
    "en", "us", "external", "career", "site",
}


def _extract_company_from_url(url: str) -> str:
    """Extract employer name from ATS job posting URL."""
    for pattern, _ in ATS_PATTERNS:
        m = pattern.search(url)
        if m:
            slug = m.group(1).lower().strip()
            if slug and slug not in PLATFORM_SLUGS and len(slug) > 1:
                return slug.replace("-", " ").replace("_", " ").title()
    return ""


def _search(query: str) -> list[dict]:
    if not API_KEY:
        return []

    payload = {
        "query": query,
        "numResults": RESULTS_PER_QUERY,
        "useAutoprompt": False,   # autoprompt drifts too broad — keep queries literal
        "type": "keyword",        # keyword within our domain list is precise enough
        "includeDomains": INCLUDE_DOMAINS,
        "contents": {
            "text": {
                "maxCharacters": 3000,
                "includeHtmlTags": False,
            },
        },
    }

    try:
        resp = requests.post(
            f"{BASE_URL}/search",
            json=payload,
            headers={
                "x-api-key": API_KEY,
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        log.warning(f"Exa: query failed — '{query[:60]}': {e}")
        return []


def scrape() -> Iterator[dict]:
    if not API_KEY:
        log.warning("Exa: EXA_API_KEY not set — skipping")
        return

    seen_urls: set[str] = set()

    for query in EXA_QUERIES:
        log.info(f"Exa: searching — '{query[:70]}'")
        results = _search(query)
        log.info(f"Exa: {len(results)} results")

        for result in results:
            url = result.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            # Must be an actual job posting URL
            company = _extract_company_from_url(url)
            if not company:
                log.debug(f"Exa: skipping non-ATS URL {url}")
                continue

            text_block = result.get("text", "") or ""
            full_text  = clean_text(text_block)
            if not full_text:
                continue

            title_str = result.get("title", "") or ""

            if not is_nyc(f"{full_text} {title_str} {url}"):
                continue

            matched = find_food_keywords(full_text)
            if not matched:
                continue

            snip = excerpt(full_text, matched[0])
            published = result.get("publishedDate", "") or ""

            yield {
                "source":                "Exa",
                "company":               company,
                "title":                 title_str,
                "location":              "New York, NY",
                "remote":                "Unknown",
                "food_keywords_matched": ", ".join(matched),
                "keyword_count":         len(matched),
                "perk_excerpt":          snip,
                "date_posted":           published[:10] if published else "",
                "url":                   url,
            }

        time.sleep(1.0)
