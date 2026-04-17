"""
Exa semantic search scraper.

Exa searches the entire web semantically — not just keyword matching.
It finds companies mentioning food perks in job posts, blog posts, review
sites (Glassdoor, Blind, Reddit), press releases, and benefits pages that
no ATS scraper would ever surface.

Requires: EXA_API_KEY secret in GitHub Actions / .env
API docs: https://docs.exa.ai/
"""
import logging
import os
import time
from typing import Iterator

import requests

from utils import find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

API_KEY   = os.getenv("EXA_API_KEY", "")
BASE_URL  = "https://api.exa.ai"
RESULTS_PER_QUERY = 15   # Exa charges per result — 15 is a good balance

# Semantic queries — written as natural language, not keyword strings.
# Exa's neural search finds conceptually similar content even without
# exact keyword matches.
EXA_QUERIES = [
    "New York company offers catered lunch to employees office perks",
    "NYC startup free meals DoorDash GrubHub employee benefits",
    "New York office food stipend meal credit company benefit",
    "NYC company catered breakfast lunch dinner employees",
    "New York company fully stocked kitchen free snacks office culture",
    "companies with the best office food perks New York City",
    "ezcater catered meals corporate office New York",
    "NYC company orders lunch employees every day",
    "New York employer meal benefit office catering program",
    "company provides free food employees New York hiring",
]

# Only include results from these domains (job boards, review sites, company sites)
# Empty list = search everything (broader but noisier)
INCLUDE_DOMAINS: list[str] = []

# Exclude pure news/unrelated domains
EXCLUDE_DOMAINS = [
    "wikipedia.org", "youtube.com", "twitter.com", "x.com",
    "facebook.com", "instagram.com", "tiktok.com",
]


def _search(query: str) -> list[dict]:
    """Run one Exa semantic search query. Returns list of result dicts."""
    if not API_KEY:
        log.warning("Exa: EXA_API_KEY not set — skipping")
        return []

    payload = {
        "query": query,
        "numResults": RESULTS_PER_QUERY,
        "useAutoprompt": True,          # Exa rewrites query for better recall
        "type": "neural",               # semantic, not keyword
        "contents": {
            "text": {
                "maxCharacters": 3000,  # enough to scan for perk keywords
                "includeHtmlTags": False,
            },
        },
    }

    if INCLUDE_DOMAINS:
        payload["includeDomains"] = INCLUDE_DOMAINS
    if EXCLUDE_DOMAINS:
        payload["excludeDomains"] = EXCLUDE_DOMAINS

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


def _extract_company(result: dict) -> str:
    """Best-effort company name from Exa result metadata."""
    # Exa returns author, title, and URL — try to derive company from these
    title = result.get("title", "")
    url   = result.get("url", "")

    # Many job posts have "Company Name — Job Title" or "Job Title at Company"
    for sep in [" | ", " — ", " - ", " at ", " @ "]:
        if sep in title:
            parts = title.split(sep)
            # Take the shorter segment — usually the company name
            company = min(parts, key=len).strip()
            if 2 < len(company) < 60:
                return company

    # Fall back to domain name (strip www., .com, etc.)
    import re
    domain_match = re.search(r"(?:https?://)?(?:www\.)?([^/]+)", url)
    if domain_match:
        domain = domain_match.group(1)
        # Strip TLD
        company = re.sub(r"\.(com|io|co|ai|net|org|jobs).*$", "", domain)
        company = company.replace("-", " ").replace("_", " ").title()
        if len(company) > 2:
            return company

    return ""


def scrape() -> Iterator[dict]:
    """
    Run all Exa queries and yield records matching NYC + food perk keywords.
    Each unique URL is yielded once regardless of how many queries match it.
    """
    if not API_KEY:
        log.warning("Exa: EXA_API_KEY not set — skipping entire source")
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

            text_block = result.get("text", "") or ""
            full_text  = clean_text(text_block)

            if not full_text:
                continue

            # NYC check
            title_str = result.get("title", "") or ""
            if not is_nyc(f"{full_text} {title_str} {url}"):
                continue

            # Food keyword check
            matched = find_food_keywords(full_text)
            if not matched:
                continue

            company = _extract_company(result)
            if not company:
                continue

            snip = excerpt(full_text, matched[0])

            published = result.get("publishedDate", "") or ""
            date_posted = published[:10] if published else ""

            yield {
                "source":                "Exa",
                "company":               company,
                "title":                 title_str,
                "location":              "New York, NY",
                "remote":                "Unknown",
                "food_keywords_matched": ", ".join(matched),
                "keyword_count":         len(matched),
                "perk_excerpt":          snip,
                "date_posted":           date_posted,
                "url":                   url,
            }

        time.sleep(1.0)  # stay well within Exa rate limits
