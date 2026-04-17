"""
Wellfound (formerly AngelList) scraper.

Wellfound is the dominant job board for NYC startups and scale-ups.
Companies in Tech, Fintech, Biotech, and Business Services — all core
ezcater ICP verticals — post here heavily.

Public API endpoint (no auth required):
  https://wellfound.com/jobs — search interface
  https://api.wellfound.com/graphql — GraphQL API (public, no key needed)
"""
import json
import logging
import time
from typing import Iterator

import requests
from bs4 import BeautifulSoup

from utils import get, find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

GRAPHQL_URL = "https://wellfound.com/graphql"
JOBS_URL    = "https://wellfound.com/jobs"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://wellfound.com/jobs",
    "x-requested-with": "XMLHttpRequest",
}

# NYC location IDs on Wellfound
NYC_LOCATION_ID = "1"  # New York City

# Search terms mapped to our food keywords
WELLFOUND_SEARCHES = [
    "free lunch",
    "catered meals",
    "DoorDash",
    "GrubHub",
    "meal stipend",
    "stocked kitchen",
    "lunch provided",
]


def scrape() -> Iterator[dict]:
    """Scrape Wellfound for NYC jobs mentioning food perks."""
    yield from _scrape_via_search()


def _scrape_via_search() -> Iterator[dict]:
    """
    Use Wellfound's job search to find NYC postings with food keywords.
    Falls back to HTML scraping if GraphQL is unavailable.
    """
    seen_urls = set()

    for keyword in WELLFOUND_SEARCHES:
        log.info(f"Wellfound: searching '{keyword}'")

        # Try GraphQL first
        results = _graphql_search(keyword)

        # Fall back to HTML scrape
        if not results:
            results = _html_search(keyword)

        log.info(f"Wellfound: {len(results)} results for '{keyword}'")

        for job in results:
            url = job.get("url", "") or job.get("jobUrl", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            company  = job.get("company", "") or job.get("companyName", "")
            title    = job.get("title", "") or job.get("jobTitle", "")
            location = job.get("location", "")
            desc     = job.get("description", "") or job.get("body", "")
            full_text = clean_text(desc)

            if not is_nyc(f"{location} {full_text}"):
                continue

            matched = find_food_keywords(full_text)
            if not matched:
                # The search keyword matched, trust it
                matched = [keyword]

            snip = excerpt(full_text, matched[0]) if full_text else (
                f"Found via Wellfound search for '{keyword}'"
            )

            yield {
                "source":                "Wellfound",
                "company":               company,
                "title":                 title,
                "location":              location or "New York, NY",
                "remote":                _infer_remote(location, full_text),
                "food_keywords_matched": ", ".join(matched),
                "keyword_count":         len(matched),
                "perk_excerpt":          snip,
                "date_posted":           job.get("postedAt", "")[:10] if job.get("postedAt") else "",
                "url":                   url,
            }

        time.sleep(1.5)


def _graphql_search(keyword: str) -> list[dict]:
    """Query Wellfound's GraphQL API for jobs."""
    query = """
    query JobSearchResults($query: String!, $locationId: String) {
      talent {
        jobListings(query: $query, locationId: $locationId, first: 50) {
          edges {
            node {
              id
              title
              description
              remoteConfig { kind }
              locationNames
              jobUrl
              postedAt
              startup {
                name
                highConcept
                markets { displayName }
              }
            }
          }
        }
      }
    }
    """
    variables = {
        "query": keyword,
        "locationId": NYC_LOCATION_ID,
    }
    try:
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        edges = (
            data.get("data", {})
                .get("talent", {})
                .get("jobListings", {})
                .get("edges", [])
        )
        results = []
        for edge in edges:
            node = edge.get("node", {})
            startup = node.get("startup", {})
            location_names = node.get("locationNames", [])
            results.append({
                "url":         node.get("jobUrl", ""),
                "title":       node.get("title", ""),
                "description": node.get("description", ""),
                "company":     startup.get("name", ""),
                "location":    ", ".join(location_names) if location_names else "New York, NY",
                "postedAt":    node.get("postedAt", ""),
            })
        return results
    except Exception as e:
        log.warning(f"Wellfound GraphQL failed for '{keyword}': {e}")
        return []


def _html_search(keyword: str) -> list[dict]:
    """Fall back to HTML scraping of Wellfound job search."""
    params = {
        "q":        keyword,
        "location": "New York City",
    }
    resp = get(JOBS_URL, params=params, headers=HEADERS)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Extract from __NEXT_DATA__ if available
    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data:
        try:
            data = json.loads(next_data.string)
            jobs_raw = (
                data.get("props", {})
                    .get("pageProps", {})
                    .get("jobListings", [])
            )
            results = []
            for j in jobs_raw:
                results.append({
                    "url":         j.get("jobUrl", ""),
                    "title":       j.get("title", ""),
                    "description": j.get("description", ""),
                    "company":     j.get("companyName", ""),
                    "location":    j.get("locationName", "New York, NY"),
                })
            return results
        except Exception:
            pass

    # Last resort: parse job cards from HTML
    results = []
    cards = soup.select("[data-test='JobListing'], .job-listing, [class*='jobListing']")
    for card in cards:
        title_el   = card.select_one("h2, h3, [class*='title']")
        company_el = card.select_one("[class*='company'], [class*='startup']")
        link_el    = card.select_one("a[href*='/jobs/']")
        title   = title_el.get_text(strip=True) if title_el else ""
        company = company_el.get_text(strip=True) if company_el else ""
        url     = "https://wellfound.com" + link_el["href"] if link_el else ""
        if title and url:
            results.append({
                "url": url, "title": title, "company": company,
                "description": "", "location": "New York, NY",
            })
    return results


def _infer_remote(location: str, text: str) -> str:
    combined = (location + " " + text).lower()
    if "remote" in combined:
        return "Remote"
    if "hybrid" in combined:
        return "Hybrid"
    return "On-site"
