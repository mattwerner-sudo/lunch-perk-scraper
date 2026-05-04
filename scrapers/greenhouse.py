"""
Greenhouse ATS scraper — full tenant crawl.

Discovers all active Greenhouse companies via sitemap (~15,000 tenants),
then queries each one's public JSON API in parallel for food perk keywords.

API: https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true
  - Public, no auth, no rate limit documented
  - content=true includes full job description inline — no per-job fetch needed
  - Returns all open roles for that company in one call

Falls back to the hardcoded GREENHOUSE_SLUGS list if sitemap is unavailable.
"""
import json
import logging
import requests
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import find_food_keywords, is_in_target_location, excerpt, clean_text
from .ats_discovery import get_slugs

log = logging.getLogger(__name__)

MAX_WORKERS = 30   # parallel company probes
TIMEOUT     = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# Fallback list used when sitemap discovery fails
GREENHOUSE_SLUGS = [
    "figma", "airtable", "stripe", "hubspot", "twilio", "amplitude",
    "mixpanel", "klaviyo", "intercom", "mongodb", "datadog", "braze",
    "movableink", "yotpo", "sisense", "appsflyer", "taboola",
    "pinterest", "reddit", "lyft", "airbnb", "dropbox", "squarespace",
    "kickstarter", "duolingo", "pagerduty", "cloudflare", "fastly",
    "elastic", "databricks", "seatgeek", "tripadvisor", "opendoor",
    "mindbody", "cision", "similarweb", "okta", "newrelic", "sumologic",
    "contentful", "storyblok", "pendo", "zuora", "marqeta",
    "fivetran", "starburst", "dremio", "clickhouse",
    "monday", "notion", "lattice", "greenhouse",
    "segment", "sendbird", "freshworks", "drift",
    "outreach", "salesloft", "terminus", "rollworks", "bombora",
    "trustradius", "capterra", "doubleverify", "pubmatic", "northbeam",
    "point72", "apollo", "betterment", "robinhood", "plaid", "wealthfront",
    "virtu", "iex", "creditkarma", "cleo", "galileo",
    "coinbase", "gemini", "ripple", "fireblocks", "alchemy", "consensys",
    "sofi", "chime", "alloy", "lithic", "highnote",
    "schonfeld", "icon", "natera", "beamtherapeutics", "10xgenomics", "veracyte",
    "zocdoc", "ritual", "calm", "peloton", "classpass",
    "oscar", "cerebral", "waymark", "cityblock",
    "gusto", "justworks", "cultureamp",
    "buzzfeed", "voxmedia", "forbes", "axios", "semafor", "fandom",
    "fanduel", "octagon", "geniussports",
    "coursera", "masterclass", "udemy", "2u",
    "glossier", "allbirds", "brooklinen", "renttherunway", "rebag",
    "harrys", "mejuri", "everlane", "etsy", "poshmark",
    "narvar", "aftership", "flexport", "shipmonk", "project44", "fourkites",
    "costar", "vts", "crexi", "orchard",
    "doordash", "goldbelly", "misfitsmarket", "hungryroot",
    "touchbistro", "revel", "agilysys", "instacart", "sweetgreen",
    "alixpartners", "ogilvy", "wpp",
]


def _fetch_company(slug: str) -> list[dict]:
    """Fetch all food-perk jobs for one Greenhouse company slug."""
    try:
        r = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            headers=HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200:
            return []
        jobs = r.json().get("jobs", [])
    except Exception:
        return []

    records = []
    for job in jobs:
        location    = (job.get("location") or {}).get("name", "")
        content_html = job.get("content", "") or ""
        content_text = clean_text(content_html)

        if not is_in_target_location(f"{location} {content_text}"):
            continue

        matched = find_food_keywords(content_text)
        if not matched:
            continue

        # Use company name from API if available, fall back to slug
        company_name = (
            job.get("company_name")
            or job.get("metadata", [{}])[0].get("value", "")
            or slug.replace("-", " ").title()
        )

        records.append({
            "source":                "Greenhouse",
            "company":               company_name,
            "title":                 job.get("title", ""),
            "location":              location,
            "url":                   job.get("absolute_url", ""),
            "date_posted":           (job.get("updated_at", "") or "")[:10],
            "food_keywords_matched": ", ".join(matched),
            "keyword_count":         len(matched),
            "perk_excerpt":          excerpt(content_text, matched[0]),
            "remote":                _infer_remote(location, content_text),
        })
    return records


def scrape(slugs: list[str] | None = None) -> Iterator[dict]:
    """
    Crawl all Greenhouse tenants in parallel, yield food-perk job records.
    Discovers tenants via sitemap; falls back to hardcoded list.
    """
    if slugs is None:
        slugs = get_slugs("greenhouse", fallback=GREENHOUSE_SLUGS)

    log.info(f"Greenhouse: crawling {len(slugs):,} tenants ({MAX_WORKERS} workers)")
    seen_urls: set[str] = set()
    hits = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_company, slug): slug for slug in slugs}
        for future in as_completed(futures):
            try:
                for rec in future.result():
                    url = rec.get("url", "")
                    if url and url in seen_urls:
                        continue
                    if url:
                        seen_urls.add(url)
                    hits += 1
                    yield rec
            except Exception as e:
                log.debug(f"Greenhouse worker error: {e}")

    log.info(f"Greenhouse: {hits} food-perk records from {len(slugs):,} tenants")


def _infer_remote(location: str, text: str) -> str:
    combined = (location + " " + text).lower()
    if "remote" in combined.split()[:20]:   # remote in location field
        return "Remote"
    if "hybrid" in combined:
        return "Hybrid"
    return "On-site"
