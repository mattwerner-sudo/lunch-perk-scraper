"""
Ashby ATS scraper — full tenant crawl.

Discovers all active Ashby companies via sitemap (~5,000 tenants),
then queries each one's public JSON API in parallel for food perk keywords.

API: https://api.ashbyhq.com/posting-api/job-board/{slug}
  - Public, no auth
  - Returns all open postings with descriptionHtml + descriptionSections inline
  - No per-job detail fetch needed

Falls back to hardcoded ASHBY_SLUGS if sitemap unavailable.
"""
import logging
import requests
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import find_food_keywords, is_in_target_location, excerpt, clean_text
from .ats_discovery import get_slugs

log = logging.getLogger(__name__)

MAX_WORKERS = 30
TIMEOUT     = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

ASHBY_SLUGS = [
    "linear", "loom", "runway", "causal", "hightouch", "replit", "harvey",
    "notion", "retool", "openai", "anthropic", "cohere",
    "mercury", "ramp", "airwallex", "capchase", "clearco", "parafin", "slope", "brex",
    "vanta", "drata",
    "leapsome", "deel", "oyster", "lattice",
    "flatiron-health", "cityblock", "benchling", "ro", "chainalysis",
    "fireblocks", "movable-ink", "sprinklr", "diligent", "tempus",
    "spekit",
]


def _fetch_company(slug: str) -> list[dict]:
    """Fetch all food-perk jobs for one Ashby company slug."""
    try:
        r = requests.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
            headers=HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200:
            return []
        data = r.json()
        jobs = data.get("jobs", data.get("jobPostings", []))
    except Exception:
        return []

    records = []
    for job in jobs:
        location     = job.get("location", "") or ""
        desc_html    = job.get("descriptionHtml", "") or job.get("descriptionPlain", "") or ""
        section_text = " ".join(
            s.get("content", "") for s in (job.get("descriptionSections") or [])
        )
        full_text = clean_text(f"{desc_html} {section_text}")

        if not is_in_target_location(f"{location} {full_text}"):
            continue

        matched = find_food_keywords(full_text)
        if not matched:
            continue

        company_name = job.get("companyName", "") or slug.replace("-", " ").title()

        records.append({
            "source":                "Ashby",
            "company":               company_name,
            "title":                 job.get("title", ""),
            "location":              location,
            "url":                   job.get("jobUrl", ""),
            "date_posted":           (job.get("publishedAt", "") or "")[:10],
            "food_keywords_matched": ", ".join(matched),
            "keyword_count":         len(matched),
            "perk_excerpt":          excerpt(full_text, matched[0]),
            "remote":                "Remote" if job.get("isRemote") else "On-site",
        })
    return records


def scrape(slugs: list[str] | None = None) -> Iterator[dict]:
    """
    Crawl all Ashby tenants in parallel, yield food-perk job records.
    Discovers tenants via sitemap; falls back to hardcoded list.
    """
    if slugs is None:
        slugs = get_slugs("ashby", fallback=ASHBY_SLUGS)

    log.info(f"Ashby: crawling {len(slugs):,} tenants ({MAX_WORKERS} workers)")
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
                log.debug(f"Ashby worker error: {e}")

    log.info(f"Ashby: {hits} food-perk records from {len(slugs):,} tenants")
