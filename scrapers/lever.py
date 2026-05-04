"""
Lever ATS scraper — full tenant crawl.

Discovers all active Lever companies via sitemap (~20,000 tenants),
then queries each one's public JSON API in parallel for food perk keywords.

API: https://api.lever.co/v0/postings/{slug}?mode=json
  - Public, no auth
  - Returns all open postings with full descriptionPlain + lists fields
  - No per-job detail fetch needed

Falls back to hardcoded LEVER_SLUGS if sitemap unavailable.
"""
import logging
import requests
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

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

LEVER_SLUGS = [
    "gettyimages", "gopuff", "arcadia",
    "ro", "hims", "cerebral",
    "vimeo", "canva", "miro", "asana",
    "seatgeek", "eventbrite",
    "webflow", "coda", "loom",
    "benchling", "ironclad", "procore",
    "brex", "mercury", "ramp",
    "nerdwallet", "stash", "acorns",
    "pipe", "capchase", "mosaic",
    "garner", "virta", "omada", "brightline",
    "newsela", "panorama",
    "draftkings", "fanatics",
    "compass", "opendoor",
    "lattice", "leapsome", "betterworks",
    "remote", "deel",
]


def _fetch_company(slug: str) -> list[dict]:
    """Fetch all food-perk jobs for one Lever company slug."""
    try:
        r = requests.get(
            f"https://api.lever.co/v0/postings/{slug}?mode=json",
            headers=HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200:
            return []
        jobs = r.json()
        if not isinstance(jobs, list):
            return []
    except Exception:
        return []

    records = []
    for job in jobs:
        cats       = job.get("categories") or {}
        location   = cats.get("location", "")
        commitment = cats.get("commitment", "")

        plain       = job.get("descriptionPlain", "") or ""
        list_text   = " ".join(lst.get("content", "") for lst in (job.get("lists") or []))
        additional  = job.get("additional", "") or ""
        full_text   = clean_text(f"{plain} {list_text} {additional}")

        if not is_in_target_location(f"{location} {full_text}"):
            continue

        matched = find_food_keywords(full_text)
        if not matched:
            continue

        company_name = job.get("company") or slug.replace("-", " ").title()

        records.append({
            "source":                "Lever",
            "company":               company_name,
            "title":                 job.get("text", ""),
            "location":              location,
            "url":                   job.get("hostedUrl", ""),
            "date_posted":           _ts_to_date(job.get("createdAt")),
            "food_keywords_matched": ", ".join(matched),
            "keyword_count":         len(matched),
            "perk_excerpt":          excerpt(full_text, matched[0]),
            "remote":                _infer_remote(location, commitment),
        })
    return records


def scrape(slugs: list[str] | None = None) -> Iterator[dict]:
    """
    Crawl all Lever tenants in parallel, yield food-perk job records.
    Discovers tenants via sitemap; falls back to hardcoded list.
    """
    if slugs is None:
        slugs = get_slugs("lever", fallback=LEVER_SLUGS)

    log.info(f"Lever: crawling {len(slugs):,} tenants ({MAX_WORKERS} workers)")
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
                log.debug(f"Lever worker error: {e}")

    log.info(f"Lever: {hits} food-perk records from {len(slugs):,} tenants")


def _ts_to_date(ts) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _infer_remote(location: str, commitment: str) -> str:
    combined = (location + " " + commitment).lower()
    if "remote" in combined:
        return "Remote"
    if "hybrid" in combined:
        return "Hybrid"
    return "On-site"
