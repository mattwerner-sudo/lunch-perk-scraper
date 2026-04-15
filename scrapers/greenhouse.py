"""
Greenhouse ATS scraper.

Greenhouse has a public JSON API for every company using it:
  https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs?content=true

We search a curated list of company slugs + auto-discover via board listings.
The `content=true` param includes the full job description — no need to
fetch individual pages.
"""
import json
import logging
from typing import Iterator

from bs4 import BeautifulSoup

from utils import get, find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

# ── Greenhouse company slugs to check ────────────────────────────────────────
# These are companies known to use Greenhouse AND have NYC offices.
# Extend this list — slugs are the subdomain/path in their Greenhouse URL.
GREENHOUSE_SLUGS = [
    # ── Verified valid ────────────────────────────────────────────────────

    # Tech / SaaS
    "figma", "airtable", "stripe", "hubspot", "twilio", "amplitude",
    "mixpanel", "klaviyo", "intercom", "zendesk", "gong",
    "zoominfo", "demandbase", "g2", "mongodb", "datadog", "braze",
    "movableink", "yotpo", "sisense", "appsflyer", "taboola",
    "pinterest", "reddit", "lyft", "airbnb", "dropbox", "squarespace",
    "kickstarter", "duolingo", "pagerduty", "cloudflare", "fastly",
    "elastic", "databricks", "seatgeek", "tripadvisor", "opendoor",
    "mindbody", "cision", "similarweb",

    # Finance / Investing
    "point72", "apollo", "betterment", "robinhood", "plaid", "wealthfront",
    "virtu", "iex", "creditkarma", "cleo", "galileo",

    # Media / Content
    "buzzfeed", "voxmedia", "forbes", "axios", "semafor",

    # Pharma / Biotech / Life Sciences (ezcater ICP Tier 1)
    "icon", "natera", "beamtherapeutics",

    # Food & Delivery
    "doordash", "goldbelly", "misfitsmarket", "hungryroot",
    "touchbistro", "revel", "agilysys",

    # E-commerce / Logistics / Supply Chain
    "glossier", "allbirds", "brooklinen", "renttherunway", "rebag",
    "narvar", "aftership", "flexport", "shipmonk",

    # Real Estate / PropTech
    "costar", "vts", "crexi",

    # Health / Wellness
    "zocdoc", "ritual", "calm", "peloton", "classpass",
    "oscar", "cerebral",

    # Professional Services / Consulting
    "alixpartners",

    # Agency / Services
    "ogilvy", "wpp",

    # ── Kept for coverage ─────────────────────────────────────────────────
    "monday", "notion", "lattice", "greenhouse",
    "salesforce", "segment", "sendbird",
    "freshworks", "drift", "chorus",
    "outreach", "salesloft", "terminus", "rollworks", "bombora",
    "trustradius", "capterra",
    "etsy", "poshmark",
    "cityblock", "instacart", "sweetgreen",
]


def scrape(slugs: list[str] = GREENHOUSE_SLUGS) -> Iterator[dict]:
    """Yield job records from Greenhouse boards."""
    for slug in slugs:
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        resp = get(url)
        if not resp:
            continue

        try:
            data = resp.json()
        except json.JSONDecodeError:
            log.warning(f"Greenhouse: invalid JSON for slug={slug}")
            continue

        jobs = data.get("jobs", [])
        log.info(f"Greenhouse {slug}: {len(jobs)} jobs")

        for job in jobs:
            location = job.get("location", {}).get("name", "")
            content_html = job.get("content", "")
            content_text = clean_text(content_html)

            # Location check: must include NYC signal in location field OR
            # job description (some postings list "New York" only in body)
            combined = f"{location} {content_text}"
            if not is_nyc(combined):
                continue

            matched_keywords = find_food_keywords(content_text)
            if not matched_keywords:
                continue

            # Build excerpt from first matched keyword
            snip = excerpt(content_text, matched_keywords[0])

            yield {
                "source": "Greenhouse",
                "company": slug.replace("-", " ").title(),
                "title": job.get("title", ""),
                "location": location,
                "url": job.get("absolute_url", ""),
                "date_posted": job.get("updated_at", "")[:10],
                "food_keywords_matched": ", ".join(matched_keywords),
                "keyword_count": len(matched_keywords),
                "perk_excerpt": snip,
                "remote": _infer_remote(location, content_text),
            }


def _infer_remote(location: str, text: str) -> str:
    loc_lower = location.lower()
    text_lower = text.lower()
    if "remote" in loc_lower:
        return "Remote"
    if "hybrid" in loc_lower or "hybrid" in text_lower:
        return "Hybrid"
    return "On-site"
