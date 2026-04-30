"""
Ashby ATS scraper.

Ashby is the fastest-growing ATS in SaaS (Figma, Linear, Loom, etc).
Their public job board API:
  https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true
"""
import json
import logging
from typing import Iterator

from utils import get, find_food_keywords, is_in_target_location, excerpt, clean_text

log = logging.getLogger(__name__)

ASHBY_SLUGS = [
    # ── Product / Dev tools ───────────────────────────────────────────────
    "linear", "loom", "runway", "causal",
    "hightouch", "replit", "harvey",
    "notion", "retool",
    "openai", "anthropic", "cohere",

    # ── Fintech / Payments ────────────────────────────────────────────────
    "mercury", "ramp", "airwallex", "capchase",
    "clearco", "parafin", "slope",
    "brex",

    # ── Security / Compliance ─────────────────────────────────────────────
    "vanta", "drata",

    # ── HR / People ops ───────────────────────────────────────────────────
    "leapsome", "deel", "oyster",
    "lattice",

    # ── NYC-heavy companies ───────────────────────────────────────────────
    "flatiron-health",
    "cityblock",
    "benchling",
    "ro",
    "chainalysis",
    "fireblocks",
    "movable-ink",
    "sprinklr",
    "diligent",
    "tempus",

    # ── Sales enablement ─────────────────────────────────────────────────
    "spekit",
]


def scrape(slugs: list[str] = ASHBY_SLUGS) -> Iterator[dict]:
    for slug in slugs:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        resp = get(url)
        if not resp:
            continue

        try:
            data = resp.json()
        except json.JSONDecodeError:
            log.warning(f"Ashby: invalid JSON for slug={slug}")
            continue

        # API returns "jobs" (not "jobPostings") and includes descriptionHtml inline
        jobs = data.get("jobs", data.get("jobPostings", []))
        log.info(f"Ashby {slug}: {len(jobs)} jobs")

        for job in jobs:
            location = job.get("location", "")

            # Description is included in the list response — no detail call needed
            desc_html = job.get("descriptionHtml", "") or job.get("descriptionPlain", "") or ""
            sections = job.get("descriptionSections", []) or []
            section_text = " ".join(s.get("content", "") for s in sections)
            full_text = clean_text(f"{desc_html} {section_text}")

            combined = f"{location} {full_text}"
            if not is_in_target_location(combined):
                continue

            matched_keywords = find_food_keywords(full_text)
            if not matched_keywords:
                continue

            snip = excerpt(full_text, matched_keywords[0])
            company_name = job.get("companyName", slug.replace("-", " ").title())

            yield {
                "source": "Ashby",
                "company": company_name,
                "title": job.get("title", ""),
                "location": location,
                "url": job.get("jobUrl", ""),
                "date_posted": (job.get("publishedAt", "") or "")[:10],
                "food_keywords_matched": ", ".join(matched_keywords),
                "keyword_count": len(matched_keywords),
                "perk_excerpt": snip,
                "remote": "Remote" if job.get("isRemote") else "On-site",
            }
