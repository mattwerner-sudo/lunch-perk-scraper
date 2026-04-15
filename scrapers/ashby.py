"""
Ashby ATS scraper.

Ashby is the fastest-growing ATS in SaaS (Figma, Linear, Loom, etc).
Their public job board API:
  https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true
"""
import json
import logging
from typing import Iterator

from utils import get, find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

ASHBY_SLUGS = [
    # ── Verified valid 2024 ───────────────────────────────────────────────

    # Product / Dev tools
    "linear", "loom", "runway", "causal",
    "hightouch", "replit", "harvey",

    # Fintech / Payments
    "mercury", "ramp", "airwallex", "capchase",
    "clearco", "parafin", "re-cap", "slope",
    "tripactions",

    # Security / Compliance
    "vanta", "drata",

    # HR / People ops
    "leapsome", "deel", "oyster",

    # Sales enablement
    "spekit",

    # ── Kept for coverage ─────────────────────────────────────────────────
    "figma", "notion", "brex", "retool",
    "openai", "anthropic", "cohere",
    "rippling", "lattice",
]


def scrape(slugs: list[str] = ASHBY_SLUGS) -> Iterator[dict]:
    for slug in slugs:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
        resp = get(url)
        if not resp:
            continue

        try:
            data = resp.json()
        except json.JSONDecodeError:
            log.warning(f"Ashby: invalid JSON for slug={slug}")
            continue

        jobs = data.get("jobPostings", [])
        log.info(f"Ashby {slug}: {len(jobs)} jobs")

        for job in jobs:
            location = job.get("location", "")
            # Ashby doesn't include full JD in list endpoint — fetch detail
            job_id = job.get("id", "")
            detail_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}/posting/{job_id}"
            detail_resp = get(detail_url)
            if not detail_resp:
                continue

            try:
                detail = detail_resp.json()
            except json.JSONDecodeError:
                continue

            # Description is in `descriptionHtml` or `descriptionSections`
            desc_html = detail.get("descriptionHtml", "") or ""
            sections = detail.get("descriptionSections", []) or []
            section_text = " ".join(s.get("content", "") for s in sections)
            full_text = clean_text(f"{desc_html} {section_text}")

            combined = f"{location} {full_text}"
            if not is_nyc(combined):
                continue

            matched_keywords = find_food_keywords(full_text)
            if not matched_keywords:
                continue

            snip = excerpt(full_text, matched_keywords[0])
            comp = job.get("compensation", {}) or {}

            yield {
                "source": "Ashby",
                "company": detail.get("companyName", slug.replace("-", " ").title()),
                "title": job.get("title", ""),
                "location": location,
                "url": job.get("jobUrl", ""),
                "date_posted": job.get("publishedAt", "")[:10],
                "food_keywords_matched": ", ".join(matched_keywords),
                "keyword_count": len(matched_keywords),
                "perk_excerpt": snip,
                "remote": "Remote" if job.get("isRemote") else "On-site",
            }
