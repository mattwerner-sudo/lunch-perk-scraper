"""
Lever ATS scraper.

Lever also has a public JSON API:
  https://api.lever.co/v0/postings/{company_slug}?mode=json

The full job description is in the `descriptionPlain` and `lists` fields.
"""
import json
import logging
from typing import Iterator

from utils import get, find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

LEVER_SLUGS = [
    # ── Verified valid 2024 ───────────────────────────────────────────────
    "gettyimages",   # large NYC media company, known food perks
    "gopuff",        # food delivery, NYC office

    # ── Kept for coverage (low hit rate but worth checking) ───────────────
    "ro", "hims", "cerebral",
    "vimeo", "squarespace",
    "canva", "miro", "asana",
    "seatgeek", "eventbrite",
]


def scrape(slugs: list[str] = LEVER_SLUGS) -> Iterator[dict]:
    for slug in slugs:
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        resp = get(url)
        if not resp:
            continue

        try:
            jobs = resp.json()
        except json.JSONDecodeError:
            log.warning(f"Lever: invalid JSON for slug={slug}")
            continue

        if not isinstance(jobs, list):
            continue

        log.info(f"Lever {slug}: {len(jobs)} jobs")

        for job in jobs:
            location = job.get("categories", {}).get("location", "")
            commitment = job.get("categories", {}).get("commitment", "")
            team = job.get("categories", {}).get("team", "")

            # Build full text from all description sections
            plain = job.get("descriptionPlain", "") or ""
            lists = job.get("lists", [])
            list_text = " ".join(
                item.get("content", "") for lst in lists for item in [lst]
            )
            additional = job.get("additional", "") or ""
            full_text = clean_text(f"{plain} {list_text} {additional}")

            combined = f"{location} {full_text}"
            if not is_nyc(combined):
                continue

            matched_keywords = find_food_keywords(full_text)
            if not matched_keywords:
                continue

            snip = excerpt(full_text, matched_keywords[0])

            yield {
                "source": "Lever",
                "company": job.get("company", slug.replace("-", " ").title()),
                "title": job.get("text", ""),
                "location": location,
                "url": job.get("hostedUrl", ""),
                "date_posted": _ts_to_date(job.get("createdAt")),
                "food_keywords_matched": ", ".join(matched_keywords),
                "keyword_count": len(matched_keywords),
                "perk_excerpt": snip,
                "remote": _infer_remote(location, commitment),
            }


def _ts_to_date(ts) -> str:
    if not ts:
        return ""
    try:
        from datetime import datetime, timezone
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
