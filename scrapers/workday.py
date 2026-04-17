"""
Workday ATS scraper.

Workday powers enterprise job boards (JPMorgan, Salesforce, Goldman, etc).
Each company has a unique tenant URL pattern:
  https://{company}.wd{N}.myworkdayjobs.com/en-US/{board}/jobs

We use their search API (JSON endpoint) to search for NYC jobs, then
fetch each JD for perk keywords.

NOTE: Workday is the trickiest to scrape — tenant IDs vary. We handle the
most common tenant patterns and surface them with best-effort.
"""
import json
import logging
from typing import Iterator

import requests

from utils import get, find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

# (company_name, tenant, wd_number, board_name)
WORKDAY_TENANTS = [
    ("JPMorgan Chase",      "jpmc",         5, "JPMorgan_Chase_External_Career_Site"),
    ("Goldman Sachs",       "goldmansachs", 1, "External_Career_Site"),
    ("Salesforce",          "salesforce",   2, "External_Career_Site"),
    ("Verizon",             "verizon",      1, "External_Career_Site"),
    ("Mastercard",          "mastercard",   5, "External_Career_Site"),
    ("American Express",    "aexp",         1, "AmexExternalSite"),
    ("Bloomberg",           "bloomberg",    5, "External_Career_Site"),
    ("WeWork",              "wework",       5, "WeWork_External"),
    ("Spotify",             "spotify",      1, "External"),
    ("Peloton",             "pel",          5, "Peloton"),
    ("BuzzFeed",            "buzzfeed",     1, "External"),
    ("NBCUniversal",        "nbcuni",       1, "External_Career_Site"),
    ("Condé Nast",          "condenast",    5, "CNExternalCareerSite"),
]

SEARCH_OFFSET_STEP = 20


def scrape(tenants=WORKDAY_TENANTS) -> Iterator[dict]:
    for company_name, tenant, wd_num, board in tenants:
        base_url = (
            f"https://{tenant}.wd{wd_num}.myworkdayjobs.com"
            f"/wday/cxs/{tenant}/{board}/jobs"
        )
        log.info(f"Workday: scanning {company_name} at {base_url}")
        offset = 0

        while True:
            payload = {
                "appliedFacets": {},
                "limit": SEARCH_OFFSET_STEP,
                "offset": offset,
                "searchText": "marketing",
            }
            try:
                import time, random
                time.sleep(1.2 + random.uniform(0, 0.5))
                resp = requests.post(
                    base_url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0",
                    },
                    timeout=15,
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
            except Exception as e:
                log.warning(f"Workday {company_name}: {e}")
                break

            job_postings = data.get("jobPostings", [])
            if not job_postings:
                break

            for posting in job_postings:
                external_path = posting.get("externalPath", "")
                title = posting.get("title", "")
                location = posting.get("locationsText", "")

                if not is_nyc(f"{location} {title}"):
                    continue

                # Fetch full JD
                detail_url = (
                    f"https://{tenant}.wd{wd_num}.myworkdayjobs.com"
                    f"/wday/cxs/{tenant}/{board}{external_path}"
                )
                try:
                    time.sleep(1.2)
                    detail_resp = requests.get(detail_url, timeout=15,
                        headers={"User-Agent": "Mozilla/5.0"})
                    detail_data = detail_resp.json()
                    jd_html = detail_data.get("jobPostingInfo", {}).get("jobDescription", "")
                    full_text = clean_text(jd_html)
                except Exception:
                    full_text = ""

                matched_keywords = find_food_keywords(full_text)
                if not matched_keywords:
                    continue

                job_url = (
                    f"https://{tenant}.wd{wd_num}.myworkdayjobs.com"
                    f"/en-US/{board}{external_path}"
                )
                snip = excerpt(full_text, matched_keywords[0])

                yield {
                    "source": "Workday",
                    "company": company_name,
                    "title": title,
                    "location": location,
                    "url": job_url,
                    "date_posted": "",
                    "food_keywords_matched": ", ".join(matched_keywords),
                    "keyword_count": len(matched_keywords),
                    "perk_excerpt": snip,
                    "remote": _infer_remote(location),
                }

            total = data.get("total", 0)
            offset += SEARCH_OFFSET_STEP
            if offset >= total:
                break


def _infer_remote(location: str) -> str:
    loc_lower = location.lower()
    if "remote" in loc_lower:
        return "Remote"
    if "hybrid" in loc_lower:
        return "Hybrid"
    return "On-site"
