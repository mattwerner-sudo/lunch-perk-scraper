"""
Workday ATS scraper.

Workday powers enterprise job boards (JPMorgan, Salesforce, Goldman, etc).
Each company has a unique tenant URL pattern:
  https://{company}.wd{N}.myworkdayjobs.com/en-US/{board}/jobs

We use their JSON search API with empty searchText to get all jobs, then
filter locally for food perk keywords.

NOTE: Workday requires exact tenant slug + wd_number + board_name per company.
      Wrong board_name → 422. Bot detection → 401/406. Coverage is best-effort.
"""
import logging
import random
import time
from typing import Iterator

import requests

from utils import find_food_keywords, excerpt, clean_text

log = logging.getLogger(__name__)

# (company_name, tenant_slug, wd_number, board_name)
# Board names verified working — 422 means the board name has changed
WORKDAY_TENANTS = [
    ("Zendesk",         "zendesk",      1, "Zendesk"),
    ("Peloton",         "pel",          5, "Peloton"),
    ("WeWork",          "wework",       5, "WeWork_External"),
    ("BuzzFeed",        "buzzfeed",     1, "External"),
    ("Condé Nast",      "condenast",    5, "CNExternalCareerSite"),
]

SEARCH_LIMIT = 20   # jobs per page


def scrape(tenants=None) -> Iterator[dict]:
    if tenants is None:
        tenants = WORKDAY_TENANTS

    for company_name, tenant, wd_num, board in tenants:
        base_url = (
            f"https://{tenant}.wd{wd_num}.myworkdayjobs.com"
            f"/wday/cxs/{tenant}/{board}/jobs"
        )
        log.info(f"Workday: scanning {company_name}")
        offset = 0
        total  = None

        while True:
            try:
                time.sleep(1.2 + random.uniform(0, 0.5))
                resp = requests.post(
                    base_url,
                    json={"appliedFacets": {}, "limit": SEARCH_LIMIT, "offset": offset, "searchText": ""},
                    headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                    timeout=15,
                )
                if resp.status_code != 200:
                    log.warning(f"Workday {company_name}: HTTP {resp.status_code}")
                    break
                data = resp.json()
            except Exception as e:
                log.warning(f"Workday {company_name}: {e}")
                break

            job_postings = data.get("jobPostings", [])
            if total is None:
                total = data.get("total", 0)

            if not job_postings:
                break

            for posting in job_postings:
                external_path = posting.get("externalPath", "")
                title         = posting.get("title", "")
                location      = posting.get("locationsText", "")

                # Fetch full JD for keyword check
                detail_url = (
                    f"https://{tenant}.wd{wd_num}.myworkdayjobs.com"
                    f"/wday/cxs/{tenant}/{board}{external_path}"
                )
                try:
                    time.sleep(1.0)
                    detail = requests.get(detail_url, timeout=15,
                                          headers={"User-Agent": "Mozilla/5.0"})
                    jd_html   = detail.json().get("jobPostingInfo", {}).get("jobDescription", "")
                    full_text = clean_text(jd_html)
                except Exception:
                    full_text = ""

                matched = find_food_keywords(full_text)
                if not matched:
                    continue

                job_url = (
                    f"https://{tenant}.wd{wd_num}.myworkdayjobs.com"
                    f"/en-US/{board}{external_path}"
                )
                yield {
                    "source":                "Workday",
                    "company":               company_name,
                    "title":                 title,
                    "location":              location,
                    "remote":                _infer_remote(location),
                    "food_keywords_matched": ", ".join(matched),
                    "keyword_count":         len(matched),
                    "perk_excerpt":          excerpt(full_text, matched[0]),
                    "date_posted":           "",
                    "url":                   job_url,
                }

            offset += SEARCH_LIMIT
            if offset >= (total or 0):
                break


def _infer_remote(location: str) -> str:
    loc_lower = location.lower()
    if "remote" in loc_lower:
        return "Remote"
    if "hybrid" in loc_lower:
        return "Hybrid"
    return "On-site"
