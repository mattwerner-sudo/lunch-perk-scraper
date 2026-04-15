"""
Built In NYC scraper.

Built In NYC (https://www.builtinnyc.com/jobs) is a curated tech job board
that explicitly lists company perks — making it one of the highest-signal
sources for food perk detection. Companies fill out structured perk profiles.

We scrape two layers:
1. Company profiles for explicit perk badges ("Free Daily Lunches", etc.)
2. Job listings for keyword mentions in JDs
"""
import json
import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from utils import get, find_food_keywords, is_nyc, excerpt, clean_text

log = logging.getLogger(__name__)

# Built In NYC uses a GraphQL-like internal API.
BUILTIN_API = "https://api.builtin.com/jobs/search"
COMPANY_API  = "https://api.builtin.com/companies"

# Built In perk slugs that map to food benefits
FOOD_PERK_SLUGS = [
    "free-daily-lunches",
    "catered-meals",
    "company-provided-meals",
    "meal-stipend",
    "food-stipend",
    "snacks-and-coffee",
    "fully-stocked-kitchen",
]


def scrape() -> Iterator[dict]:
    """
    Strategy A: Search jobs in NYC, fetch each JD for food keywords.
    Strategy B: Pull companies with food perks, then surface their open roles.
    """
    yield from _scrape_jobs()
    yield from _scrape_companies_with_food_perks()


def _scrape_jobs() -> Iterator[dict]:
    """Page through Built In NYC job listings and check each JD."""
    page = 1
    per_page = 50

    while True:
        params = {
            "city": "New York City",
            "page": page,
            "per_page": per_page,
            "sort": "date",
        }
        resp = get(BUILTIN_API, params=params)
        if not resp:
            break

        try:
            data = resp.json()
        except json.JSONDecodeError:
            break

        jobs = data.get("jobs", data.get("data", []))
        if not jobs:
            break

        log.info(f"Built In NYC jobs: page {page}, {len(jobs)} results")

        for job in jobs:
            url = job.get("url", "") or job.get("applyUrl", "")
            title = job.get("title", "") or job.get("jobTitle", "")
            company = job.get("company", {})
            company_name = company.get("name", "") if isinstance(company, dict) else str(company)
            location = job.get("location", "") or "New York, NY"

            # Fetch full JD from the detail URL
            jd_text = _fetch_builtin_jd(url)
            if not jd_text:
                continue

            matched_keywords = find_food_keywords(jd_text)
            if not matched_keywords:
                continue

            snip = excerpt(jd_text, matched_keywords[0])

            yield {
                "source": "Built In NYC",
                "company": company_name,
                "title": title,
                "location": location,
                "url": url,
                "date_posted": job.get("datePosted", "")[:10] if job.get("datePosted") else "",
                "food_keywords_matched": ", ".join(matched_keywords),
                "keyword_count": len(matched_keywords),
                "perk_excerpt": snip,
                "remote": job.get("remote", ""),
            }

        total = data.get("total", 0)
        if page * per_page >= total:
            break
        page += 1


def _fetch_builtin_jd(url: str) -> str:
    """Fetch and parse a Built In job detail page."""
    if not url or not url.startswith("http"):
        return ""
    resp = get(url)
    if not resp:
        return ""
    soup = BeautifulSoup(resp.text, "lxml")
    # Built In wraps the JD in a div with data-id="job-description"
    jd_div = soup.find(attrs={"data-id": "job-description"}) or \
             soup.find("div", class_=re.compile(r"job.?description", re.I)) or \
             soup.find("section", class_=re.compile(r"description", re.I))
    if jd_div:
        return clean_text(jd_div.get_text(" ", strip=True))
    return clean_text(soup.get_text(" ", strip=True))[:5000]


def _scrape_companies_with_food_perks() -> Iterator[dict]:
    """
    Pull companies that have food perk badges on Built In NYC,
    then surface their open NYC roles.
    """
    for perk_slug in FOOD_PERK_SLUGS:
        params = {
            "city": "New York City",
            "perks": perk_slug,
            "per_page": 100,
        }
        resp = get(COMPANY_API, params=params)
        if not resp:
            continue
        try:
            data = resp.json()
        except json.JSONDecodeError:
            continue

        companies = data.get("companies", data.get("data", []))
        log.info(f"Built In NYC perk '{perk_slug}': {len(companies)} companies")

        for co in companies:
            co_name = co.get("name", "")
            jobs_url = co.get("jobsUrl", "") or co.get("url", "")

            # Each company record often has a jobs list embedded
            for job in co.get("jobs", []):
                title = job.get("title", "")
                location = job.get("location", "")
                url = job.get("url", "")

                yield {
                    "source": "Built In NYC (Company Perk)",
                    "company": co_name,
                    "title": title,
                    "location": location,
                    "url": url,
                    "date_posted": "",
                    "food_keywords_matched": perk_slug.replace("-", " "),
                    "keyword_count": 1,
                    "perk_excerpt": f"Company lists '{perk_slug.replace('-',' ')}' as a perk on Built In NYC",
                    "remote": job.get("remote", ""),
                }
