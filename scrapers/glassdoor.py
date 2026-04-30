"""
Glassdoor Benefits scraper.

Glassdoor's company benefits pages are publicly indexed (no login required
for viewing benefit summaries). We use their public search to find NYC
companies that list food-related benefits, then surface their open roles.

Benefit categories relevant to food perks:
  - "Free Lunch or Snacks" (category slug: free-lunch-or-snacks)
  - "Company Cafeteria" (cafeteria)
  - "Meal Plan or Allowance" (meal-plan-or-allowance)

Approach:
  1. Search Glassdoor for NYC employers with food benefit categories
  2. Collect the company name + Glassdoor employer ID
  3. Fetch open job listings for each matching company via their jobs API
  4. Filter to NYC roles
"""
import json
import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from utils import get, find_food_keywords, is_in_target_location, excerpt, clean_text

log = logging.getLogger(__name__)

# Glassdoor benefit category IDs for food perks (from their public filter UI)
FOOD_BENEFIT_CATEGORIES = [
    ("Free Lunch or Snacks",  "free-lunch-or-snacks"),
    ("Meal Plan or Allowance", "meal-plan-or-allowance"),
    ("Company Cafeteria",      "cafeteria"),
    ("Stocked Kitchen",        "stocked-kitchen"),
]

GD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.glassdoor.com/",
}


def scrape() -> Iterator[dict]:
    """Main entry point — yields job records for NYC companies with food perks."""
    companies = _find_companies_with_food_perks()
    log.info(f"Glassdoor: found {len(companies)} companies with food perks in NYC")

    for co in companies:
        yield from _get_company_jobs(co)


def _find_companies_with_food_perks() -> list[dict]:
    """
    Search Glassdoor for NYC employers listing food benefits.
    Returns list of dicts: {name, employer_id, benefit_label, gd_url}
    """
    companies = {}  # employer_id → record (deduplicate across benefit categories)

    for benefit_label, benefit_slug in FOOD_BENEFIT_CATEGORIES:
        # Glassdoor benefits search — filtered to New York City
        # The locationId for NYC metro on Glassdoor is 1132348
        url = (
            "https://www.glassdoor.com/Benefits/new-york-city-"
            f"{benefit_slug}-SRCH_IL.0,13_IM615_KO14,"
            f"{14 + len(benefit_slug)}.htm"
        )
        resp = get(url, headers=GD_HEADERS)
        if not resp:
            log.warning(f"Glassdoor: no response for benefit={benefit_slug}")
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        # Glassdoor renders company benefit listings in a React-hydrated page;
        # the initial data is embedded as JSON in a <script id="__NEXT_DATA__"> tag
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data:
            try:
                data = json.loads(next_data.string)
                employers = (
                    data.get("props", {})
                        .get("pageProps", {})
                        .get("employers", [])
                    or data.get("props", {})
                        .get("pageProps", {})
                        .get("benefitsData", {})
                        .get("employers", [])
                )
                for emp in employers:
                    eid = str(emp.get("employerId") or emp.get("id") or "")
                    if not eid or eid in companies:
                        continue
                    companies[eid] = {
                        "employer_id": eid,
                        "name": emp.get("employerName") or emp.get("name", ""),
                        "benefit_label": benefit_label,
                        "gd_url": emp.get("detailUrl", ""),
                    }
                log.info(f"Glassdoor {benefit_slug}: {len(employers)} employers (via __NEXT_DATA__)")
                continue
            except (json.JSONDecodeError, KeyError):
                pass

        # Fallback: parse the HTML directly
        employer_links = soup.select("a[href*='/Benefits/']")
        for link in employer_links:
            href = link.get("href", "")
            # Extract E-number from URL like /Benefits/Google-Benefits-E9079.htm
            m = re.search(r"-E(\d+)\.htm", href)
            if not m:
                continue
            eid = m.group(1)
            if eid in companies:
                continue
            name = link.get_text(strip=True)
            companies[eid] = {
                "employer_id": eid,
                "name": name,
                "benefit_label": benefit_label,
                "gd_url": f"https://www.glassdoor.com{href}",
            }

        log.info(f"Glassdoor {benefit_slug}: parsed {len(employer_links)} employer links")

    return list(companies.values())


def _get_company_jobs(co: dict) -> Iterator[dict]:
    """Fetch open NYC jobs for a company identified via Glassdoor benefits."""
    eid = co["employer_id"]
    co_name = co["name"]

    # Glassdoor's job listing API — public endpoint, no auth required
    # Returns JSON for the employer's current open roles
    api_url = (
        f"https://www.glassdoor.com/Jobs/{co_name.replace(' ', '-')}"
        f"-Jobs-E{eid}.htm"
    )
    params = {
        "sc.occupationParam": "marketing",
        "locT": "C",
        "locId": "1132348",  # New York City metro
    }
    resp = get(api_url, headers=GD_HEADERS, params=params)
    if not resp:
        return

    soup = BeautifulSoup(resp.text, "lxml")

    # Try __NEXT_DATA__ first
    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data:
        try:
            data = json.loads(next_data.string)
            jobs_list = (
                data.get("props", {})
                    .get("pageProps", {})
                    .get("jobListings", {})
                    .get("jobListings", [])
            )
            for job in jobs_list:
                location = job.get("location", "")
                if not is_in_target_location(location):
                    continue
                title = job.get("jobTitle", "")
                job_url = "https://www.glassdoor.com" + job.get("jobLink", "")

                yield {
                    "source": "Glassdoor Benefits",
                    "company": co_name,
                    "title": title,
                    "location": location,
                    "url": job_url,
                    "date_posted": job.get("listingAge", ""),
                    "food_keywords_matched": co["benefit_label"],
                    "keyword_count": 1,
                    "perk_excerpt": (
                        f"Company lists '{co['benefit_label']}' as a verified "
                        f"benefit on Glassdoor (employee-reported)"
                    ),
                    "remote": _infer_remote(location),
                }
            return
        except (json.JSONDecodeError, KeyError):
            pass

    # Fallback: yield just the company + benefit (no individual job links)
    # This still gives GTM team a high-signal company to target
    if co_name:
        yield {
            "source": "Glassdoor Benefits",
            "company": co_name,
            "title": "(see all open roles)",
            "location": "New York, NY",
            "url": api_url,
            "date_posted": "",
            "food_keywords_matched": co["benefit_label"],
            "keyword_count": 1,
            "perk_excerpt": (
                f"Company lists '{co['benefit_label']}' as a verified "
                f"benefit on Glassdoor (employee-reported)"
            ),
            "remote": "",
        }


def _infer_remote(location: str) -> str:
    loc = location.lower()
    if "remote" in loc:
        return "Remote"
    if "hybrid" in loc:
        return "Hybrid"
    return "On-site"
