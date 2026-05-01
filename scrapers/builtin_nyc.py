"""
Built In scraper (nationwide).

Built In (builtinnyc.com, builtin.com) curates tech job boards with
explicit company perk profiles — high-signal for food perks.

Their internal API endpoints changed and now return 405. We fall back to
scraping the public HTML search results directly.
"""
import json
import logging
import re
import time
from typing import Iterator

from bs4 import BeautifulSoup

from utils import get, find_food_keywords, is_in_target_location, excerpt, clean_text

log = logging.getLogger(__name__)

BUILTIN_SITES = [
    "https://www.builtinnyc.com",
    "https://www.builtinchicago.org",
    "https://www.builtinla.com",
    "https://www.builtinboston.com",
    "https://www.builtinaustin.com",
    "https://www.builtinseattle.com",
    "https://builtin.com",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}

FOOD_KEYWORDS_SEARCH = [
    "free lunch",
    "catered meals",
    "meal stipend",
    "DoorDash",
    "stocked kitchen",
]

# Built In perk badge slugs to check on company profile pages
FOOD_PERK_SLUGS = [
    "free-daily-lunches",
    "catered-meals",
    "meal-stipend",
    "snacks-and-coffee",
    "fully-stocked-kitchen",
]


def scrape() -> Iterator[dict]:
    yield from _scrape_builtin_search()


def _scrape_builtin_search() -> Iterator[dict]:
    """
    Search builtin.com for jobs mentioning food perks via their public search pages.
    Falls back gracefully if the site structure changes.
    """
    seen_urls: set[str] = set()

    for keyword in FOOD_KEYWORDS_SEARCH:
        encoded = keyword.replace(" ", "+")
        url = f"https://builtin.com/jobs/search?q={encoded}"
        log.info(f"Built In: searching '{keyword}'")

        resp = get(url, headers=HEADERS)
        if not resp:
            log.warning(f"Built In: no response for '{keyword}'")
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        # Try __NEXT_DATA__ JSON
        next_data = soup.find("script", id="__NEXT_DATA__")
        jobs_found = 0
        if next_data:
            try:
                data = json.loads(next_data.string)
                job_list = (
                    data.get("props", {})
                        .get("pageProps", {})
                        .get("jobs", [])
                    or data.get("props", {})
                        .get("pageProps", {})
                        .get("jobListings", [])
                )
                for job in job_list:
                    result = _parse_job_record(job, keyword, seen_urls)
                    if result:
                        jobs_found += 1
                        yield result
                log.info(f"Built In '{keyword}': {jobs_found} matches via __NEXT_DATA__")
                time.sleep(1.5)
                continue
            except Exception:
                pass

        # Fallback: parse job cards from HTML
        cards = soup.select("[data-id='job-card'], .job-card, [class*='JobCard'], article[class*='job']")
        for card in cards:
            title_el   = card.select_one("h2, h3, [class*='title']")
            company_el = card.select_one("[class*='company'], [class*='employer']")
            link_el    = card.select_one("a[href*='/jobs/']")
            if not link_el:
                continue
            href = link_el.get("href", "")
            job_url = href if href.startswith("http") else f"https://builtin.com{href}"
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            title   = title_el.get_text(strip=True) if title_el else ""
            company = company_el.get_text(strip=True) if company_el else ""
            location = ""
            loc_el = card.select_one("[class*='location'], [data-testid='location']")
            if loc_el:
                location = loc_el.get_text(strip=True)
            if not is_in_target_location(location):
                continue
            jobs_found += 1
            yield {
                "source":                "Built In",
                "company":               company,
                "title":                 title,
                "location":              location,
                "remote":                "",
                "food_keywords_matched": keyword,
                "keyword_count":         1,
                "perk_excerpt":          f"Found via Built In search for '{keyword}'",
                "date_posted":           "",
                "url":                   job_url,
            }

        log.info(f"Built In '{keyword}': {jobs_found} matches via HTML")
        time.sleep(1.5)


def _parse_job_record(job: dict, keyword: str, seen_urls: set) -> dict | None:
    url = job.get("url") or job.get("jobUrl") or job.get("applyUrl") or ""
    if not url or url in seen_urls:
        return None
    seen_urls.add(url)

    title    = job.get("title") or job.get("jobTitle") or ""
    company  = job.get("company") or ""
    if isinstance(company, dict):
        company = company.get("name") or ""
    location = job.get("location") or job.get("locationName") or ""
    desc_raw = job.get("description") or job.get("body") or ""
    full_text = clean_text(str(desc_raw))

    if not is_in_target_location(f"{location} {full_text}"):
        return None

    matched = find_food_keywords(full_text) or [keyword]
    snip = excerpt(full_text, matched[0]) if full_text else f"Found via Built In search for '{keyword}'"

    return {
        "source":                "Built In",
        "company":               str(company),
        "title":                 str(title),
        "location":              str(location),
        "remote":                "",
        "food_keywords_matched": ", ".join(matched),
        "keyword_count":         len(matched),
        "perk_excerpt":          snip,
        "date_posted":           (job.get("datePosted") or "")[:10],
        "url":                   url,
    }
