"""
JobSpy scraper — LinkedIn, Indeed, ZipRecruiter, Glassdoor, Google Jobs.

Single library replaces 5 separate scrapers. Returns full job descriptions
so we can scan for food perk keywords exactly like the ATS scrapers.
"""
import logging
import time
from typing import Iterator

from jobspy import scrape_jobs

from utils import find_food_keywords, is_in_target_location, excerpt, clean_text
from config import SEARCH_QUERIES, DELAY_BETWEEN_REQUESTS

log = logging.getLogger(__name__)

# LinkedIn handled by Apify (better volume + accuracy)
# ZipRecruiter blocked (CF WAF 403)
# Glassdoor handled by our own scraper
JOBSPY_SOURCES = ["indeed", "google"]

# How many results to request per search term per site
RESULTS_PER_QUERY = 100

# Hours old — only pull recent listings (7 days)
HOURS_OLD = 336


def scrape() -> Iterator[dict]:
    """
    Run each search query across all JobSpy sources.
    Yields job records that match NYC + food perk keywords.
    """
    seen_urls = set()  # deduplicate across queries

    for query in SEARCH_QUERIES:
        log.info(f"JobSpy: searching '{query}' in New York City")

        try:
            df = scrape_jobs(
                site_name=JOBSPY_SOURCES,
                search_term=query,
                location="New York City, NY",
                results_wanted=RESULTS_PER_QUERY,
                hours_old=HOURS_OLD,
                linkedin_fetch_description=True,
                description_format="markdown",
                verbose=0,
            )
        except Exception as e:
            log.warning(f"JobSpy query '{query}' failed: {e}")
            time.sleep(3)
            continue

        if df is None or df.empty:
            log.info(f"JobSpy: no results for '{query}'")
            continue

        log.info(f"JobSpy: {len(df)} raw results for '{query}'")

        for _, row in df.iterrows():
            url = str(row.get("job_url", "") or "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            # Build location string
            location_parts = [
                str(row.get("city", "") or ""),
                str(row.get("state", "") or ""),
            ]
            location = ", ".join(p for p in location_parts if p and p != "nan")

            # JobSpy already filters by location but double-check
            company = str(row.get("company_name", "") or "")
            title = str(row.get("title", "") or "")
            description = str(row.get("description", "") or "")
            full_text = clean_text(description)

            # NYC check on location + description
            if not is_in_target_location(f"{location} {full_text} {title}"):
                continue

            matched_keywords = find_food_keywords(full_text)
            if not matched_keywords:
                continue

            snip = excerpt(full_text, matched_keywords[0])
            source_site = str(row.get("site", "") or "JobSpy").title()

            # Comp
            min_amt = row.get("min_amount")
            max_amt = row.get("max_amount")
            interval = str(row.get("interval", "") or "")
            comp = ""
            if min_amt and max_amt and str(min_amt) != "nan" and str(max_amt) != "nan":
                if "year" in interval.lower():
                    comp = f"${int(min_amt/1000)}k–${int(max_amt/1000)}k"
                else:
                    comp = f"${min_amt}–${max_amt} {interval}"

            # Remote
            is_remote = row.get("is_remote")
            remote = "Remote" if is_remote else "On-site"

            date_posted = ""
            dp = row.get("date_posted")
            if dp and str(dp) != "nan" and str(dp) != "None":
                date_posted = str(dp)[:10]

            yield {
                "source": f"JobSpy/{source_site}",
                "company": company,
                "title": title,
                "location": location or "New York, NY",
                "remote": remote,
                "food_keywords_matched": ", ".join(matched_keywords),
                "keyword_count": len(matched_keywords),
                "perk_excerpt": snip,
                "date_posted": date_posted,
                "url": url,
            }

        # Be polite between queries
        time.sleep(DELAY_BETWEEN_REQUESTS * 2)
