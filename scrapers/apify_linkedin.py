"""
Apify LinkedIn Jobs Scraper.

Uses the curious_coder LinkedIn Jobs Scraper actor (hKByXkMQaC5Qt9UMN)
via Apify's cloud infrastructure — rotating proxies, no IP rate limits.

Cost: ~$0.001 per result. A full weekly run costs under $2.

Requires APIFY_API_TOKEN in .env
"""
import os
import time
import json
import logging
from typing import Iterator

import requests
from dotenv import load_dotenv

from utils import find_food_keywords, is_nyc, excerpt, clean_text
from config import FOOD_KEYWORDS

load_dotenv()
log = logging.getLogger(__name__)

ACTOR_ID       = "hKByXkMQaC5Qt9UMN"
API_TOKEN      = os.getenv("APIFY_API_TOKEN", "")
BASE_URL       = "https://api.apify.com/v2"
POLL_INTERVAL  = 10   # seconds between status checks
MAX_WAIT       = 600  # 10 minutes max per run
RESULTS_LIMIT  = 100  # results per keyword search

# Search directly for food perk keywords on LinkedIn in NYC
LINKEDIN_SEARCHES = [
    {"keyword": "free lunch",    "location": "New York City, New York"},
    {"keyword": "catered lunch", "location": "New York City, New York"},
    {"keyword": "catered meals", "location": "New York City, New York"},
    {"keyword": "DoorDash",      "location": "New York City, New York"},
    {"keyword": "GrubHub",       "location": "New York City, New York"},
    {"keyword": "Uber Eats",     "location": "New York City, New York"},
    {"keyword": "Forkable",      "location": "New York City, New York"},
    {"keyword": "Sharebite",     "location": "New York City, New York"},
    {"keyword": "meal stipend",  "location": "New York City, New York"},
    {"keyword": "lunch stipend", "location": "New York City, New York"},
    {"keyword": "stocked kitchen","location": "New York City, New York"},
    {"keyword": "meal credit",   "location": "New York City, New York"},
]


def _start_run(search: dict) -> tuple[str, str, str]:
    """Start one Apify run. Returns (run_id, dataset_id, keyword)."""
    keyword = search["keyword"]
    if not API_TOKEN:
        return "", "", keyword

    encoded_keyword = keyword.replace(" ", "+")
    linkedin_url = (
        f"https://www.linkedin.com/jobs/search/"
        f"?keywords={encoded_keyword}"
        f"&location=New+York+City%2C+New+York"
        f"&position=1&pageNum=0"
    )
    input_payload = {
        "count":           RESULTS_LIMIT,
        "scrapeCompany":   True,
        "splitByLocation": False,
        "urls":            [linkedin_url],
    }
    run_url = f"{BASE_URL}/acts/{ACTOR_ID}/runs?token={API_TOKEN}"
    try:
        resp = requests.post(run_url, json=input_payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()["data"]
        log.info(f"Apify: started run for '{keyword}' → {data['id']}")
        return data["id"], data["defaultDatasetId"], keyword
    except Exception as e:
        log.error(f"Apify: failed to start run for '{keyword}': {e}")
        return "", "", keyword



def _wait_and_fetch(run_id: str, dataset_id: str, keyword: str) -> list[dict]:
    """Poll until run completes then fetch results."""
    if not run_id:
        return []

    status_url = f"{BASE_URL}/actor-runs/{run_id}?token={API_TOKEN}"
    waited = 0
    while waited < MAX_WAIT:
        time.sleep(POLL_INTERVAL)
        waited += POLL_INTERVAL
        try:
            status = requests.get(status_url, timeout=15).json()["data"]["status"]
            if status == "SUCCEEDED":
                break
            if status in {"FAILED", "ABORTED", "TIMED-OUT"}:
                log.error(f"Apify run {run_id} ended: {status}")
                return []
        except Exception as e:
            log.warning(f"Apify status check failed: {e}")

    dataset_url = (
        f"{BASE_URL}/datasets/{dataset_id}/items"
        f"?token={API_TOKEN}&format=json&clean=true"
    )
    try:
        items = requests.get(dataset_url, timeout=30).json()
        log.info(f"Apify: {len(items)} results for '{keyword}'")
        return items
    except Exception as e:
        log.error(f"Apify: failed to fetch dataset: {e}")
        return []


def scrape() -> Iterator[dict]:
    """
    Fire ALL keyword searches simultaneously, then collect results in parallel.
    Cuts total Apify time from (searches × wait) to (1 × wait).
    """
    if not API_TOKEN:
        log.error("Apify scraper skipped — APIFY_API_TOKEN not set in .env")
        return

    import concurrent.futures

    # Start all runs at once
    log.info(f"Apify: launching {len(LINKEDIN_SEARCHES)} parallel runs...")
    runs = []
    for search in LINKEDIN_SEARCHES:
        run_id, dataset_id, keyword = _start_run(search)
        if run_id:
            runs.append((run_id, dataset_id, keyword))
        time.sleep(0.5)

    log.info(f"Apify: {len(runs)} runs started — waiting for completion...")


    if not runs:                                          
        log.warning("Apify: no runs succeeded, skipping fetch")
        return                                            

    # Collect all results in parallel
    seen_urls = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(runs)) as pool:
        futures = {
            pool.submit(_wait_and_fetch, run_id, dataset_id, keyword): keyword
            for run_id, dataset_id, keyword in runs
        }
        for future in concurrent.futures.as_completed(futures):
            keyword = futures[future]
            try:
                items = future.result()
            except Exception as e:
                log.error(f"Apify result fetch failed for '{keyword}': {e}")
                continue

            for item in items:
                url = str(item.get("jobUrl") or item.get("url") or "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                company  = str(item.get("companyName") or item.get("company") or "")
                title    = str(item.get("title") or item.get("jobTitle") or "")
                location = str(item.get("location") or "")
                desc_raw = str(item.get("description") or item.get("jobDescription") or "")
                full_text = clean_text(desc_raw)

                if not is_nyc(f"{location} {full_text}"):
                    continue

                matched_keywords = find_food_keywords(full_text)
                if not matched_keywords:
                    if keyword.lower() not in full_text.lower():
                        continue
                    matched_keywords = [keyword]

                snip = excerpt(full_text, matched_keywords[0]) if full_text else ""
                date_posted = str(item.get("postedAt") or item.get("datePosted") or "")[:10]

                yield {
                    "source":                "Apify/LinkedIn",
                    "company":               company,
                    "title":                 title,
                    "location":              location or "New York, NY",
                    "remote":                "Remote" if item.get("workplaceType") == "Remote" else "On-site",
                    "food_keywords_matched": ", ".join(matched_keywords),
                    "keyword_count":         len(matched_keywords),
                    "perk_excerpt":          snip,
                    "date_posted":           date_posted,
                    "url":                   url,
                }
