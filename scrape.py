"""
Main scraper runner.

Usage:
  python3 scrape.py                     # full run: scrape → verify → enrich → notify
  python3 scrape.py --sources gh lv    # specific sources only
  python3 scrape.py --no-verify        # skip live verification
  python3 scrape.py --no-notify        # skip Slack notification
  python3 scrape.py --dry-run          # preview only, no writes

Sources: gh gd lv ab bn wd js ap wf ex fc
"""
import argparse
import csv
import importlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import OUTPUT_CSV
from utils import log
from verify_live import verify_jobs
import db
import enrich as enricher
from notify_slack import send_new_companies_alert

SOURCES = {
    "gh": ("Greenhouse",              "scrapers.greenhouse",        "scrape"),
    "lv": ("Lever",                   "scrapers.lever",             "scrape"),
    "ab": ("Ashby",                   "scrapers.ashby",             "scrape"),
    "bn": ("Built In NYC",            "scrapers.builtin_nyc",       "scrape"),
    "wd": ("Workday",                 "scrapers.workday",           "scrape"),
    "gd": ("Glassdoor Benefits",      "scrapers.glassdoor",         "scrape"),
    "js": ("JobSpy (Indeed/Google)",  "scrapers.jobspy_scraper",    "scrape"),
    "ap": ("Apify LinkedIn",          "scrapers.apify_linkedin",    "scrape"),
    "wf": ("Wellfound",               "scrapers.wellfound",         "scrape"),
    "ex": ("Exa",                     "scrapers.exa_scraper",       "scrape"),
    "fc": ("Firecrawl",               "scrapers.firecrawl_scraper", "scrape"),
}

FIELDNAMES = [
    "source", "company", "title", "location", "remote",
    "food_keywords_matched", "keyword_count", "perk_excerpt",
    "date_posted", "url",
]


def _run_one(key: str, dry_run: bool) -> tuple[str, list[dict]]:
    """Run a single scraper and return (label, records). Never raises."""
    label, module_path, fn_name = SOURCES[key]
    try:
        mod = importlib.import_module(module_path)
        fn = getattr(mod, fn_name)
    except Exception as e:
        log.error(f"Failed to load {label}: {e}")
        return label, []
    records = []
    try:
        for record in fn():
            records.append(record)
            if dry_run and len(records) >= 5:
                break
    except Exception as e:
        log.error(f"  {label} crashed mid-scrape: {e}", exc_info=True)
    log.info(f"  {label}: {len(records)} keyword matches")
    return label, records


def scrape_all(selected_sources: list[str], dry_run: bool) -> list[dict]:
    raw = []
    # I/O-bound scrapers run in parallel; cap workers to avoid hammering rate limits
    max_workers = min(len(selected_sources), 5)
    log.info(f"Running {len(selected_sources)} scrapers ({max_workers} parallel workers)")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_run_one, key, dry_run): key for key in selected_sources}
        for future in as_completed(futures):
            _, records = future.result()
            raw.extend(records)
    return raw


def run(selected_sources, dry_run, verify, notify):
    db.init()

    # ── 1. Scrape ──────────────────────────────────────────────────────────
    raw = scrape_all(selected_sources, dry_run)
    log.info(f"Total raw matches: {len(raw)}")

    if dry_run:
        print(f"\n--- DRY RUN ({len(raw)} raw matches, no writes) ---")
        for r in raw[:15]:
            print(f"  [{r['source']}] {r['company']} — {r['title']}")
            print(f"    Keywords : {r['food_keywords_matched']}")
            print(f"    Excerpt  : {r['perk_excerpt'][:100]}")
            print()
        return

    if not raw:
        log.warning("No matches found.")
        return

    # ── 2. Live verification ───────────────────────────────────────────────
    if verify:
        live = verify_jobs(raw, max_workers=10)
    else:
        log.warning("Skipping verification (--no-verify)")
        live = raw

    # ── 3. Write raw CSV ───────────────────────────────────────────────────
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(live)

    # ── 4. Enrich: rollup → score → persist → export dashboard ────────────
    result = enricher.run()
    if not result:
        return
    new_cos, companies_df, stats = result

    # ── 5. Log run ─────────────────────────────────────────────────────────
    db.log_run(
        raw_count=len(raw),
        live_count=len(live),
        new_count=len(new_cos),
    )

    # ── 6. Slack notification ──────────────────────────────────────────────
    if notify and new_cos:
        sent = send_new_companies_alert(new_cos, stats)
        if sent:
            db.mark_notified([c["company"] for c in new_cos])

    # ── 7. Summary ─────────────────────────────────────────────────────────
    removed = len(raw) - len(live)
    print(f"""
Run complete
  Raw matches      : {len(raw)}
  Stale removed    : {removed}
  Live listings    : {len(live)}
  Companies total  : {stats.get('total_companies', '?')}
  Net new today    : {len(new_cos)}
  Slack alert      : {'sent' if notify and new_cos else 'skipped'}

Files
  Raw jobs         : {OUTPUT_CSV}
  Company rollup   : lunch_perk_jobs_enriched.csv
  Dashboard data   : dashboard_data.js
  Database         : lunch_perks.db
""")


def main():
    parser = argparse.ArgumentParser(description="Nationwide Lunch Perk Job Scraper")
    parser.add_argument("--sources", nargs="+", choices=list(SOURCES.keys()),
                    default=["gh", "lv", "ab", "wd", "js", "ap", "wf"])
    parser.add_argument("--no-verify",  action="store_true")
    parser.add_argument("--no-notify",  action="store_true")
    parser.add_argument("--dry-run",    action="store_true")
    args = parser.parse_args()

    run(
        selected_sources=args.sources,
        dry_run=args.dry_run,
        verify=not args.no_verify and not args.dry_run,
        notify=not args.no_notify and not args.dry_run,
    )


if __name__ == "__main__":
    main()
