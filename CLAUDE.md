# lunch-perk-scraper — Claude Code Project Guide

## What this tool does
Scrapes job postings nationwide to find companies offering food perks (free lunch, catered meals, DoorDash, GrubHub, meal stipends, etc.). Outputs a scored company list for GTM/sales use, segmented by account ownership.

## Account segmentation (3 tiers)
- **managed** — account has an assigned rep (`managed_accounts.csv`, ~875 accounts)
- **unmanaged** — account exists but has no assigned rep (`unmanaged_accounts.csv`, ~33k accounts)
- **prospect** — not in either list (cold, unknown)
- Segment ≠ customer status. Do not assume managed = customer or unmanaged = non-customer.

## Key files
- `config.py` — keywords, location filter (set to None = nationwide), output paths
- `scrape.py` — parallel scraper runner (ThreadPoolExecutor, max 5 workers)
- `scrapers/` — one module per source (Greenhouse, Lever, Ashby, Workday, JobSpy, Apify, Wellfound, Exa, Firecrawl, Glassdoor, Built In)
- `account_lookup.py` — domain-first, three-tier account matching
- `enrich.py` — rolls up job→company, scores, infers market, tags segment
- `db.py` — SQLite persistence (companies + runs tables); non-destructive column migrations
- `notify_slack.py` — three separate Slack streams (managed / unmanaged / prospect)
- `dashboard.html` — static dashboard; filter by segment, market, confidence, source, keywords

## Scoring logic (`enrich.py`)
- Food keyword score + source bonus + ICP vertical + size signals + persona signals
- Managed accounts: +15 score boost. Unmanaged: +8 boost.
- Confidence tiers: High ≥25, Medium ≥12, Low <12

## Architecture principles
- Language: Python only. No Node.js.
- DB: SQLite now; migrate to Postgres (Supabase) when multi-user or concurrent writes are needed.
- Scraping: I/O-bound → ThreadPoolExecutor. Never asyncio (scrapers use requests, not httpx/aiohttp).
- Data volume: hundreds–low thousands of companies per run. Pandas is fine; do not rewrite to Polars.
- No LLM extraction pipeline — `find_food_keywords()` in `utils.py` handles classification.
- No vector dedup — company-level dedup is handled at upsert time in `db.py`.
- Location filter: `LOCATION_FILTER = None` in `config.py` means nationwide. Set to a list to restrict.

## Slack env vars (.env)
- `SLACK_WEBHOOK_URL` — prospect alerts
- `SLACK_WEBHOOK_MANAGED_URL` — managed account alerts (falls back to above)
- `SLACK_WEBHOOK_UNMANAGED_URL` — unmanaged account alerts (falls back to above)

## What's next / planned
- Territory → rep routing table (market → rep name / Slack handle)
- Domain-based account matching already live; name fuzzy match is fallback
- Supabase Postgres migration (when ready to scale)
- TheirStack API ($59/mo) — best single addition for job discovery breadth
- PDL API (free tier) — company enrichment (industry, size, HQ)
