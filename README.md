# Lunch Perk Job Scraper

Identifies companies offering food perks (DoorDash, Grubhub, free lunch, catered meals, meal stipends, etc.) by analyzing job postings **nationwide**. Scores and segments companies for GTM / outbound targeting across managed accounts, unmanaged accounts, and net-new prospects.

## What it does

- Scrapes 11+ job sources weekly for food perk signals in job descriptions
- Scores each company by keyword strength, source credibility, and hiring volume
- Segments results: **managed** (rep-owned), **unmanaged** (in CRM, no rep), **prospect** (net-new)
- Detects **office expansion** — when a company posts food-perk JDs in a city not in their billing data (greenfield sale signal)
- Routes Slack alerts by territory market
- Publishes a live dashboard to GitHub Pages after every run

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # add your API keys
```

### Required API keys (`.env`)

| Key | Used for |
|---|---|
| `SLACK_WEBHOOK_URL` | Prospect alerts |
| `APIFY_API_TOKEN` | LinkedIn scraping via Apify |
| `EXA_API_KEY` | Exa semantic search |
| `FIRECRAWL_API_KEY` | Firecrawl ATS discovery |
| `THEIRSTACK_API_KEY` | TheirStack job search (account monitor + discovery) |

## Run

```bash
# Full run — all sources, live verification, Slack alert
python3 scrape.py

# Dry run — preview only, no writes, no credits spent
python3 scrape.py --dry-run

# Specific sources
python3 scrape.py --sources gh lv ab ts --no-notify

# TheirStack account monitor (checks all managed + ICP unmanaged domains)
python3 scrape.py --sources ts

# TheirStack discovery (net-new companies, no domain filter)
python3 scrape.py --sources ts --ts-mode discovery

# Targeted ATS scraper (direct per-account ATS polling)
python3 scrape.py --targeted
```

## Sources

| Key | Source | Notes |
|---|---|---|
| `gh` | Greenhouse | Public JSON API, full JD content |
| `lv` | Lever | Public JSON API, full JD |
| `ab` | Ashby | Fast-growing SaaS ATS |
| `bn` | Built In NYC | Company perk profiles — highest signal |
| `wd` | Workday | Enterprise companies |
| `gd` | Glassdoor Benefits | Employee-verified perks |
| `js` | JobSpy (Indeed/Google) | Broad job board coverage |
| `ap` | Apify LinkedIn | LinkedIn job postings |
| `wf` | Wellfound | Startup/tech companies |
| `ex` | Exa | Semantic search across ATS domains |
| `fc` | Firecrawl | ATS discovery via web crawl |
| `ts` | TheirStack | Job search API — account monitor + discovery |

## Output

| File | Description |
|---|---|
| `lunch_perk_jobs.csv` | Raw scraped matches |
| `lunch_perk_jobs_enriched.csv` | Deduped, scored, segmented by account |
| `dashboard_data.js` | Powers the live dashboard |

**Live dashboard:** [mattwerner-sudo.github.io/lunch-perk-scraper](https://mattwerner-sudo.github.io/lunch-perk-scraper/)

## Scoring

| Signal | Score |
|---|---|
| DoorDash / Grubhub / UberEats mention | +10 |
| Free lunch / catered meals | +7 |
| Meal stipend / food stipend | +5 |
| Stocked kitchen | +3 |
| Glassdoor Benefits source | +8 |
| Built In perk badge | +6 |
| Managed account | +15 |
| Unmanaged account | +8 |
| Expansion confirmed (new office signal) | +15 |
| Expansion possible | +8 |
| Existing office confirmed | +10 |

**Confidence tiers:** High ≥25 · Medium ≥12 · Low <12

## Location & Expansion Detection

Billing address data (from CRM export) is authoritative for territory routing. Job description locations are used for **expansion detection**: if a company posts food-perk JDs in a city not in their billing data, that's a signal they're opening a new office — a greenfield sale opportunity.

## Account Segmentation

- **managed** — account has an assigned rep (`managed_accounts.csv`)
- **unmanaged** — in CRM, no assigned rep (`unmanaged_accounts.csv`)
- **prospect** — not in either list

Segment ≠ customer status. Do not assume managed = customer.

## Territory Routing

Edit `territories.csv` to assign reps to markets. Each row maps a metro market to a rep name, Slack handle, and webhook env var. 17 markets supported. Once filled in, Slack alerts route automatically to the right rep.

## Architecture

```
scrape.py (parallel ThreadPoolExecutor)
    → scrapers/          one module per source
    → verify_live.py     confirm job URLs are still live
    → enrich.py          rollup → score → expansion detection → export
    → db.py              SQLite persistence (WAL mode)
    → notify_slack.py    territory-routed Slack alerts
    → dashboard_data.js  static dashboard on GitHub Pages
```
