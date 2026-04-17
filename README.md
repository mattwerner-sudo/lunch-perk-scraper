# NYC Lunch Perk Job Scraper

Identifies companies offering food perks (DoorDash, Grubhub, free lunch, etc.)
to NYC employees by analyzing their job postings. Built for GTM / ABM targeting.

## Setup

```bash
cd ~/lunch-perk-scraper
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium   # only needed if adding browser-based scrapers later
```

## Run

```bash
# Full run — all sources + live verification (removes stale/closed listings)
python3 scrape.py

# Quick test — only Greenhouse + Lever, first 5 results each, no verification
python3 scrape.py --sources gh lv --dry-run

# Specific sources
python3 scrape.py --sources gh lv ab gd   # Greenhouse, Lever, Ashby, Glassdoor

# Skip live verification (faster, but CSV may include stale listings)
python3 scrape.py --no-verify

# After scraping, enrich + score for GTM
python3 enrich.py
```

## Output files

| File | Description |
|------|-------------|
| `lunch_perk_jobs.csv` | Raw scraped matches |
| `lunch_perk_jobs_enriched.csv` | Deduped, scored, with inferred domain |
| `slack_summary.txt` | Copy-paste summary for your GTM Slack channel |

## Sources

| Key | Source | Why it works |
|-----|--------|-------------|
| `gh` | Greenhouse | Public JSON API, full JD content, no auth needed |
| `lv` | Lever | Public JSON API, full JD, no auth needed |
| `ab` | Ashby | Fast-growing SaaS ATS, JSON API |
| `bn` | Built In NYC | Lists company perk profiles explicitly — highest signal |
| `wd` | Workday | Enterprise companies (banks, media, etc.) |
| `gd` | Glassdoor Benefits | Employee-verified food perks; filtered to NYC companies |

## Adding more companies

**Greenhouse / Lever / Ashby:** Add the company's ATS slug to the list in the
respective scraper file. The slug is the subdomain in their careers URL:
- `https://boards.greenhouse.io/SLUG` → add `"SLUG"` to `GREENHOUSE_SLUGS`
- `https://jobs.lever.co/SLUG` → add `"SLUG"` to `LEVER_SLUGS`
- `https://jobs.ashbyhq.com/SLUG` → add `"SLUG"` to `ASHBY_SLUGS`

## Configuration

Edit `config.py` to:
- Add/remove food perk keywords (`FOOD_KEYWORDS`)
- Add/remove NYC location signals (`NYC_SIGNALS`)
- Tune request delays (`DELAY_BETWEEN_REQUESTS`)

## GTM Workflow

```
scrape.py → lunch_perk_jobs.csv
     ↓
enrich.py → lunch_perk_jobs_enriched.csv  (scored, deduped)
     ↓
Import to Salesforce / HubSpot / Clay
     ↓
Segment by gtm_score → prioritize top 20% for outbound
```

## Other Data Signals to Layer In (2nd/3rd Party)

Beyond job postings, here are other ways to identify NYC companies with food perks:

### High-signal (direct evidence)
- **DoorDash for Work / Grubhub Corporate accounts** — companies purchasing
  corporate accounts often announce it in press releases or Slack communities
- **Built In NYC company profiles** — structured perk fields, highly accurate
- **Glassdoor "Benefits" tab** — employees self-report perks; scrape-able
- **Levels.fyi company profiles** — tech companies list food perks explicitly
- **LinkedIn company pages** — "Life" tab often mentions office perks

### Medium-signal (inferred)
- **Office lease size** (CoStar, LoopNet) — large NYC tenants likely have
  catered lunches as retention
- **Headcount 100–2000 in NYC** — sweet spot for corporate lunch programs;
  too small = no budget, too large = cafeteria
- **G2 Crowd / Capterra tech stack** — companies using DoorDash for Work's
  API or Forkable's platform are findable via G2 integrations

### Intent signals
- **Job postings for "Office Manager" or "Facilities" in NYC** that mention
  food vendor management = high probability they have a lunch program
- **"Workplace Experience" or "Employee Experience" manager roles** — these
  people own the lunch program budget

### Communities to monitor
- **Slack communities**: Demand Collective, RevGenius, Pavilion, Sales Assembly
  — members often mention their company perks organically
- **Twitter/X and LinkedIn** — employees posting about "free lunch" or tagging
  @DoorDash_ForWork or @GrubhubCorporate
