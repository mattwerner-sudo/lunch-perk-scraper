"""
Scraper configuration — edit these to tune your search.
"""

# ── Food perk keywords ──────────────────────────────────────────────────────
# Any job description containing at least one of these (case-insensitive)
# will be flagged as a lunch-perk role.
FOOD_KEYWORDS = [
    "free lunch",
    "free food",
    "free meals",
    "catered lunch",
    "catered meals",
    "lunch provided",
    "meals provided",
    "doordash",
    "grubhub",
    "ubereats",
    "uber eats",
    "forkable",
    "sharebite",
    "seamless corporate",  # NYC dominant; "seamless" alone is too generic
    "seamless for business",
    "order seamless",
    "seamless account",
    "daily lunch",
    "company lunch",
    "office lunch",
    "meal stipend",
    "food stipend",
    "lunch stipend",
    "lunch credit",
    "food credit",
    "meal credit",
    "snacks and meals",
    "fully stocked kitchen",
    "stocked kitchen",
]

# ── NYC location signals ────────────────────────────────────────────────────
NYC_SIGNALS = [
    "new york city",
    "new york, ny",
    "new york, new york",
    "nyc",
    " ny ",      # surrounded by spaces to avoid false positives like "any"
    "manhattan",
    "brooklyn",
    "queens",
    "bronx",
    "new york",
]

# ── Job search queries ──────────────────────────────────────────────────────
# Search directly for food perk keywords — any role at any company.
# We don't care about the role. We care about the company having food perks.
# JobSpy searches these terms in job descriptions across LinkedIn/Indeed/Google.
# The food keyword filter in utils.py then confirms the match.

SEARCH_QUERIES = [
    "free lunch",
    "catered lunch",
    "catered meals",
    "DoorDash",
    "GrubHub",
    "Uber Eats",
    "Forkable",
    "Sharebite",
    "meal stipend",
    "lunch stipend",
    "food stipend",
    "meal credit",
    "stocked kitchen",
    "daily lunch",
]

# ── Output ──────────────────────────────────────────────────────────────────
OUTPUT_CSV = "lunch_perk_jobs.csv"
OUTPUT_ENRICHED_CSV = "lunch_perk_jobs_enriched.csv"  # after dedup + scoring

# ── Request settings ────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 15       # seconds per HTTP request
DELAY_BETWEEN_REQUESTS = 1.2  # seconds; be a good citizen
MAX_RETRIES = 3
