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
    "catered breakfast",
    "lunch provided",
    "meals provided",
    "lunch is on us",
    "we provide lunch",
    "doordash",
    "grubhub",
    "ubereats",
    "uber eats",
    "forkable",
    "sharebite",
    "caviar",
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
    "fully stocked kitchen",
]

# ── Location filtering ───────────────────────────────────────────────────────
# Set to None to scrape nationwide (recommended).
# Set to a list of strings to restrict to specific markets, e.g.:
#   LOCATION_FILTER = ["new york", "nyc", "boston", "chicago"]
# Any job whose location contains at least one of these strings (case-insensitive)
# will pass the filter. All others are dropped.
LOCATION_FILTER = None  # None = no geographic restriction

# ── Job search queries ──────────────────────────────────────────────────────
# Search directly for food perk keywords — any role at any company.
# We don't care about the role. We care about the company having food perks.
# JobSpy searches these terms in job descriptions across LinkedIn/Indeed/Google.
# The food keyword filter in utils.py then confirms the match.

SEARCH_QUERIES = [
    "free lunch",
    "catered lunch",
    "catered meals",
    "catered breakfast",
    "DoorDash",
    "GrubHub",
    "Uber Eats",
    "Forkable",
    "Sharebite",
    "Caviar",
    "meal stipend",
    "lunch stipend",
    "food stipend",
    "meal credit",
    "daily lunch",
    "free food",
    "lunch provided",
    "fully stocked kitchen",
    "company lunch",
    "office lunch",
    "ezCater",
]

# ── Output ──────────────────────────────────────────────────────────────────
OUTPUT_CSV = "lunch_perk_jobs.csv"
OUTPUT_ENRICHED_CSV = "lunch_perk_jobs_enriched.csv"  # after dedup + scoring

# ── Request settings ────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 15       # seconds per HTTP request
DELAY_BETWEEN_REQUESTS = 1.2  # seconds; be a good citizen
MAX_RETRIES = 3
