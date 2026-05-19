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

# ── Contact enrichment ──────────────────────────────────────────────────────
# In-house decision-maker discovery for high-confidence companies.
# Runs after scoring; only companies with gtm_score >= ENRICH_MIN_SCORE are enriched.
ENRICH_MIN_SCORE   = 25   # matches "High" confidence tier in enrich.py
ENRICH_MAX_WORKERS = 4    # parallel enrichers per company
ENRICH_PER_COMPANY = 3    # max contacts returned per company
ENRICH_TIMEOUT     = 12   # seconds per discovery source

# Persona regex → normalized persona key. Order matters: first match wins.
# Drives both title-based filtering and downstream Slack/dashboard labels.
TARGET_PERSONAS: list[tuple[str, str]] = [
    (r"office\s+manager|office\s+admin|office\s+coordinator|office\s+ops|"
     r"office\s+operations|workplace\s+(experience|operations|coordinator|manager)|"
     r"facilit(y|ies)\s+(manager|coordinator|director)",                "office_manager"),
    (r"people\s+(ops|operations)|people\s+experience|"
     r"employee\s+experience|head\s+of\s+people|chief\s+people|"
     r"(vp|vice\s+president|director)\s+(of\s+)?people|"
     r"hr\s+(director|manager|lead)|human\s+resources",                 "people_ops"),
    (r"executive\s+assistant|exec\s+admin|chief\s+of\s+staff",          "exec_admin"),
    (r"\bcfo\b|chief\s+financial|controller|"
     r"head\s+of\s+finance|vp\s+(of\s+)?finance",                       "finance"),
    (r"\bcoo\b|chief\s+operating|head\s+of\s+ops|vp\s+(of\s+)?operations","operations"),
    (r"total\s+rewards|benefits\s+(manager|director|lead)",             "benefits"),
]
