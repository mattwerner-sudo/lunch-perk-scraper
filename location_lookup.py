"""
Account location lookup.

Maps company domain → set of known office metro markets, derived from
Billing City / Billing State in managed_accounts.csv and unmanaged_accounts.csv.

This is more reliable than inferring market from JD location strings because:
  - Billing address is the actual office, not a remote employee's city
  - One company can have multiple offices — all are indexed
  - Used for territory routing even when JD location is blank

Usage:
    from location_lookup import get_markets, get_primary_market

    get_markets("acme.com")         # → {"New York", "Chicago"}
    get_primary_market("acme.com")  # → "New York"  (most offices)
"""
import csv
from pathlib import Path
from functools import lru_cache
from collections import defaultdict, Counter

MANAGED_CSV   = Path(__file__).parent / "managed_accounts.csv"
UNMANAGED_CSV = Path(__file__).parent / "unmanaged_accounts.csv"

# Mirrors MARKET_SIGNALS in enrich.py — kept in sync manually.
# Keyed on lowercase city+state for fast lookup.
_CITY_TO_MARKET: dict[str, str] = {}

_MARKET_CITY_SIGNALS: list[tuple[str, list[tuple[str, str | None]]]] = [
    # (market, [(city_lower, state_lower_or_None), ...])
    ("New York",      [("new york", None), ("manhattan", None), ("brooklyn", None),
                       ("queens", None), ("bronx", None), ("jersey city", "new jersey"),
                       ("hoboken", "new jersey")]),
    ("Boston",        [("boston", None), ("cambridge", "massachusetts"),
                       ("somerville", "massachusetts"), ("quincy", "massachusetts")]),
    ("Chicago",       [("chicago", None)]),
    ("San Francisco", [("san francisco", None), ("palo alto", None), ("mountain view", None),
                       ("redwood city", None), ("menlo park", None), ("san mateo", None),
                       ("sunnyvale", None), ("santa clara", None), ("oakland", None),
                       ("san jose", None), ("foster city", None), ("burlingame", None)]),
    ("Los Angeles",   [("los angeles", None), ("santa monica", None), ("culver city", None),
                       ("el segundo", None), ("manhattan beach", None), ("west hollywood", None),
                       ("pasadena", None), ("burbank", None), ("glendale", "california"),
                       ("long beach", "california"), ("irvine", None), ("anaheim", None)]),
    ("Seattle",       [("seattle", None), ("bellevue", "washington"), ("redmond", "washington"),
                       ("kirkland", "washington"), ("tacoma", None)]),
    ("Austin",        [("austin", "texas")]),
    ("Dallas",        [("dallas", None), ("fort worth", None), ("plano", "texas"),
                       ("irving", "texas"), ("frisco", "texas"), ("allen", "texas"),
                       ("arlington", "texas")]),
    ("Houston",       [("houston", None), ("the woodlands", None), ("sugar land", None),
                       ("katy", "texas")]),
    ("Atlanta",       [("atlanta", None), ("alpharetta", None), ("buckhead", None),
                       ("marietta", None), ("dunwoody", None)]),
    ("Washington DC", [("washington", "district of columbia"), ("washington", "dc"),
                       ("arlington", "virginia"), ("bethesda", "maryland"),
                       ("mclean", "virginia"), ("tysons", None), ("reston", "virginia"),
                       ("falls church", None), ("alexandria", "virginia")]),
    ("Philadelphia",  [("philadelphia", None), ("wilmington", "delaware"),
                       ("cherry hill", None), ("king of prussia", None)]),
    ("Miami",         [("miami", None), ("fort lauderdale", None), ("boca raton", None),
                       ("coral gables", None), ("doral", None)]),
    ("Denver",        [("denver", None), ("boulder", "colorado"), ("aurora", "colorado"),
                       ("lakewood", "colorado"), ("englewood", "colorado")]),
    ("Minneapolis",   [("minneapolis", None), ("st. paul", None), ("saint paul", None),
                       ("bloomington", "minnesota"), ("eden prairie", None)]),
    ("Phoenix",       [("phoenix", None), ("scottsdale", None), ("tempe", "arizona"),
                       ("chandler", "arizona"), ("mesa", "arizona"), ("gilbert", "arizona")]),
    ("Nashville",     [("nashville", None), ("franklin", "tennessee"), ("brentwood", "tennessee")]),
    ("Charlotte",     [("charlotte", "north carolina")]),
    ("Raleigh",       [("raleigh", None), ("durham", None), ("chapel hill", None), ("cary", None)]),
    ("Indianapolis",  [("indianapolis", None)]),
    ("Columbus",      [("columbus", "ohio")]),
    ("San Diego",     [("san diego", None)]),
    ("Portland",      [("portland", "oregon")]),
    ("Salt Lake City",[("salt lake city", None), ("provo", None)]),
    ("Kansas City",   [("kansas city", None), ("overland park", None)]),
    ("St. Louis",     [("st. louis", None), ("saint louis", None)]),
    ("Detroit",       [("detroit", None), ("ann arbor", None), ("troy", "michigan"),
                       ("dearborn", None)]),
    ("Pittsburgh",    [("pittsburgh", None)]),
    ("Cincinnati",    [("cincinnati", None)]),
    ("Cleveland",     [("cleveland", None)]),
    ("Baltimore",     [("baltimore", None)]),
    ("San Antonio",   [("san antonio", None)]),
    ("Orlando",       [("orlando", None)]),
    ("Tampa",         [("tampa", None), ("st. petersburg", "florida"), ("clearwater", None)]),
    ("Las Vegas",     [("las vegas", None), ("henderson", "nevada")]),
]


def _city_market(city: str, state: str) -> str:
    city_l  = city.strip().lower()
    state_l = state.strip().lower()
    for market, signals in _MARKET_CITY_SIGNALS:
        for sig_city, sig_state in signals:
            if city_l == sig_city and (sig_state is None or sig_state in state_l):
                return market
    return "Other"


@lru_cache(maxsize=1)
def _build_index() -> dict[str, Counter]:
    """
    Returns domain → Counter({market: office_count}).
    Loaded once from both CSVs.
    """
    index: dict[str, Counter] = defaultdict(Counter)

    for path, domain_col in [
        (MANAGED_CSV,   "Company Domain Name"),
        (UNMANAGED_CSV, "Domain"),
    ]:
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                domain = row.get(domain_col, "").strip().lower()
                city   = row.get("Billing City", "").strip()
                state  = row.get("Billing State/Province", "").strip()
                if not domain or not city:
                    continue
                market = _city_market(city, state)
                index[domain][market] += 1

    return dict(index)


def get_markets(domain: str) -> set[str]:
    """All known metro markets for this domain (from billing addresses)."""
    domain = domain.strip().lower()
    counter = _build_index().get(domain, {})
    return {m for m in counter if m != "Other"}


def get_primary_market(domain: str) -> str:
    """
    Single best market for territory routing.
    Returns the market with the most office rows, or 'Other' if unknown.
    """
    domain = domain.strip().lower()
    counter = _build_index().get(domain, {})
    if not counter:
        return "Other"
    # Prefer non-Other market with highest count
    best = max(
        ((m, n) for m, n in counter.items() if m != "Other"),
        key=lambda x: x[1],
        default=None,
    )
    return best[0] if best else "Other"


def get_all_office_cities(domain: str) -> list[dict]:
    """
    Return raw city+state rows for a domain — for dashboard display.
    """
    domain = domain.strip().lower()
    results = []
    seen = set()
    for path, domain_col in [
        (MANAGED_CSV,   "Company Domain Name"),
        (UNMANAGED_CSV, "Domain"),
    ]:
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get(domain_col, "").strip().lower() != domain:
                    continue
                city  = row.get("Billing City", "").strip()
                state = row.get("Billing State/Province", "").strip()
                if not city:
                    continue
                key = (city.lower(), state.lower())
                if key in seen:
                    continue
                seen.add(key)
                results.append({
                    "city":   city,
                    "state":  state,
                    "market": _city_market(city, state),
                })
    return results
