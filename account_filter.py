"""
Account pre-filter and tier assignment.

Three-brain design:
  GTM (Clay)   — waterfall: only work accounts worth touching
  Quant        — suppress false-positive verticals before any compute
  Infra (Google) — load once, cache, profile coverage

Tiers:
  1 — Managed (all 875, every run)
  2 — Unmanaged ICP match (~19.5k, rotating 1k/run)
  3 — Unmanaged unclassified (4.4k, occasional sample)
  X — Suppressed (9.7k, never)
"""
import csv
import random
from pathlib import Path
from functools import lru_cache

MANAGED_CSV   = Path(__file__).parent / "managed_accounts.csv"
UNMANAGED_CSV = Path(__file__).parent / "unmanaged_accounts.csv"

# ── ICP vertical whitelist ────────────────────────────────────────────────────
ICP_EZ_VERTICALS = {
    "Technology", "Info and Comms", "Business Services",
    "Construction and Engineering", "Education", "Pharma",
    "Transportation", "Professional Services",
}

ICP_ZI_INDUSTRIES = {
    "Law Firms & Legal Services", "Industrial Machinery & Equipment",
    "Colleges & Universities", "Custom Software & IT Services",
    "Pharmaceuticals", "Medical Devices & Equipment",
    "Management Consulting", "Accounting Services",
    "Architecture, Engineering & Design", "Commercial & Residential Construction",
    "Content & Collaboration Software", "Advertising & Marketing",
    "Research & Development", "Building Materials",
}

# ── Suppression lists — high false-positive rate for food perk signals ────────
SUPPRESS_EZ_VERTICALS = {
    "Health Social Services", "Retail and Wholesale", "Rec and Arts",
    "Government and Education", "Other", "Unknown", "Retail", "Retail and Tech",
}

SUPPRESS_ZI_INDUSTRIES = {
    "Non-Profit & Charitable Organizations", "Religious Organizations",
    "Elderly Care Services", "K-12 Schools", "Membership Organizations",
    "Restaurants & Food Service", "Hospitality",
}

# Company name keywords that indicate food/restaurant industry (false positives)
SUPPRESS_NAME_TOKENS = {
    "restaurant", "pizza", "burger", "taco", "sushi", "grill", "cafe",
    "bakery", "catering", "food service", "hospitality", "hotel", "resort",
}


def _is_suppressed(row: dict) -> bool:
    ez  = row.get("ezCater Vertical", "").strip()
    zi  = row.get("Zoominfo Industry", "").strip()
    name = row.get("Account Name", "").lower()
    if ez in SUPPRESS_EZ_VERTICALS:
        return True
    if zi in SUPPRESS_ZI_INDUSTRIES:
        return True
    if any(tok in name for tok in SUPPRESS_NAME_TOKENS):
        return True
    return False


def _is_icp(row: dict) -> bool:
    ez = row.get("ezCater Vertical", "").strip()
    zi = row.get("Zoominfo Industry", "").strip()
    return ez in ICP_EZ_VERTICALS or zi in ICP_ZI_INDUSTRIES


def _load_csv(path: Path, domain_col: str) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # Normalise domain column to "domain" key
    for r in rows:
        r["_domain"] = r.get(domain_col, "").strip().lower()
    return [r for r in rows if r["_domain"]]  # drop rows with no domain


@lru_cache(maxsize=1)
def _managed_accounts() -> list[dict]:
    return _load_csv(MANAGED_CSV, "Company Domain Name")


@lru_cache(maxsize=1)
def _unmanaged_accounts() -> list[dict]:
    return _load_csv(UNMANAGED_CSV, "Domain")


def get_tier1() -> list[dict]:
    """All managed accounts — scrape every run."""
    return _managed_accounts()


def get_tier2(sample_size: int = 1000, seed: int | None = None) -> list[dict]:
    """
    ICP-matched unmanaged accounts.
    Returns a random sample of `sample_size` per run so the full list
    rotates over ~19 weeks (19,500 accounts / 1,000 per run).
    """
    accounts = _unmanaged_accounts()
    icp = [r for r in accounts if not _is_suppressed(r) and _is_icp(r)]
    if seed is not None:
        random.seed(seed)
    return random.sample(icp, min(sample_size, len(icp)))


def get_tier3(sample_size: int = 100) -> list[dict]:
    """Unclassified unmanaged — small rotating sample."""
    accounts = _unmanaged_accounts()
    unclassified = [
        r for r in accounts
        if not _is_suppressed(r) and not _is_icp(r)
    ]
    return random.sample(unclassified, min(sample_size, len(unclassified)))


def get_all_tiers(
    tier2_sample: int = 1000,
    tier3_sample: int = 100,
    include_tier3: bool = False,
) -> list[dict]:
    """
    Full account list for a targeted scrape run.
    Tier 1 always included. Tier 3 optional (off by default).
    """
    accounts = get_tier1() + get_tier2(tier2_sample)
    if include_tier3:
        accounts += get_tier3(tier3_sample)
    # Deduplicate by domain
    seen = set()
    deduped = []
    for r in accounts:
        d = r["_domain"]
        if d not in seen:
            seen.add(d)
            deduped.append(r)
    return deduped


def coverage_stats() -> dict:
    """Return stats for observability reporting."""
    managed   = _managed_accounts()
    unmanaged = _unmanaged_accounts()
    icp       = [r for r in unmanaged if not _is_suppressed(r) and _is_icp(r)]
    suppressed = [r for r in unmanaged if _is_suppressed(r)]
    unclass   = [r for r in unmanaged if not _is_suppressed(r) and not _is_icp(r)]
    return {
        "managed":          len(managed),
        "unmanaged_total":  len(unmanaged),
        "unmanaged_icp":    len(icp),
        "unmanaged_suppressed": len(suppressed),
        "unmanaged_unclassified": len(unclass),
        "weeks_to_full_cycle": round(len(icp) / 1000, 1),
    }
