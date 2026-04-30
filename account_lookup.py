"""
Account segmentation — three tiers:
  "managed"   → account with an assigned rep  (managed_accounts.csv,   ~875 rows)
  "unmanaged" → account without an assigned rep (unmanaged_accounts.csv, ~33k rows)
  "prospect"  → not in either account list

Matching strategy (in priority order):
  1. Domain match   — most reliable; used when inferred_domain overlaps CSV domain
  2. Name match     — normalized fuzzy token match as fallback
"""
import re
import csv
from pathlib import Path
from functools import lru_cache

MANAGED_CSV   = Path(__file__).parent / "managed_accounts.csv"
UNMANAGED_CSV = Path(__file__).parent / "unmanaged_accounts.csv"


# ── Normalization helpers ─────────────────────────────────────────────────────

def _norm_name(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(
        r"\b(inc|llc|ltd|corp|co|group|holdings|international|system|systems"
        r"|university|college|institute|services|solutions|technologies|technology"
        r"|associates|partners|consulting|foundation|trust|board of regents"
        r"|the)\b",
        " ", name,
    )
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def _norm_domain(domain: str) -> str:
    """Strip scheme, www, trailing slashes, lowercase."""
    d = domain.lower().strip().rstrip("/")
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    # Take only the root domain (ignore paths like mlb.com/padres)
    return d.split("/")[0]


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_csv(path: Path, name_col: str, domain_col: str) -> tuple[dict, dict]:
    """
    Returns (domain_index, name_index) for a given CSV.
    domain_index: {normalized_domain: row_dict}
    name_index:   {normalized_name:   row_dict}
    """
    domain_idx: dict[str, dict] = {}
    name_idx:   dict[str, dict] = {}
    if not path.exists():
        return domain_idx, name_idx

    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            account = {
                "account_name":     row.get(name_col, "").strip(),
                "domain":           row.get(domain_col, "").strip(),
                "zoominfo_id":      row.get("ZoomInfo Company ID", ""),
                "ezcater_vertical": row.get("ezCater Vertical", ""),
                "industry":         row.get("Industry", ""),
                "zi_industry":      row.get("Zoominfo Industry", ""),
            }
            raw_domain = row.get(domain_col, "").strip()
            if raw_domain:
                nd = _norm_domain(raw_domain)
                if nd:
                    domain_idx[nd] = account

            raw_name = row.get(name_col, "").strip()
            if raw_name and raw_name.lower() not in ("not provided", ""):
                nn = _norm_name(raw_name)
                if nn:
                    name_idx[nn] = account

    return domain_idx, name_idx


@lru_cache(maxsize=1)
def _managed() -> tuple[dict, dict]:
    return _load_csv(MANAGED_CSV, "Account Name", "Company Domain Name")


@lru_cache(maxsize=1)
def _unmanaged() -> tuple[dict, dict]:
    return _load_csv(UNMANAGED_CSV, "Account Name", "Domain")


# ── Public API ────────────────────────────────────────────────────────────────

def lookup(company_name: str, inferred_domain: str = "") -> tuple[str, dict | None]:
    """
    Return (segment, account_row).
    segment is one of: "managed", "unmanaged", "prospect"
    account_row is the matched CSV row dict, or None for prospects.

    Priority: managed > unmanaged > prospect.
    Matching: domain first, then name tokens.
    """
    nd = _norm_domain(inferred_domain) if inferred_domain else ""
    nn = _norm_name(company_name) if company_name else ""
    tokens = set(nn.split())

    def _find(domain_idx: dict, name_idx: dict) -> dict | None:
        # 1. Exact domain match
        if nd and nd in domain_idx:
            return domain_idx[nd]
        # 2. Exact normalized name match
        if nn and nn in name_idx:
            return name_idx[nn]
        # 3. All tokens in query appear in a known name (min 2 tokens)
        if len(tokens) >= 2:
            for key, row in name_idx.items():
                if tokens.issubset(set(key.split())):
                    return row
        return None

    mgd_domain, mgd_name = _managed()
    row = _find(mgd_domain, mgd_name)
    if row:
        return "managed", row

    unm_domain, unm_name = _unmanaged()
    row = _find(unm_domain, unm_name)
    if row:
        return "unmanaged", row

    return "prospect", None


def segment(company_name: str, inferred_domain: str = "") -> str:
    seg, _ = lookup(company_name, inferred_domain)
    return seg


def managed_count() -> int:
    d, n = _managed()
    return len(d)


def unmanaged_count() -> int:
    d, n = _unmanaged()
    return len(d)
