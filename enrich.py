"""
Post-scrape enrichment pipeline.

Rolls up job-level records to company-level, scores them,
persists to SQLite, and exports dashboard data + CSV.
"""
import re
import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from config import OUTPUT_CSV, OUTPUT_ENRICHED_CSV
import account_lookup

# ── Scoring rubric ───────────────────────────────────────────────────────────
KEYWORD_SCORE = {
    "doordash": 10,
    "grubhub": 10,
    "ubereats": 10,
    "uber eats": 10,
    "forkable": 10,
    "sharebite": 10,
    "seamless corporate": 8,
    "catered lunch": 7,
    "catered meals": 7,
    "free lunch": 7,
    "free food": 6,
    "free meals": 6,
    "daily lunch": 7,
    "meal stipend": 5,
    "food stipend": 5,
    "lunch stipend": 5,
    "meal credit": 5,
    "food credit": 5,
    "lunch credit": 5,
    "stocked kitchen": 3,
    "fully stocked kitchen": 4,
}

SOURCE_BONUS = {
    "Glassdoor Benefits": 8,
    "Built In NYC (Company Perk)": 6,
    "Built In NYC": 3,
    "Greenhouse": 2,
    "Lever": 2,
    "Ashby": 2,
    "Workday": 1,
}

# ── ezcater ICP vertical scoring ─────────────────────────────────────────────
# Companies in these verticals are more likely to be a fit for Corporate Portal
# or Relish. Matched against company name + job title + industry field.

ICP_VERTICAL_SIGNALS = {
    # Tier 1 verticals — highest fit
    "pharma": 8, "pharmaceutical": 8, "biotech": 8, "biotechnology": 8,
    "medical device": 8, "life sciences": 8,
    "finance": 6, "financial services": 6, "investment": 6, "asset management": 6,
    "insurance": 6, "banking": 6,
    "technology": 5, "software": 5, "saas": 5, "ecommerce": 5,

    # Tier 2 verticals
    "construction": 5, "engineering": 5, "real estate": 4,
    "consulting": 4, "management consulting": 4, "accounting": 4, "legal": 4,
    "law firm": 4, "professional services": 4,

    # Education (universities only — not K-12)
    "university": 5, "college": 4,

    # Additional Relish verticals
    "advertising": 3, "marketing agency": 3, "media": 3,
    "sports": 4, "professional sports": 5,
}

# ── ezcater ICP size signals ──────────────────────────────────────────────────
# Job title keywords that suggest the company is large enough to be Tier 1/2.
# Senior titles at enterprise companies signal 1,000+ employee orgs.

SIZE_SIGNALS = {
    "global": 4, "enterprise": 4, "fortune": 5,
    "director": 3, "vp ": 3, "vice president": 3, "head of": 2, "senior": 1,
}

# ── ezcater buyer persona signals ────────────────────────────────────────────
# Job titles matching our buyer personas get a bonus — they are the decision
# makers who actually own food programs.

PERSONA_SIGNALS = {
    "facilities": 5, "workplace experience": 5, "workplace operations": 5,
    "real estate": 4, "office manager": 4, "office operations": 4,
    "total rewards": 5, "people operations": 4, "employee experience": 4,
    "procurement": 5, "sourcing": 4, "vendor management": 4,
    "program manager": 3, "director of operations": 3,
    "human resources": 3, "hr director": 4, "hr manager": 3,
}

CONFIDENCE_TIERS = {
    range(25, 999): "High",
    range(12, 25):  "Medium",
    range(0, 12):   "Low",
}


def get_confidence(score: int) -> str:
    for r, label in CONFIDENCE_TIERS.items():
        if score in r:
            return label
    return "Low"


def _icp_tier(score: int) -> str:
    """Map GTM score to ezcater ICP tier language."""
    if score >= 30:
        return "Very Viable"
    if score >= 18:
        return "Viable"
    return "Monitor"


def score_row(row) -> int:
    s = 0
    kws = str(row.get("food_keywords_matched", "")).lower()
    title = str(row.get("title", "") or "").lower()
    company = str(row.get("company", "") or "").lower()
    excerpt = str(row.get("perk_excerpt", "") or "").lower()
    combined = f"{company} {title} {excerpt}"

    # Food keyword score
    s += sum(v for k, v in KEYWORD_SCORE.items() if k in kws)
    s += int(row.get("keyword_count", 1)) * 2

    # Source bonus
    s += SOURCE_BONUS.get(row.get("source", ""), 0)

    # ICP vertical bonus — is this company in a target industry?
    s += sum(v for k, v in ICP_VERTICAL_SIGNALS.items() if k in combined)

    # Size signal bonus — does the title/company suggest enterprise scale?
    s += sum(v for k, v in SIZE_SIGNALS.items() if k in combined)

    # Persona bonus — is this a decision-maker title we care about?
    s += sum(v for k, v in PERSONA_SIGNALS.items() if k in title)

    return s


# ── Metro market inference ────────────────────────────────────────────────────
MARKET_SIGNALS: dict[str, list[str]] = {
    "New York":     ["new york", "nyc", "manhattan", "brooklyn", "queens", "bronx"],
    "Boston":       ["boston", "cambridge, ma", "somerville", " ma,", ", ma "],
    "Chicago":      ["chicago", " il,", ", il "],
    "San Francisco":["san francisco", "sf,", "bay area", "palo alto", "mountain view",
                     "redwood city", "menlo park", "san mateo", "sunnyvale", "santa clara"],
    "Los Angeles":  ["los angeles", " la,", ", la ", "santa monica", "culver city", "el segundo"],
    "Seattle":      ["seattle", "bellevue, wa", "redmond, wa", "kirkland, wa"],
    "Austin":       ["austin", " tx,", ", tx "],
    "Dallas":       ["dallas", "fort worth", "plano, tx", "irving, tx"],
    "Houston":      ["houston", "the woodlands"],
    "Atlanta":      ["atlanta", "alpharetta", "buckhead"],
    "Washington DC":["washington, dc", "washington dc", " dc,", "arlington, va",
                     "bethesda", "mclean, va", "tysons"],
    "Philadelphia": ["philadelphia", "philly", " pa,", ", pa "],
    "Miami":        ["miami", "fort lauderdale", "boca raton"],
    "Denver":       ["denver", "boulder, co", "aurora, co"],
    "Minneapolis":  ["minneapolis", "st. paul", "twin cities"],
    "Phoenix":      ["phoenix", "scottsdale", "tempe, az"],
}


def infer_market(location: str) -> str:
    """Map a location string to a named metro market."""
    loc = (location or "").lower()
    for market, signals in MARKET_SIGNALS.items():
        if any(sig in loc for sig in signals):
            return market
    return "Other"


def infer_domain(company: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9 ]", "", company).strip().lower()
    slug = clean.split()[0] if clean else "unknown"
    return f"{slug}.com"


def rollup_to_companies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse job-level rows into one row per company.
    Aggregates: role count, all unique keywords, best score, best source, sample role/URL.
    """
    df["company"] = df["company"].fillna("").astype(str)
    df["gtm_score"] = df.apply(score_row, axis=1)
    df["company_norm"] = df["company"].str.strip().str.lower()
    df = df[df["company_norm"] != ""]  # drop rows with no company name

    groups = defaultdict(list)
    for _, row in df.iterrows():
        groups[row["company_norm"]].append(row.to_dict())

    records = []
    for norm_name, rows in groups.items():
        # Pick the best row as the "representative"
        best = max(rows, key=lambda r: r.get("gtm_score", 0))

        # Unique job titles — deduplicate across sources (same title from
        # LinkedIn and Greenhouse counts as ONE role, not two)
        unique_titles = {
            str(r.get("title", "") or "").strip().lower()
            for r in rows
            if str(r.get("title", "") or "").strip()
        }

        # Aggregate all unique food keywords seen across every role
        all_kws = set()
        for r in rows:
            for kw in str(r.get("food_keywords_matched", "")).split(", "):
                if kw.strip():
                    all_kws.add(kw.strip())

        # All sources that found this company
        sources_seen = list({r.get("source", "") for r in rows})

        # Source hierarchy: prefer highest-signal source
        source_priority = list(SOURCE_BONUS.keys())
        best_source = next(
            (s for s in source_priority if s in sources_seen),
            sources_seen[0] if sources_seen else "",
        )

        # All sources as a readable string for the dashboard
        all_sources = ", ".join(sorted(sources_seen))

        score = best["gtm_score"]
        company_name = best["company"].strip()
        location_str = best.get("location", "")

        # Account segmentation — domain-first, three tiers
        domain = infer_domain(company_name)
        seg, acct_row = account_lookup.lookup(company_name, domain)
        ezcater_vertical = acct_row["ezcater_vertical"] if acct_row else ""
        zi_industry      = acct_row["zi_industry"]      if acct_row else ""
        # Score boosts: managed (rep-owned) > unmanaged (no rep yet) > prospect (unknown)
        if seg == "managed":
            score += 15
        elif seg == "unmanaged":
            score += 8

        records.append({
            "company":             company_name,
            "inferred_domain":     infer_domain(company_name),
            "gtm_score":           score,
            "confidence":          get_confidence(score),
            "icp_tier":            _icp_tier(score),
            "segment":             seg,
            "market":              infer_market(location_str),
            "ezcater_vertical":    ezcater_vertical,
            "zi_industry":         zi_industry,
            "unique_roles_with_perk": len(unique_titles),
            "role_count":          len(unique_titles),
            "top_keywords":        ", ".join(sorted(all_kws)),
            "best_source":         best_source,
            "all_sources":         all_sources,
            "location":            location_str,
            "remote":              best.get("remote", ""),
            "sample_title":        best.get("title", ""),
            "sample_url":          best.get("url", ""),
            "perk_excerpt":        best.get("perk_excerpt", "")[:200],
            "date_first_seen":     best.get("date_posted", ""),
            "is_new":              1,  # will be updated by db.py on upsert
        })

    result = pd.DataFrame(records).sort_values("gtm_score", ascending=False)
    return result.reset_index(drop=True)


def export_dashboard_js(companies_df: pd.DataFrame, stats: dict, path: str = "dashboard_data.js"):
    """Export company data as a JS file loadable by the dashboard HTML."""
    records = companies_df.to_dict(orient="records")
    js = (
        "// Auto-generated by enrich.py — do not edit manually\n"
        f"const DASHBOARD_DATA = {json.dumps(records, indent=2)};\n\n"
        f"const DASHBOARD_STATS = {json.dumps(stats, indent=2)};\n"
    )
    Path(path).write_text(js, encoding="utf-8")
    print(f"Dashboard data exported → {path}")


def run():
    if not Path(OUTPUT_CSV).exists():
        print(f"No raw data found at {OUTPUT_CSV}. Run scrape.py first.")
        return

    import db
    db.init()

    df = pd.read_csv(OUTPUT_CSV)
    print(f"Raw job records   : {len(df)}")

    # Roll up to company level
    companies = rollup_to_companies(df)
    print(f"Unique companies  : {len(companies)}")

    # Persist to SQLite — get net new vs updated
    new_cos, updated_cos = db.upsert_companies(companies.to_dict(orient="records"))
    print(f"Net new companies : {len(new_cos)}")
    print(f"Updated companies : {len(updated_cos)}")

    # Mark is_new correctly from DB perspective
    new_names = {c["company"].strip().lower() for c in new_cos}
    companies["is_new"] = companies["company"].str.strip().str.lower().isin(new_names).astype(int)

    # Write enriched company-level CSV
    companies.to_csv(OUTPUT_ENRICHED_CSV, index=False)
    print(f"Saved to          : {OUTPUT_ENRICHED_CSV}")

    # Export dashboard JS
    stats = db.get_stats()
    stats["run_date"] = __import__("datetime").date.today().isoformat()
    export_dashboard_js(companies, stats)

    # Slack summary
    top = companies.head(5)[["company", "gtm_score", "top_keywords"]].to_string(index=False)
    print(f"\nTop companies by GTM score:\n{top}")

    return new_cos, companies, stats


if __name__ == "__main__":
    run()
