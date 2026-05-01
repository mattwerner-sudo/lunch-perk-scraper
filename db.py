"""
SQLite persistence layer.

Tracks every company ever seen so each run only surfaces NET NEW companies.
Schema is intentionally flat — one row per company, updated in place.
"""
import sqlite3
import json
from datetime import date
from pathlib import Path

DB_PATH = Path(__file__).parent / "lunch_perks.db"


def _add_column_if_missing(con: sqlite3.Connection, table: str, column: str, definition: str):
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    # WAL mode: concurrent readers don't block writers (Google Infra principle)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def init():
    """Create tables if they don't exist."""
    with _conn() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            domain          TEXT,
            first_seen      TEXT NOT NULL,
            last_seen       TEXT NOT NULL,
            times_seen      INTEGER DEFAULT 1,
            is_new          INTEGER DEFAULT 1,   -- 1 on first appearance, 0 after
            gtm_score       INTEGER DEFAULT 0,
            top_keywords    TEXT,
            role_count      INTEGER DEFAULT 0,
            sample_title    TEXT,
            sample_url      TEXT,
            location        TEXT,
            perk_excerpt    TEXT,
            source          TEXT,
            notified        INTEGER DEFAULT 0,   -- 1 after Slack notification sent
            segment         TEXT DEFAULT 'prospect', -- 'managed' or 'prospect'
            market          TEXT,                -- city/metro inferred from location
            ezcater_vertical TEXT,              -- from managed_accounts.csv if matched
            zi_industry     TEXT                -- ZoomInfo industry from managed_accounts.csv
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_company_name
            ON companies(name);

        -- Velocity tracking: one row per company per week
        CREATE TABLE IF NOT EXISTS company_velocity (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name  TEXT NOT NULL,
            week          TEXT NOT NULL,  -- ISO week: YYYY-WW
            signal_count  INTEGER DEFAULT 0,
            keyword_set   TEXT,
            UNIQUE(company_name, week)
        );
        """)
        # Non-destructive migrations for existing databases
        _add_column_if_missing(con, "companies", "segment",              "TEXT DEFAULT 'prospect'")
        _add_column_if_missing(con, "companies", "market",               "TEXT")
        _add_column_if_missing(con, "companies", "ezcater_vertical",     "TEXT")
        _add_column_if_missing(con, "companies", "zi_industry",          "TEXT")
        _add_column_if_missing(con, "companies", "loc_signal_strength",  "TEXT DEFAULT 'noise'")
        _add_column_if_missing(con, "companies", "expansion_confirmed",  "TEXT")
        _add_column_if_missing(con, "companies", "expansion_possible",   "TEXT")
        _add_column_if_missing(con, "companies", "existing_confirmed",   "TEXT")
        _add_column_if_missing(con, "companies", "existing_possible",    "TEXT")
        _add_column_if_missing(con, "companies", "location_jd_count",    "INTEGER DEFAULT 0")
        _add_column_if_missing(con, "companies", "location_detail",      "TEXT")
        _add_column_if_missing(con, "companies", "known_markets",        "TEXT")
        _add_column_if_missing(con, "companies", "office_cities",        "TEXT")
        con.executescript("""

        CREATE TABLE IF NOT EXISTS ats_cache (
            domain      TEXT PRIMARY KEY,
            ats_type    TEXT NOT NULL,
            ats_slug    TEXT NOT NULL,
            checked_at  TEXT NOT NULL
        );

        -- One row per (company, market): queryable location signal table.
        -- Replaces the JSON blob in companies.location_detail for anything
        -- requiring a real query (e.g. "all companies with confirmed Chicago office").
        CREATE TABLE IF NOT EXISTS company_locations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name    TEXT NOT NULL,
            market          TEXT NOT NULL,
            jd_count        INTEGER DEFAULT 0,
            signal_strength TEXT DEFAULT 'noise',
            max_kw_score    INTEGER DEFAULT 0,
            last_seen       TEXT NOT NULL,
            UNIQUE(company_name, market)
        );

        CREATE INDEX IF NOT EXISTS idx_cloc_market
            ON company_locations(market, signal_strength);

        CREATE TABLE IF NOT EXISTS runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at      TEXT NOT NULL,
            raw_count   INTEGER,
            live_count  INTEGER,
            new_count   INTEGER
        );
        """)
    cleanup_stale()


def upsert_companies(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Insert or update company records.

    Returns:
        (new_companies, updated_companies)
        new_companies  = companies appearing for the first time today
        updated_companies = companies seen before, now refreshed
    """
    today = date.today().isoformat()
    new_cos, updated_cos = [], []

    with _conn() as con:
        for r in records:
            name = r.get("company", "").strip()
            if not name:
                continue

            existing = con.execute(
                "SELECT * FROM companies WHERE name = ?", (name,)
            ).fetchone()

            if existing is None:
                con.execute("""
                    INSERT INTO companies
                        (name, domain, first_seen, last_seen, times_seen, is_new,
                         gtm_score, top_keywords, role_count, sample_title,
                         sample_url, location, perk_excerpt, source, notified,
                         segment, market, ezcater_vertical, zi_industry,
                         loc_signal_strength, expansion_confirmed, expansion_possible,
                         existing_confirmed, existing_possible,
                         location_jd_count, location_detail, known_markets, office_cities)
                    VALUES (?,?,?,?,1,1,?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    name, r.get("inferred_domain", ""), today, today,
                    r.get("gtm_score", 0), r.get("top_keywords", r.get("food_keywords_matched", "")),
                    r.get("role_count", 1),
                    r.get("sample_title", r.get("title", "")),
                    r.get("sample_url", r.get("url", "")),
                    r.get("location", ""), r.get("perk_excerpt", ""),
                    r.get("best_source", r.get("source", "")), r.get("segment", "prospect"),
                    r.get("market", ""), r.get("ezcater_vertical", ""),
                    r.get("zi_industry", ""), r.get("loc_signal_strength", "noise"),
                    r.get("expansion_confirmed", ""), r.get("expansion_possible", ""),
                    r.get("existing_confirmed", ""), r.get("existing_possible", ""),
                    r.get("location_jd_count", 0), r.get("location_detail", "[]"),
                    r.get("known_markets", ""), r.get("office_cities", "[]"),
                ))
                new_cos.append(r)
            else:
                con.execute("""
                    UPDATE companies SET
                        last_seen           = ?, times_seen = times_seen + 1, is_new = 0,
                        gtm_score           = MAX(gtm_score, ?),
                        top_keywords        = ?, role_count = ?,
                        sample_title        = ?, sample_url = ?,
                        perk_excerpt        = ?, source     = ?,
                        segment             = ?, market     = ?,
                        ezcater_vertical    = ?, zi_industry = ?,
                        loc_signal_strength = ?,
                        expansion_confirmed = ?, expansion_possible = ?,
                        existing_confirmed  = ?, existing_possible  = ?,
                        location_jd_count   = ?, location_detail    = ?,
                        known_markets       = ?, office_cities       = ?
                    WHERE name = ?
                """, (
                    today, r.get("gtm_score", 0), r.get("top_keywords", r.get("food_keywords_matched", "")),
                    r.get("role_count", 1),
                    r.get("sample_title", r.get("title", "")),
                    r.get("sample_url", r.get("url", "")),
                    r.get("perk_excerpt", ""), r.get("best_source", r.get("source", "")),
                    r.get("segment", "prospect"), r.get("market", ""),
                    r.get("ezcater_vertical", ""), r.get("zi_industry", ""),
                    r.get("loc_signal_strength", "noise"),
                    r.get("expansion_confirmed", ""), r.get("expansion_possible", ""),
                    r.get("existing_confirmed", ""), r.get("existing_possible", ""),
                    r.get("location_jd_count", 0), r.get("location_detail", "[]"),
                    r.get("known_markets", ""), r.get("office_cities", "[]"),
                    name,
                ))
                updated_cos.append(r)

    return new_cos, updated_cos


def record_velocity(companies: list[dict]):
    """
    Record weekly signal counts per company for velocity tracking.
    week format: YYYY-WW (ISO week number).
    """
    from datetime import date
    today = date.today()
    week  = f"{today.isocalendar()[0]}-{today.isocalendar()[1]:02d}"
    with _conn() as con:
        for c in companies:
            name  = c.get("company", "").strip()
            count = c.get("role_count", 1)
            kws   = c.get("top_keywords", "")
            if not name:
                continue
            con.execute("""
                INSERT INTO company_velocity (company_name, week, signal_count, keyword_set)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(company_name, week) DO UPDATE SET
                    signal_count = signal_count + excluded.signal_count,
                    keyword_set  = excluded.keyword_set
            """, (name, week, count, kws))


def get_velocity(company_name: str, weeks: int = 4) -> list[dict]:
    """Return last N weeks of velocity data for a company."""
    with _conn() as con:
        rows = con.execute("""
            SELECT week, signal_count, keyword_set
            FROM company_velocity
            WHERE company_name = ?
            ORDER BY week DESC
            LIMIT ?
        """, (company_name, weeks)).fetchall()
    return [dict(r) for r in rows]


def get_accelerating_companies(min_delta: int = 2) -> list[dict]:
    """
    Return companies whose signal count increased week-over-week by at least min_delta.
    This is the Quant signal: rate of change > presence.
    """
    with _conn() as con:
        rows = con.execute("""
            SELECT
                a.company_name,
                a.week          AS this_week,
                a.signal_count  AS this_count,
                b.week          AS prev_week,
                b.signal_count  AS prev_count,
                (a.signal_count - b.signal_count) AS delta
            FROM company_velocity a
            JOIN company_velocity b
              ON a.company_name = b.company_name
            WHERE a.week = (SELECT MAX(week) FROM company_velocity WHERE company_name = a.company_name)
              AND b.week = (
                  SELECT MAX(week) FROM company_velocity
                  WHERE company_name = a.company_name AND week < a.week
              )
              AND (a.signal_count - b.signal_count) >= ?
            ORDER BY delta DESC
        """, (min_delta,)).fetchall()
    return [dict(r) for r in rows]


def log_run(raw_count: int, live_count: int, new_count: int):
    today = date.today().isoformat()
    with _conn() as con:
        con.execute(
            "INSERT INTO runs (ran_at, raw_count, live_count, new_count) VALUES (?,?,?,?)",
            (today, raw_count, live_count, new_count),
        )


def mark_notified(company_names: list[str]):
    with _conn() as con:
        con.executemany(
            "UPDATE companies SET notified = 1 WHERE name = ?",
            [(n,) for n in company_names],
        )


def get_all_companies(order_by: str = "gtm_score DESC") -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            f"SELECT *, name AS company FROM companies ORDER BY {order_by}"
        ).fetchall()
        return [dict(r) for r in rows]


def get_new_unnotified() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM companies WHERE is_new=1 AND notified=0 ORDER BY gtm_score DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def upsert_company_locations(records: list[dict]):
    """
    Persist per-office location signals (one row per company+market).
    Replaces the JSON blob approach — fully queryable.
    records come from locations_df in enrich.rollup_to_companies().
    """
    today = date.today().isoformat()
    with _conn() as con:
        for r in records:
            company = r.get("company_norm") or r.get("company_name", "")
            market  = r.get("market", "")
            if not company or not market:
                continue
            con.execute("""
                INSERT INTO company_locations
                    (company_name, market, jd_count, signal_strength, max_kw_score, last_seen)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_name, market) DO UPDATE SET
                    jd_count        = excluded.jd_count,
                    signal_strength = excluded.signal_strength,
                    max_kw_score    = excluded.max_kw_score,
                    last_seen       = excluded.last_seen
            """, (
                company,
                market,
                r.get("jd_count", 0),
                r.get("signal_strength", "noise"),
                r.get("max_kw_score", 0),
                today,
            ))


def get_confirmed_offices(market: str | None = None, min_strength: str = "confirmed") -> list[dict]:
    """
    Query company_locations directly — e.g. 'show me all confirmed Chicago offices'.
    min_strength: 'confirmed' returns only confirmed; 'possible' returns both.
    """
    strength_filter = ("confirmed",) if min_strength == "confirmed" else ("confirmed", "possible")
    placeholders = ",".join("?" * len(strength_filter))
    query = f"""
        SELECT cl.company_name, cl.market, cl.jd_count, cl.signal_strength, cl.max_kw_score,
               c.gtm_score, c.segment, c.top_keywords, c.sample_url
        FROM company_locations cl
        LEFT JOIN companies c ON c.name = cl.company_name
        WHERE cl.signal_strength IN ({placeholders})
    """
    params = list(strength_filter)
    if market:
        query += " AND cl.market = ?"
        params.append(market)
    query += " ORDER BY cl.jd_count DESC, c.gtm_score DESC"
    with _conn() as con:
        rows = con.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def cleanup_stale(ats_cache_days: int = 90, velocity_weeks: int = 52):
    """
    Remove expired ATS cache entries and old velocity records.
    ats_cache_days: entries older than this are re-detected on next lookup
    velocity_weeks: velocity rows older than this are pruned (keeps ~1 year)
    """
    with _conn() as con:
        ats_deleted = con.execute(
            "DELETE FROM ats_cache WHERE checked_at < date('now', ?)",
            (f"-{ats_cache_days} days",)
        ).rowcount
        # ISO week arithmetic: drop weeks older than velocity_weeks ago
        velocity_deleted = con.execute("""
            DELETE FROM company_velocity
            WHERE week < strftime('%Y-%W', date('now', ?))
        """, (f"-{velocity_weeks * 7} days",)).rowcount
    return {"ats_cache_deleted": ats_deleted, "velocity_rows_deleted": velocity_deleted}


def get_stats() -> dict:
    with _conn() as con:
        total = con.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        new_today = con.execute(
            "SELECT COUNT(*) FROM companies WHERE first_seen = ?",
            (date.today().isoformat(),)
        ).fetchone()[0]
        runs = con.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        last_run = con.execute(
            "SELECT ran_at, new_count FROM runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return {
            "total_companies": total,
            "new_today": new_today,
            "total_runs": runs,
            "last_run_date": last_run["ran_at"] if last_run else None,
            "last_run_new": last_run["new_count"] if last_run else 0,
        }
