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
        """)
        # Non-destructive migrations for existing databases
        _add_column_if_missing(con, "companies", "segment",          "TEXT DEFAULT 'prospect'")
        _add_column_if_missing(con, "companies", "market",           "TEXT")
        _add_column_if_missing(con, "companies", "ezcater_vertical", "TEXT")
        _add_column_if_missing(con, "companies", "zi_industry",      "TEXT")
        con.executescript("""

        CREATE TABLE IF NOT EXISTS runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at      TEXT NOT NULL,
            raw_count   INTEGER,
            live_count  INTEGER,
            new_count   INTEGER
        );
        """)


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
                         segment, market, ezcater_vertical, zi_industry)
                    VALUES (?,?,?,?,1,1,?,?,?,?,?,?,?,?,0,?,?,?,?)
                """, (
                    name,
                    r.get("inferred_domain", ""),
                    today, today,
                    r.get("gtm_score", 0),
                    r.get("food_keywords_matched", ""),
                    r.get("role_count", 1),
                    r.get("sample_title", r.get("title", "")),
                    r.get("sample_url", r.get("url", "")),
                    r.get("location", ""),
                    r.get("perk_excerpt", ""),
                    r.get("source", ""),
                    r.get("segment", "prospect"),
                    r.get("market", ""),
                    r.get("ezcater_vertical", ""),
                    r.get("zi_industry", ""),
                ))
                new_cos.append(r)
            else:
                # Update: refresh score/keywords, bump times_seen, clear is_new
                con.execute("""
                    UPDATE companies SET
                        last_seen        = ?,
                        times_seen       = times_seen + 1,
                        is_new           = 0,
                        gtm_score        = MAX(gtm_score, ?),
                        top_keywords     = ?,
                        role_count       = ?,
                        sample_title     = ?,
                        sample_url       = ?,
                        perk_excerpt     = ?,
                        source           = ?,
                        segment          = ?,
                        market           = ?,
                        ezcater_vertical = ?,
                        zi_industry      = ?
                    WHERE name = ?
                """, (
                    today,
                    r.get("gtm_score", 0),
                    r.get("food_keywords_matched", ""),
                    r.get("role_count", 1),
                    r.get("sample_title", r.get("title", "")),
                    r.get("sample_url", r.get("url", "")),
                    r.get("perk_excerpt", ""),
                    r.get("source", ""),
                    r.get("segment", "prospect"),
                    r.get("market", ""),
                    r.get("ezcater_vertical", ""),
                    r.get("zi_industry", ""),
                    name,
                ))
                updated_cos.append(r)

    return new_cos, updated_cos


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
            f"SELECT * FROM companies ORDER BY {order_by}"
        ).fetchall()
        return [dict(r) for r in rows]


def get_new_unnotified() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM companies WHERE is_new=1 AND notified=0 ORDER BY gtm_score DESC"
        ).fetchall()
        return [dict(r) for r in rows]


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
