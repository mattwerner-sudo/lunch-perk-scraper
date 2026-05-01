"""
Slack alerting via Incoming Webhooks.

Two webhook URLs supported (both optional):
  SLACK_WEBHOOK_URL          → prospects channel (net-new companies)
  SLACK_WEBHOOK_MANAGED_URL  → managed accounts channel (rep-owned accounts)

Set in .env. Get webhooks at: https://api.slack.com/messaging/webhooks
"""
import os
import json
import logging
import requests
from datetime import date
from dotenv import load_dotenv
import db
import location_lookup

load_dotenv()
log = logging.getLogger(__name__)

WEBHOOK_URL           = os.getenv("SLACK_WEBHOOK_URL", "")
WEBHOOK_MANAGED_URL   = os.getenv("SLACK_WEBHOOK_MANAGED_URL", WEBHOOK_URL)
WEBHOOK_UNMANAGED_URL = os.getenv("SLACK_WEBHOOK_UNMANAGED_URL", WEBHOOK_URL)


def _load_territories() -> dict[str, dict]:
    """Load market → rep/webhook mapping from territories.csv."""
    import csv
    from pathlib import Path
    path = Path(__file__).parent / "territories.csv"
    if not path.exists():
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {row["market"]: row for row in csv.DictReader(f)}


def _territory_webhook(market: str, segment: str) -> str:
    """
    Resolve the right webhook for a market + segment combo.
    Priority: territory-specific env var → segment default → global fallback.
    """
    territories = _load_territories()
    row = territories.get(market) or territories.get("Other", {})
    env_var = row.get("webhook_env_var", "")
    territory_webhook = os.getenv(env_var, "") if env_var else ""

    if territory_webhook:
        return territory_webhook
    if segment == "managed":
        return WEBHOOK_MANAGED_URL
    if segment == "unmanaged":
        return WEBHOOK_UNMANAGED_URL
    return WEBHOOK_URL


def _rep_tag(market: str) -> str:
    """Return '@rep_name ' prefix if a rep is assigned to this market."""
    territories = _load_territories()
    row = territories.get(market) or territories.get("Other", {})
    handle = row.get("slack_handle", "").strip()
    rep    = row.get("rep_name", "").strip()
    if handle:
        return f"<@{handle}> "
    if rep and rep.lower() != "unassigned":
        return f"@{rep} "
    return ""

SOURCE_EMOJI = {
    "Glassdoor Benefits":           ":star: Glassdoor (employee-verified)",
    "Built In NYC (Company Perk)":  ":star: Built In (perk badge)",
    "Built In NYC":                 ":large_blue_circle: Built In",
    "Greenhouse":                   ":large_blue_circle: Greenhouse",
    "Lever":                        ":large_blue_circle: Lever",
    "Ashby":                        ":large_blue_circle: Ashby",
    "Workday":                      ":white_circle: Workday",
}


def _company_block(co: dict) -> dict:
    score      = co.get("gtm_score", 0)
    source     = co.get("best_source", co.get("source", ""))
    src_label  = SOURCE_EMOJI.get(source, f":white_circle: {source}")
    keywords   = co.get("top_keywords", co.get("food_keywords_matched", ""))
    url        = co.get("sample_url", co.get("url", ""))
    role_count = co.get("role_count", 1)
    domain     = co.get("inferred_domain", "")
    # Billing address is authoritative for territory routing
    market     = location_lookup.get_primary_market(domain) if domain else ""
    if not market or market == "Other":
        market = co.get("market", "")
    segment    = co.get("segment", "prospect")
    vertical   = co.get("ezcater_vertical", "")
    perk       = co.get("perk_excerpt", "")[:140]

    # Velocity: check if signal count is accelerating week-over-week
    velocity = db.get_velocity(co.get("company", ""), weeks=2)
    accelerating = (
        len(velocity) >= 2 and
        velocity[0]["signal_count"] > velocity[1]["signal_count"]
    )
    seg_tag      = ":rotating_light: *MANAGED ACCT*" if segment == "managed" else ""
    velocity_tag = "  :fire: *Accelerating*" if accelerating else ""
    location_tag = f" :round_pushpin: {market}" if market and market != "Other" else ""
    vertical_tag = f"  `{vertical}`" if vertical else ""
    role_label = f"{role_count} role{'s' if role_count != 1 else ''} mentioning food perks"

    text = (
        f"{seg_tag}{'  ' if seg_tag else ''}*<{url}|{co['company']}>*"
        f"  `score: {score}`{location_tag}{vertical_tag}{velocity_tag}\n"
        f"{src_label}\n"
        f":pushpin: _{role_label}_ — `{keywords}`\n"
    )
    if perk:
        text += f'> "{perk}"\n'

    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _post(webhook: str, payload: dict) -> bool:
    try:
        resp = requests.post(
            webhook,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200 and resp.text == "ok":
            return True
        log.error(f"Slack error: {resp.status_code} — {resp.text}")
        return False
    except Exception as e:
        log.error(f"Slack request failed: {e}")
        return False


def send_new_companies_alert(new_companies: list[dict], stats: dict) -> bool:
    """
    Post Slack alerts for net-new companies with food perks.
    Managed accounts go to SLACK_WEBHOOK_MANAGED_URL; prospects to SLACK_WEBHOOK_URL.
    Returns True if at least one message sent successfully.
    """
    if not new_companies:
        log.info("No new companies to notify about")
        return False

    from collections import defaultdict
    managed   = [c for c in new_companies if c.get("segment") == "managed"]
    unmanaged = [c for c in new_companies if c.get("segment") == "unmanaged"]
    prospects = [c for c in new_companies if c.get("segment") == "prospect"]
    today     = date.today().strftime("%B %-d, %Y")
    total     = stats.get("total_companies", 0)
    sent      = False

    # Group by market for territory routing
    def _group_by_market(companies):
        groups = defaultdict(list)
        for c in companies:
            groups[c.get("market", "Other") or "Other"].append(c)
        return groups

    # ── Managed account alert — routed by territory ───────────────────────────
    if managed:
        for market, cos in _group_by_market(managed).items():
            webhook = _territory_webhook(market, "managed")
            if not webhook:
                continue
            rep_tag = _rep_tag(market)
            blocks = [
                {"type": "header", "text": {"type": "plain_text",
                    "text": f":briefcase: {len(cos)} Managed Account{'s' if len(cos) != 1 else ''} — {market}", "emoji": True}},
                {"type": "context", "elements": [{"type": "mrkdwn",
                    "text": f"{rep_tag}{today}  •  Rep-owned accounts with active food perk hiring activity"}]},
                {"type": "divider"},
            ]
            for co in cos[:10]:
                blocks.append(_company_block(co))
            if len(cos) > 10:
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": f"_...and {len(cos) - 10} more managed accounts in {market}._"}})
            blocks += [{"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn",
                    "text": ":bulb: These rep-owned accounts are actively hiring roles that mention food perks."}]}]
            payload = {"text": f":briefcase: {len(cos)} managed accounts with food perk signal — {market}", "blocks": blocks}
            if _post(webhook, payload):
                log.info(f"Slack: sent {len(cos)} managed alerts for {market}")
                sent = True

    # ── Unmanaged account alert — routed by territory ─────────────────────────
    if unmanaged:
        for market, cos in _group_by_market(unmanaged).items():
            webhook = _territory_webhook(market, "unmanaged")
            if not webhook:
                continue
            rep_tag = _rep_tag(market)
            blocks = [
                {"type": "header", "text": {"type": "plain_text",
                    "text": f":dart: {len(cos)} Unmanaged Account{'s' if len(cos) != 1 else ''} — {market}", "emoji": True}},
                {"type": "context", "elements": [{"type": "mrkdwn",
                    "text": f"{rep_tag}{today}  •  Unmanaged accounts with active food perk hiring activity"}]},
                {"type": "divider"},
            ]
            for co in cos[:10]:
                blocks.append(_company_block(co))
            if len(cos) > 10:
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": f"_...and {len(cos) - 10} more unmanaged accounts in {market}._"}})
            blocks += [{"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn",
                    "text": ":bulb: These unmanaged accounts are actively hiring roles that mention food perks."}]}]
            payload = {"text": f":dart: {len(cos)} unmanaged accounts with food perk signal — {market}", "blocks": blocks}
            if _post(webhook, payload):
                log.info(f"Slack: sent {len(cos)} unmanaged alerts for {market}")
                sent = True

    # ── Net-new prospect alert ────────────────────────────────────────────────
    if prospects and WEBHOOK_URL:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":fork_and_knife: {len(prospects)} New Lunch Perk Prospects",
                    "emoji": True,
                },
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"{today}  •  {total} total companies in database"}],
            },
            {"type": "divider"},
        ]
        for co in prospects[:10]:
            blocks.append(_company_block(co))
        if len(prospects) > 10:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"_...and {len(prospects) - 10} more. Open the dashboard to see all._"}})
        blocks += [
            {"type": "divider"},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": ":bulb: These companies mention food perks (DoorDash, GrubHub, free lunch, catered meals, etc.) in job postings. All listings verified live as of today."}]},
        ]
        payload = {"text": f":fork_and_knife: {len(prospects)} new lunch perk prospects found", "blocks": blocks}
        if _post(WEBHOOK_URL, payload):
            log.info(f"Slack: sent {len(prospects)} prospect alerts")
            sent = True

    return sent
