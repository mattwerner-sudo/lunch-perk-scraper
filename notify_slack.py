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

load_dotenv()
log = logging.getLogger(__name__)

WEBHOOK_URL           = os.getenv("SLACK_WEBHOOK_URL", "")
WEBHOOK_MANAGED_URL   = os.getenv("SLACK_WEBHOOK_MANAGED_URL", WEBHOOK_URL)
WEBHOOK_UNMANAGED_URL = os.getenv("SLACK_WEBHOOK_UNMANAGED_URL", WEBHOOK_URL)

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
    market     = co.get("market", "")
    segment    = co.get("segment", "prospect")
    vertical   = co.get("ezcater_vertical", "")
    perk       = co.get("perk_excerpt", "")[:140]

    seg_tag = ":rotating_light: *MANAGED ACCT*" if segment == "managed" else ""
    location_tag = f" :round_pushpin: {market}" if market and market != "Other" else ""
    vertical_tag = f"  `{vertical}`" if vertical else ""
    role_label = f"{role_count} role{'s' if role_count != 1 else ''} mentioning food perks"

    text = (
        f"{seg_tag}{'  ' if seg_tag else ''}*<{url}|{co['company']}>*"
        f"  `score: {score}`{location_tag}{vertical_tag}\n"
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

    managed   = [c for c in new_companies if c.get("segment") == "managed"]
    unmanaged = [c for c in new_companies if c.get("segment") == "unmanaged"]
    prospects = [c for c in new_companies if c.get("segment") == "prospect"]
    today     = date.today().strftime("%B %-d, %Y")
    total     = stats.get("total_companies", 0)
    sent      = False

    # ── Managed account alert ─────────────────────────────────────────────────
    if managed and WEBHOOK_MANAGED_URL:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":briefcase: {len(managed)} Managed Account{'s' if len(managed) != 1 else ''} with Food Perk Signal",
                    "emoji": True,
                },
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"{today}  •  Rep-owned accounts with active food perk hiring activity"}],
            },
            {"type": "divider"},
        ]
        for co in managed[:10]:
            blocks.append(_company_block(co))
        if len(managed) > 10:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"_...and {len(managed) - 10} more managed accounts._"}})
        blocks += [
            {"type": "divider"},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": ":bulb: These rep-owned accounts are actively hiring roles that mention food perks."}]},
        ]
        payload = {"text": f":briefcase: {len(managed)} managed accounts with food perk signal", "blocks": blocks}
        if _post(WEBHOOK_MANAGED_URL, payload):
            log.info(f"Slack: sent {len(managed)} managed expansion alerts")
            sent = True

    # ── Unmanaged account alert ───────────────────────────────────────────────
    if unmanaged and WEBHOOK_UNMANAGED_URL:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":dart: {len(unmanaged)} Unmanaged Account{'s' if len(unmanaged) != 1 else ''} with Food Perk Signal",
                    "emoji": True,
                },
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"{today}  •  Unmanaged accounts (no assigned rep) with active food perk hiring activity"}],
            },
            {"type": "divider"},
        ]
        for co in unmanaged[:10]:
            blocks.append(_company_block(co))
        if len(unmanaged) > 10:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"_...and {len(unmanaged) - 10} more unmanaged accounts._"}})
        blocks += [
            {"type": "divider"},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": ":bulb: These unmanaged accounts are actively hiring roles that mention food perks."}]},
        ]
        payload = {"text": f":dart: {len(unmanaged)} unmanaged accounts with food perk signal", "blocks": blocks}
        if _post(WEBHOOK_UNMANAGED_URL, payload):
            log.info(f"Slack: sent {len(unmanaged)} unmanaged target alerts")
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
