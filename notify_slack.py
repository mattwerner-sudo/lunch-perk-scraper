"""
Slack alerting via Incoming Webhooks.

Set your webhook URL in .env:
  SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../...

Get a webhook at: https://api.slack.com/messaging/webhooks
"""
import os
import json
import logging
import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# Confidence tier labels (based on source)
SOURCE_EMOJI = {
    "Glassdoor Benefits": ":star: Glassdoor (employee-verified)",
    "Built In NYC (Company Perk)": ":star: Built In NYC (perk badge)",
    "Built In NYC": ":large_blue_circle: Built In NYC",
    "Greenhouse": ":large_blue_circle: Greenhouse",
    "Lever": ":large_blue_circle: Lever",
    "Ashby": ":large_blue_circle: Ashby",
    "Workday": ":white_circle: Workday",
}


def send_new_companies_alert(new_companies: list[dict], stats: dict) -> bool:
    """
    Post a Slack alert for net-new companies with food perks.

    Returns True if message was sent successfully.
    """
    if not WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return False

    if not new_companies:
        log.info("No new companies to notify about")
        return False

    today = date.today().strftime("%B %-d, %Y")
    total = stats.get("total_companies", 0)

    # Build Block Kit payload
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":fork_and_knife: {len(new_companies)} New NYC Lunch Perk Companies",
                "emoji": True,
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"{today}  •  {total} total companies in database",
                }
            ],
        },
        {"type": "divider"},
    ]

    # Add a section per company (up to 10; link to full list for rest)
    shown = new_companies[:10]
    for co in shown:
        score = co.get("gtm_score", 0)
        source = co.get("source", "")
        source_label = SOURCE_EMOJI.get(source, f":white_circle: {source}")
        keywords = co.get("top_keywords", co.get("food_keywords_matched", ""))
        url = co.get("sample_url", co.get("url", ""))
        title = co.get("sample_title", co.get("title", "Open role"))
        excerpt = co.get("perk_excerpt", "")[:140]
        role_count = co.get("role_count", 1)

        role_label = f"{role_count} role{'s' if role_count != 1 else ''} mentioning food perks"

        text = (
            f"*<{url}|{co['name']}>*  `score: {score}`\n"
            f"{source_label}\n"
            f":pushpin: _{role_label}_ — `{keywords}`\n"
        )
        if excerpt:
            text += f'> "{excerpt}"\n'

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": text},
        })

    if len(new_companies) > 10:
        remaining = len(new_companies) - 10
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"_...and {remaining} more. Open the dashboard to see all._",
            },
        })

    blocks += [
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        ":bulb: These companies mention food perks (DoorDash, GrubHub, "
                        "free lunch, catered meals, etc.) in NYC job postings. "
                        "All listings verified live as of today."
                    ),
                }
            ],
        },
    ]

    payload = {
        "text": f":fork_and_knife: {len(new_companies)} new NYC lunch perk companies found",
        "blocks": blocks,
    }

    try:
        resp = requests.post(
            WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200 and resp.text == "ok":
            log.info(f"Slack: notified about {len(new_companies)} new companies")
            return True
        else:
            log.error(f"Slack error: {resp.status_code} — {resp.text}")
            return False
    except Exception as e:
        log.error(f"Slack request failed: {e}")
        return False
