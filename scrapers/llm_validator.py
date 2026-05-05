"""
LLM-powered post-processing for enriched company output.

Uses claude-haiku-4-5 to validate each enriched company and filter out:
  - Catering vendors / food delivery platforms (they mention food but sell it, don't buy it)
  - Blog posts / content pages / aggregator sites
  - Companies with clearly non-US headquarters (not caught by regex geo filter)
  - False positives where keyword match is incidental (e.g. "DoorDash" in a job requirement)

Cost: ~$0.02/run on 200-300 companies.
"""
import os
import json
import logging
from typing import Any

log = logging.getLogger(__name__)

BATCH_SIZE = 20  # companies per API call

SYSTEM_PROMPT = """You are a data quality filter for a B2B sales prospecting tool.
Your job: given a list of companies found via job-posting analysis, classify each one.

For each company you will receive: name, inferred_domain, location, top_keywords, sample_title, perk_excerpt, segment.

Classify each company as one of:
- "keep"   : A real US-based employer offering food perks (free lunch, catered meals, meal stipends, food delivery services like DoorDash/GrubHub/etc.) to their employees.
- "vendor" : A food/catering company itself (they sell food/catering — DoorDash, GrubHub, ezCater, Fooda, ZeroCater, caterers, restaurants, meal kit companies, food brands).
- "content": A blog post, news article, job board, aggregator site, or any non-employer page.
- "non_us" : A company that appears to be headquartered outside the US with no US operations evident.

Rules:
- Food delivery platforms (DoorDash, GrubHub, Uber Eats, Seamless, Caviar, Forkable, Sharebite) are always "vendor" — they ARE the perk, not a buyer of it.
- If a company has segment "managed" or "unmanaged" they are known ezCater accounts — classify conservatively. Only mark "vendor" if they are clearly a food company. Never mark "content".
- When in doubt, classify as "keep".

Respond ONLY with a JSON array, one object per company, in the same order received:
[{"name": "Company Name", "verdict": "keep|vendor|content|non_us", "reason": "one sentence"}]"""


def validate_companies(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Run LLM validation on a list of enriched company dicts.
    Returns the filtered list (only "keep" verdicts).
    Skips validation if ANTHROPIC_API_KEY is not set.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("LLM validator: ANTHROPIC_API_KEY not set — skipping validation")
        return companies

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        log.warning(f"LLM validator: failed to init Anthropic client: {e}")
        return companies

    kept = []
    dropped = []
    total = len(companies)

    for batch_start in range(0, total, BATCH_SIZE):
        batch = companies[batch_start:batch_start + BATCH_SIZE]
        batch_input = [
            {
                "name":          c.get("company", ""),
                "inferred_domain": c.get("inferred_domain", ""),
                "location":      c.get("location", ""),
                "top_keywords":  c.get("top_keywords", ""),
                "sample_title":  c.get("sample_title", ""),
                "perk_excerpt":  (c.get("perk_excerpt", "") or "")[:300],
                "segment":       c.get("segment", "prospect"),
            }
            for c in batch
        ]

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2048,
                system=SYSTEM_PROMPT,
                messages=[{
                    "role": "user",
                    "content": json.dumps(batch_input, ensure_ascii=False),
                }],
            )
            raw = response.content[0].text.strip()

            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = "\n".join(raw.split("\n")[1:])
                raw = raw.rsplit("```", 1)[0].strip()

            verdicts = json.loads(raw)
            if not isinstance(verdicts, list) or len(verdicts) != len(batch):
                log.warning(
                    f"LLM validator: unexpected response length ({len(verdicts)} vs {len(batch)}), keeping batch"
                )
                kept.extend(batch)
                continue

            for company, verdict_obj in zip(batch, verdicts):
                verdict = verdict_obj.get("verdict", "keep")
                reason  = verdict_obj.get("reason", "")
                if verdict == "keep":
                    kept.append(company)
                else:
                    dropped.append({**company, "_llm_verdict": verdict, "_llm_reason": reason})
                    log.info(
                        f"LLM validator: DROP [{verdict}] {company.get('company','')} — {reason}"
                    )

        except json.JSONDecodeError as e:
            log.warning(f"LLM validator: JSON parse error in batch {batch_start}: {e} — keeping batch")
            kept.extend(batch)
        except Exception as e:
            log.warning(f"LLM validator: API error in batch {batch_start}: {e} — keeping batch")
            kept.extend(batch)

    log.info(
        f"LLM validator: {total} in → {len(kept)} kept, {len(dropped)} dropped "
        f"({len([d for d in dropped if d['_llm_verdict']=='vendor'])} vendor, "
        f"{len([d for d in dropped if d['_llm_verdict']=='content'])} content, "
        f"{len([d for d in dropped if d['_llm_verdict']=='non_us'])} non_us)"
    )

    return kept
