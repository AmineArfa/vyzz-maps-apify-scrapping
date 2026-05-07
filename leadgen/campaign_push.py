"""Push a filtered batch of raw.scraped_leads rows into an Instantly campaign.

This is the heart of the campaign composer flow. The contract is:

- For each lead with `instantly_lead_id` already set: MOVE it into the target
  campaign via Instantly's move endpoint. The id stays the same on both sides
  — no orphan in the old campaign, no duplicate in the new one.
- For each lead without `instantly_lead_id`: CREATE it in Instantly, then
  immediately write the returned id back to `raw.scraped_leads` so the row
  is always linked. The write-back happens inside the same loop iteration,
  not as a separate batch — even if the loop is interrupted halfway through
  we never end up with rows in Instantly that have no link in raw.

The push is per-lead. We could batch the create path with `/leads/add`, but
matching returned ids back to raw rows by email is fragile (case, trims,
duplicates) and the savings aren't worth losing the strict create-then-
writeback ordering. Instantly's per-call rate is fine for the scale we
operate at; if it ever isn't, parallelize via a ThreadPoolExecutor.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .instantly import (
    export_leads_to_instantly,
    inject_lid_to_lead,
    is_valid_uuid,
    move_lead_to_campaign,
    search_lead_by_email,
)
from .ticket_tier import TIERS


def _normalize_email(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _process_one(
    lead: dict,
    *,
    api_key: str,
    campaign_id: str,
    debug: bool,
) -> dict:
    """Process a single lead. Returns a result dict with op + status fields.

    Operations: 'moved' | 'created' | 'skipped' | 'failed' | 'already_in_place'.

    Side effect: when a lead is created, the returned `instantly_lead_id`
    field is set so the caller can write it back to raw.scraped_leads.
    """
    raw_id = lead.get("id")
    email = _normalize_email(lead.get("key_contact_email"))
    instantly_lead_id = lead.get("instantly_lead_id")
    current_campaign_id = lead.get("instantly_campaign_id")

    base = {
        "id": raw_id,
        "email": email,
        "company_name": lead.get("company_name"),
        "industry": lead.get("industry"),
        "ticket_tier": lead.get("ticket_tier"),
        "instantly_lead_id": instantly_lead_id,
    }

    # ── Already in target ────────────────────────────────────────────────
    # Defensive: callers should already filter these out at SQL level (see
    # build_where's `exclude_already_in_campaign_id`), but skip them here
    # too so the recategorize loop is safe to retry without burning API
    # calls on leads that are already where they should be.
    if (
        instantly_lead_id and is_valid_uuid(instantly_lead_id)
        and current_campaign_id == campaign_id
    ):
        return {
            **base, "op": "already_in_place",
            "instantly_lead_id": instantly_lead_id, "error": None,
        }

    # ── Move path ────────────────────────────────────────────────────────
    if instantly_lead_id and is_valid_uuid(instantly_lead_id):
        ok, err = move_lead_to_campaign(api_key, instantly_lead_id, campaign_id, debug=debug)
        if ok:
            return {**base, "op": "moved", "instantly_lead_id": instantly_lead_id, "error": None}
        return {**base, "op": "failed", "error": f"move failed: {err}"}

    # ── Create path ──────────────────────────────────────────────────────
    if not email:
        return {**base, "op": "skipped", "error": "no email"}

    cnt, created, _, err = export_leads_to_instantly(
        api_key, campaign_id, [lead], debug=debug,
    )
    if cnt > 0 and created:
        new_id = created[0].get("id")
        if new_id and is_valid_uuid(new_id):
            inject_lid_to_lead(api_key, new_id, debug=debug)
            return {**base, "op": "created", "instantly_lead_id": new_id, "error": None}
        return {**base, "op": "failed", "error": "create returned no id"}

    # Create returned 0 — likely already exists in Instantly under this
    # email but in a different campaign (so our raw row didn't have an id).
    # Look it up and move it in instead. This keeps us at one Instantly
    # lead per email, never duplicates.
    found, _ = search_lead_by_email(api_key, email, debug=debug)
    if found and found.get("id") and is_valid_uuid(found["id"]):
        found_id = found["id"]
        ok, move_err = move_lead_to_campaign(api_key, found_id, campaign_id, debug=debug)
        if ok:
            return {**base, "op": "moved", "instantly_lead_id": found_id, "error": None}
        return {**base, "op": "failed", "error": f"create=0, move-after-search failed: {move_err}"}

    if err:
        return {**base, "op": "failed", "error": f"create failed: {err}"}
    return {**base, "op": "failed", "error": "create returned 0 leads, search found nothing"}


def push_leads_to_campaign(
    backend,
    *,
    api_key: str,
    leads: list[dict],
    campaign_id: str,
    debug: bool = False,
    max_workers: int = 5,
) -> dict:
    """Push `leads` into Instantly `campaign_id` and write back the resulting ids.

    Returns:
        {
            "moved": int,
            "created": int,
            "skipped": int,
            "failed": int,
            "details": [ <per-lead result dict>, ... ],
        }
    """
    if not leads:
        return {"moved": 0, "created": 0, "skipped": 0, "failed": 0, "details": []}

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(_process_one, lead, api_key=api_key, campaign_id=campaign_id, debug=debug)
            for lead in leads
        ]
        for fut in as_completed(futures):
            results.append(fut.result())

    # ── Write back instantly_lead_id / instantly_campaign_id ─────────────
    # For 'moved' and 'created' rows. We always set instantly_campaign_id
    # to the target so the raw view stays in sync with Instantly's truth.
    now_iso = datetime.now(timezone.utc).isoformat()
    writeback: list[dict] = []
    for r in results:
        if r["op"] in ("moved", "created") and r.get("id") and r.get("instantly_lead_id"):
            writeback.append({
                "id": r["id"],
                "fields": {
                    "instantly_lead_id": r["instantly_lead_id"],
                    "instantly_campaign_id": campaign_id,
                    "instantly_statuts": "Success",
                    "last_synced_at": now_iso,
                },
            })

    if writeback:
        backend.batch_update(writeback)

    counts = {"moved": 0, "created": 0, "skipped": 0, "failed": 0, "already_in_place": 0}
    for r in results:
        counts[r["op"]] = counts.get(r["op"], 0) + 1
    counts["details"] = results
    return counts


def recategorize_all_by_tier(
    backend,
    *,
    api_key: str,
    resolve_campaign_id,
    debug: bool = False,
    max_workers: int = 5,
) -> dict:
    """One-click: re-route every lead in raw.scraped_leads into its tier campaign.

    Iterates LOW → MID → HIGH. For each tier:
      1. Resolve the target Instantly campaign id via `resolve_campaign_id(tier)`
         (caller decides find-or-create policy and naming).
      2. Pull every matching lead — including leads already in another active
         campaign — because the whole point is to reassign.
      3. Run the standard push machinery: existing leads MOVE (preserving
         instantly_lead_id and lid), new leads CREATE + writeback.

    Leads with `ticket_tier IS NULL` are not included in any tier filter, so
    they stay in their current campaign untouched. The operator should fix
    `industry` on those rows first if they want them recategorized.
    """
    by_tier: dict[str, dict] = {}
    for tier in TIERS:
        c_id = resolve_campaign_id(tier)
        if not c_id or not is_valid_uuid(c_id):
            by_tier[tier] = {
                "tier": tier,
                "campaign_id": None,
                "moved": 0, "created": 0, "skipped": 0, "failed": 0,
                "details": [],
                "error": f"could not resolve campaign for tier '{tier}'",
            }
            continue

        spec = {"type": "ticket_tier", "value": tier}
        # SQL-level skip for leads already in the target campaign — keeps
        # each iteration cheap so the operator can re-run the loop safely
        # if a previous run was interrupted (network, rate limit, etc.).
        total_in_tier = backend.count_leads_by_filter(
            spec, exclude_in_active_campaign=False,
        )
        leads = backend.fetch_leads_by_filter(
            spec,
            exclude_in_active_campaign=False,
            exclude_already_in_campaign_id=c_id,
        )
        already_in_place = max(total_in_tier - len(leads), 0)

        push_result = push_leads_to_campaign(
            backend, api_key=api_key, leads=leads,
            campaign_id=c_id, debug=debug, max_workers=max_workers,
        )
        push_result["tier"] = tier
        push_result["campaign_id"] = c_id
        push_result["error"] = None
        # SQL excluded these so they don't show up in details, but the
        # operator should still see how many we left untouched.
        push_result["already_in_place"] = (
            push_result.get("already_in_place", 0) + already_in_place
        )
        by_tier[tier] = push_result

    aggregated = {
        "moved": 0, "created": 0, "skipped": 0, "failed": 0,
        "already_in_place": 0,
    }
    for r in by_tier.values():
        for k in aggregated:
            aggregated[k] += r.get(k, 0)
    aggregated["by_tier"] = by_tier
    return aggregated
