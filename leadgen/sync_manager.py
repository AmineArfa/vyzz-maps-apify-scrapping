from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import streamlit as st

from .airtable_utils import batch_update_leads
from .instantly import (
    delete_lead_from_instantly,
    export_leads_to_instantly,
    find_or_create_instantly_campaign,
    get_lead_from_instantly,
    is_valid_uuid,
    reset_campaign_cache,
    update_lead_in_instantly,
)
from .json_sanitize import sanitize_for_json


def _classify_error(err: str | None) -> str:
    msg = str(err or "").lower()
    if "out of range float values are not json compliant" in msg or " nan" in msg:
        return "nan_json"
    if "rate limit exceeded" in msg or "statuscode\":429" in msg or " 429" in msg:
        return "rate_limited"
    if "missing api_key, campaign_id, or leads" in msg or "missing api_key/campaign_id" in msg:
        return "missing_config"
    if "invalid lead id" in msg or "invalid lead id format" in msg:
        return "invalid_id"
    if "not found" in msg or "404" in msg:
        return "not_found"
    return "other"


def _process_single_lead(lead: dict, *, secrets: dict, debug_mode: bool):
    clean_data = sanitize_for_json(lead or {})

    lead_id_airtable = clean_data.get("id")
    lead_id_instantly = clean_data.get("instantly_lead_id")
    email_avail = clean_data.get("email_available")
    has_email_mark = str(email_avail) == "1" or email_avail is True
    email = clean_data.get("key_contact_email")
    if not isinstance(email, str) or not email.strip():
        clean_data["key_contact_email"] = None

    industry = clean_data.get("industry")
    if not industry:
        industry = "Generic"
    if isinstance(industry, list):
        industry = industry[0]

    # Campaign ID - module-level lock in instantly.py prevents duplicates
    camp_name = f"{industry} - Cold Outreach"
    c_id = find_or_create_instantly_campaign(secrets["instantly_key"], camp_name, debug=debug_mode)
    if not c_id:
        return {
            "id": lead_id_airtable,
            "status": "Failed",
            "error": f"Create Failed: No campaign available for '{camp_name}' (check Instantly API key / rate limit / permissions)",
        }

    # Result tracker
    new_instantly_id = lead_id_instantly
    operation = "Skip"

    # Instantly Logic
    if lead_id_instantly and is_valid_uuid(lead_id_instantly):
        # Scenario: Existing Sync (Valid UUID)
        if has_email_mark and clean_data.get("key_contact_email"):
            # 1. Fetch current lead from Instantly to check email
            inst_lead, get_err = get_lead_from_instantly(secrets["instantly_key"], lead_id_instantly)

            email_changed = False
            if inst_lead:
                inst_email = inst_lead.get("email")
                if inst_email and inst_email.lower() != clean_data["key_contact_email"].lower():
                    email_changed = True
            elif "404" in str(get_err) or "not found" in str(get_err).lower():
                # Lead doesn't exist anymore anyway
                email_changed = True

            if email_changed:
                # DELETE and RE-POST (identity change)
                operation = "Create"
                delete_lead_from_instantly(secrets["instantly_key"], lead_id_instantly)

                cnt, created, _, create_err = export_leads_to_instantly(
                    secrets["instantly_key"], c_id, [clean_data], debug=debug_mode
                )
                if cnt > 0 and created:
                    new_instantly_id = created[0].get("id")
                else:
                    return {"id": lead_id_airtable, "status": "Failed", "error": f"Email changed, RE-POST failed: {create_err}"}
            else:
                # PATCH (same email or typo fix)
                operation = "Update"
                raw_name = clean_data.get("key_contact_name", "")
                name_parts = str(raw_name).split(" ")
                first_name = name_parts[0] if name_parts else ""
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

                custom_vars = {
                    "postalCode": clean_data.get("postal_code"),
                    "jobTitle": clean_data.get("key_contact_position"),
                    "address": clean_data.get("postal_address"),
                    "City": clean_data.get("city"),
                    "state": clean_data.get("state"),
                }
                custom_vars = {k: v for k, v in custom_vars.items() if v not in (None, "", [], {})}

                patch_payload = {
                    "email": clean_data.get("key_contact_email"),
                    "first_name": first_name,
                    "last_name": last_name,
                    "company_name": clean_data.get("company_name"),
                    "website": clean_data.get("website"),
                    "phone": clean_data.get("generic_phone"),
                    "custom_variables": custom_vars or None,
                }
                patch_payload = sanitize_for_json(patch_payload)
                success, err = update_lead_in_instantly(
                    secrets["instantly_key"], lead_id_instantly, patch_payload, debug=debug_mode
                )
                if not success:
                    # Fallback: if PATCH fails for any reason (404, email conflict, etc.),
                    # create a NEW contact in Instantly and update Airtable with the new ID.
                    # This handles cases where email was modified but PATCH doesn't support email changes.
                    operation = "Create"
                    # Try to delete the old lead first (ignore errors - it may already be gone)
                    delete_lead_from_instantly(secrets["instantly_key"], lead_id_instantly)

                    cnt, created, _, create_err = export_leads_to_instantly(
                        secrets["instantly_key"], c_id, [clean_data], debug=debug_mode
                    )
                    if cnt > 0 and created:
                        new_instantly_id = created[0].get("id")
                    else:
                        return {"id": lead_id_airtable, "status": "Failed", "error": f"Update failed ({err}), Create also failed: {create_err}"}
        else:
            # DELETE
            operation = "Delete"
            success, err = delete_lead_from_instantly(secrets["instantly_key"], lead_id_instantly, debug=debug_mode)
            new_instantly_id = None  # Clear it
            if not success:
                if "not found" in str(err).lower():
                    new_instantly_id = None
                else:
                    return {"id": lead_id_airtable, "status": "Failed", "error": f"Delete Failed: {err}"}
    else:
        # Scenario: No Sync yet OR Invalid ID (Treat as New)
        if has_email_mark and clean_data.get("key_contact_email"):
            # POST
            operation = "Create"
            cnt, created, _, err = export_leads_to_instantly(
                secrets["instantly_key"], c_id, [clean_data], debug=debug_mode
            )
            if cnt > 0 and created:
                new_instantly_id = created[0].get("id")
            elif err:
                return {"id": lead_id_airtable, "status": "Failed", "error": f"Create Failed: {err}"}
        elif lead_id_instantly:
            # We have an invalid ID but no email to sync. Just clear the garbage ID.
            new_instantly_id = None
            operation = "Skip"

    return {
        "id": lead_id_airtable,
        "status": "Success",
        "op": operation,
        "new_instantly_id": new_instantly_id,
        "campaign_id": c_id if operation in ("Create", "Update") else None,
    }


def sync_pending_leads(
    pending_records: list[dict],
    table_leads,
    *,
    secrets: dict,
    debug_mode: bool,
    max_records: int = 5000,
    status,
):
    timestamp_now = pd.Timestamp.now(tz="UTC").isoformat()
    airtable_updates: list[dict] = []

    reset_campaign_cache()
    status.write("📋 Loading existing campaigns...")

    total_all = len(pending_records)
    skipped = 0
    if max_records and total_all > max_records:
        skipped = total_all - max_records
        pending_records = pending_records[:max_records]
        status.write(
            f"🧭 Sync cap: processing {max_records} of {total_all} leads "
            f"({skipped} deferred to next refresh)."
        )

    total = len(pending_records)
    completed = 0
    progress_bar = st.progress(0, text="Starting sync...")

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(_process_single_lead, lead, secrets=secrets, debug_mode=debug_mode)
            for lead in pending_records
        ]
        for future in as_completed(futures):
            res = future.result()
            results.append(res)
            completed += 1

            progress_bar.progress(
                min(completed / total, 1.0),
                text=f"Processed {completed}/{total} leads",
            )
            if res.get("status") == "Success":
                status.write(f"✅ {res.get('id')}: {res.get('op', 'Sync')}")
            else:
                status.write(f"⚠️ {res.get('id')}: {res.get('error')}")

    # 3. Process Results and Update Airtable
    success_count = 0
    failure_rows: list[dict] = []
    failure_counts: Counter[str] = Counter()
    failure_samples: dict[str, list[str]] = defaultdict(list)
    for res in results:
        # IMPORTANT: Only update last_synced_at on SUCCESS so failures stay "Pending"
        if res.get("status") == "Success":
            success_count += 1
            fields = {"last_synced_at": timestamp_now}

            op = res.get("op")
            if op in ("Create", "Update"):
                fields["instantly_statuts"] = "Success"
            elif op == "Delete":
                fields["instantly_statuts"] = None  # Clear status on delete
            else:  # Skip
                fields["instantly_statuts"] = None

            # Persist the (new) Instantly IDs
            fields["instantly_lead_id"] = res.get("new_instantly_id")
            fields["instantly_campaign_id"] = res.get("campaign_id")

            airtable_updates.append({"id": res["id"], "fields": fields})
        else:
            # On Failure: We mark as Failed in Airtable but do NOT update last_synced_at
            # This keeps it in the "Pending" list for user to fix/retry.
            airtable_updates.append(
                {
                    "id": res["id"],
                    "fields": {"instantly_statuts": "Failed"},
                }
            )
            err = res.get("error")
            cat = _classify_error(err)
            failure_counts[cat] += 1
            if len(failure_samples[cat]) < 10:
                failure_samples[cat].append(str(res["id"]))
            failure_rows.append(res)

    if failure_counts:
        status.write("—")
        status.write("🧪 Failure summary (by category):")
        for cat, cnt in failure_counts.most_common():
            sample = ", ".join(failure_samples.get(cat, [])[:5])
            status.write(f"- {cat}: {cnt} (sample: {sample})")

    # 4. Final Airtable Batch Update
    if airtable_updates:
        status.write(f"📝 Finalizing {len(airtable_updates)} updates in Airtable...")
        if batch_update_leads(table_leads, airtable_updates):
            status.write(f"✅ Airtable updated ({success_count} leads successfully synced).")
        else:
            status.write("❌ Airtable update failed.")
            return {"error": "airtable_update_failed"}
    else:
        status.write("ℹ️ No updates to push to Airtable.")

    status.update(label="✅ Sync Complete!", state="complete", expanded=False)

    return {
        "timestamp": pd.Timestamp.now().strftime("%H:%M:%S"),
        "count": success_count,
        "failures": len(failure_rows),
        "skipped": skipped,
        "failure_counts": dict(failure_counts),
        "failure_samples": dict(failure_samples),
        "details": results,
    }
