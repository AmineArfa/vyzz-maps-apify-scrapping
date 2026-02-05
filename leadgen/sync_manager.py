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
    search_lead_by_email,
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
    if "email changed" in msg or "create failed" in msg:
        return "email_change"
    if "duplicate" in msg or "already exists" in msg:
        return "duplicate"
    return "other"


def _build_patch_payload(clean_data: dict) -> dict:
    """Build a PATCH payload from clean_data."""
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
        "competitor1": clean_data.get("competitor1"),
        "competitor2": clean_data.get("competitor2"),
        "competitor3": clean_data.get("competitor3"),
    }
    custom_vars = {k: v for k, v in custom_vars.items() if v not in (None, "", [], {})}

    return sanitize_for_json({
        "email": clean_data.get("key_contact_email"),
        "first_name": first_name,
        "last_name": last_name,
        "company_name": clean_data.get("company_name"),
        "website": clean_data.get("website"),
        "phone": clean_data.get("generic_phone"),
        "custom_variables": custom_vars or None,
    })


def _process_single_lead(lead: dict, *, secrets: dict, debug_mode: bool):
    """
    Process a single lead for sync to Instantly.
    
    Handles all scenarios:
    - New lead with email that may or may not exist in Instantly
    - Existing lead with same email (update)
    - Existing lead with changed email (delete old + create/link new)
    - Duplicate emails (link to existing Instantly lead)
    - Lead removal (delete from Instantly)
    """
    clean_data = sanitize_for_json(lead or {})
    api_key = secrets["instantly_key"]

    lead_id_airtable = clean_data.get("id")
    lead_id_instantly = clean_data.get("instantly_lead_id")
    email_avail = clean_data.get("email_available")
    has_email_mark = str(email_avail) == "1" or email_avail is True
    email = clean_data.get("key_contact_email")
    if not isinstance(email, str) or not email.strip():
        clean_data["key_contact_email"] = None
        email = None
    else:
        email = email.strip().lower()
        clean_data["key_contact_email"] = email

    industry = clean_data.get("industry")
    if not industry:
        industry = "Generic"
    if isinstance(industry, list):
        industry = industry[0]

    # Campaign ID - module-level lock in instantly.py prevents duplicates
    camp_name = f"{industry} - Cold Outreach"
    c_id = find_or_create_instantly_campaign(api_key, camp_name, debug=debug_mode)
    if not c_id:
        return {
            "id": lead_id_airtable,
            "status": "Failed",
            "error": f"Create Failed: No campaign available for '{camp_name}' (check Instantly API key / rate limit / permissions)",
        }

    # Result tracker
    new_instantly_id = lead_id_instantly
    operation = "Skip"

    # =========================================================================
    # SCENARIO A: Has valid instantly_lead_id (existing sync)
    # =========================================================================
    if lead_id_instantly and is_valid_uuid(lead_id_instantly):
        
        # A1: Should have email in Instantly
        if has_email_mark and email:
            # Fetch current lead from Instantly
            inst_lead, get_err = get_lead_from_instantly(api_key, lead_id_instantly)
            lead_exists_in_instantly = inst_lead is not None
            inst_email = (inst_lead.get("email") or "").lower() if inst_lead else None
            
            # Determine if email changed
            email_matches = inst_email and inst_email == email
            
            if lead_exists_in_instantly and email_matches:
                # A1a: Lead exists and email matches -> PATCH update
                operation = "Update"
                patch_payload = _build_patch_payload(clean_data)
                success, err = update_lead_in_instantly(api_key, lead_id_instantly, patch_payload, debug=debug_mode)
                
                if not success:
                    # PATCH failed - try delete + create as fallback
                    operation = "Create"
                    delete_lead_from_instantly(api_key, lead_id_instantly)
                    cnt, created, _, create_err = export_leads_to_instantly(api_key, c_id, [clean_data], debug=debug_mode)
                    if cnt > 0 and created:
                        new_instantly_id = created[0].get("id")
                    else:
                        # Check if email now exists (race condition or duplicate)
                        existing, _ = search_lead_by_email(api_key, email, campaign_id=c_id, debug=debug_mode)
                        if existing:
                            new_instantly_id = existing.get("id")
                            operation = "Link"
                        else:
                            return {"id": lead_id_airtable, "status": "Failed", "error": f"Update failed ({err}), Create also failed: {create_err}"}
            else:
                # A1b: Email changed OR lead doesn't exist in Instantly anymore
                # First, check if new email already exists in Instantly
                existing_with_new_email, _ = search_lead_by_email(api_key, email, campaign_id=c_id, debug=debug_mode)
                
                if existing_with_new_email:
                    # New email already exists -> Link to existing, delete old
                    operation = "Link"
                    new_instantly_id = existing_with_new_email.get("id")
                    
                    # Delete old lead if it still exists and is different
                    if lead_exists_in_instantly and new_instantly_id != lead_id_instantly:
                        delete_lead_from_instantly(api_key, lead_id_instantly)
                else:
                    # New email doesn't exist -> Delete old + Create new
                    operation = "Create"
                    if lead_exists_in_instantly:
                        delete_lead_from_instantly(api_key, lead_id_instantly)
                    
                    cnt, created, _, create_err = export_leads_to_instantly(api_key, c_id, [clean_data], debug=debug_mode)
                    if cnt > 0 and created:
                        new_instantly_id = created[0].get("id")
                    else:
                        # Double-check: maybe it was created by another thread/process
                        existing, _ = search_lead_by_email(api_key, email, campaign_id=c_id, debug=debug_mode)
                        if existing:
                            new_instantly_id = existing.get("id")
                            operation = "Link"
                        else:
                            return {"id": lead_id_airtable, "status": "Failed", "error": f"Email changed, create failed: {create_err}"}
        else:
            # A2: No email -> DELETE from Instantly
            operation = "Delete"
            success, err = delete_lead_from_instantly(api_key, lead_id_instantly, debug=debug_mode)
            new_instantly_id = None
            if not success and "not found" not in str(err).lower():
                return {"id": lead_id_airtable, "status": "Failed", "error": f"Delete Failed: {err}"}
    
    # =========================================================================
    # SCENARIO B: No instantly_lead_id or invalid (new sync)
    # =========================================================================
    else:
        if has_email_mark and email:
            # B1: Has email -> Check if it already exists in Instantly
            existing_lead, search_err = search_lead_by_email(api_key, email, campaign_id=c_id, debug=debug_mode)
            
            if existing_lead:
                # B1a: Email already exists in Instantly -> Link to it
                operation = "Link"
                new_instantly_id = existing_lead.get("id")
            else:
                # B1b: Email doesn't exist -> Create new lead
                operation = "Create"
                cnt, created, _, err = export_leads_to_instantly(api_key, c_id, [clean_data], debug=debug_mode)
                if cnt > 0 and created:
                    new_instantly_id = created[0].get("id")
                elif err:
                    # Final check: maybe created by race condition
                    existing, _ = search_lead_by_email(api_key, email, campaign_id=c_id, debug=debug_mode)
                    if existing:
                        new_instantly_id = existing.get("id")
                        operation = "Link"
                    else:
                        return {"id": lead_id_airtable, "status": "Failed", "error": f"Create Failed: {err}"}
                else:
                    # cnt == 0 but no error - lead was skipped (already exists)
                    # Search for it
                    existing, _ = search_lead_by_email(api_key, email, campaign_id=c_id, debug=debug_mode)
                    if existing:
                        new_instantly_id = existing.get("id")
                        operation = "Link"
                    else:
                        new_instantly_id = None
                        operation = "Skip"
        else:
            # B2: No email -> Nothing to sync
            new_instantly_id = None if lead_id_instantly else lead_id_instantly
            operation = "Skip"

    return {
        "id": lead_id_airtable,
        "status": "Success",
        "op": operation,
        "new_instantly_id": new_instantly_id,
        "campaign_id": c_id if operation in ("Create", "Update", "Link") else None,
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
            if op in ("Create", "Update", "Link"):
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
