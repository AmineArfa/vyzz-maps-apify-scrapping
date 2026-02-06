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
from .millionverifier import GOOD_STATUSES, verify_pending_leads


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


# ---------------------------------------------------------------------------
# MillionVerifier Gatekeeper – cleanup & orchestration
# ---------------------------------------------------------------------------


def _process_bad_lead(lead: dict, *, api_key: str, debug_mode: bool) -> dict:
    """Handle a single bad lead: remove from Instantly if present.

    Uses ``instantly_lead_id`` when available (fast path), otherwise falls
    back to ``search_lead_by_email``.
    """
    clean = sanitize_for_json(lead or {})
    airtable_id = clean.get("id")
    instantly_id = clean.get("instantly_lead_id")
    email = clean.get("key_contact_email")
    if isinstance(email, str):
        email = email.strip().lower() or None

    deleted = False
    error: str | None = None

    # Fast path: we already know the Instantly lead ID
    if instantly_id and is_valid_uuid(instantly_id):
        ok, err = delete_lead_from_instantly(api_key, instantly_id, debug=debug_mode)
        if ok:
            deleted = True
        elif "not found" in str(err or "").lower():
            # Already gone – that's fine
            deleted = False
        else:
            error = err
    elif email:
        # Slow path: search by email
        found, search_err = search_lead_by_email(api_key, email, debug=debug_mode)
        if found:
            found_id = found.get("id")
            if found_id:
                ok, err = delete_lead_from_instantly(api_key, found_id, debug=debug_mode)
                if ok:
                    deleted = True
                else:
                    error = err
        elif search_err:
            error = search_err

    return {
        "id": airtable_id,
        "deleted": deleted,
        "error": error,
    }


def cleanup_bad_leads(
    bad_leads: list[tuple[dict, str]],
    table_leads,
    *,
    secrets: dict,
    debug_mode: bool,
    status,
) -> dict:
    """Remove bad leads from Instantly and update Airtable.

    Parameters
    ----------
    bad_leads : list[tuple[dict, str]]
        Each element is ``(record_dict, verification_status)``.
    table_leads
        Airtable table object for batch updates.
    secrets : dict
        Application secrets (needs ``instantly_key``).
    debug_mode : bool
        Whether to show debug output.
    status
        Streamlit status widget for progress messages.

    Returns
    -------
    dict
        Summary with ``deleted``, ``not_found``, ``errors``, ``details`` keys.
    """
    if not bad_leads:
        return {"deleted": 0, "not_found": 0, "errors": 0, "details": []}

    api_key = secrets["instantly_key"]
    timestamp_now = pd.Timestamp.now(tz="UTC").isoformat()

    status.write(f"🛡️ Cleaning up {len(bad_leads)} bad leads from Instantly...")

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                _process_bad_lead, rec, api_key=api_key, debug_mode=debug_mode
            ): (rec, v_status)
            for rec, v_status in bad_leads
        }
        for future in as_completed(futures):
            res = future.result()
            rec, v_status = futures[future]
            res["verification_status"] = v_status
            results.append(res)

    # Build Airtable updates
    airtable_updates: list[dict] = []
    deleted_count = 0
    not_found_count = 0
    error_count = 0

    for res in results:
        airtable_id = res["id"]
        if not airtable_id:
            continue

        fields: dict = {
            "verification_status": res["verification_status"],
            "last_synced_at": timestamp_now,
        }
        # Clear Instantly references since the lead should not be there
        fields["instantly_lead_id"] = None
        fields["instantly_campaign_id"] = None
        fields["instantly_statuts"] = "Blocked"

        airtable_updates.append({"id": airtable_id, "fields": fields})

        if res.get("error"):
            error_count += 1
            status.write(f"⚠️ {airtable_id}: cleanup error – {res['error']}")
        elif res["deleted"]:
            deleted_count += 1
            status.write(f"🗑️ {airtable_id}: removed from Instantly ({res['verification_status']})")
        else:
            not_found_count += 1

    # Batch update Airtable
    if airtable_updates:
        status.write(f"📝 Updating {len(airtable_updates)} bad-lead records in Airtable...")
        if batch_update_leads(table_leads, airtable_updates):
            status.write(
                f"✅ Bad leads updated – {deleted_count} deleted from Instantly, "
                f"{not_found_count} not found (already clean), {error_count} errors."
            )
        else:
            status.write("❌ Airtable update for bad leads failed.")

    return {
        "deleted": deleted_count,
        "not_found": not_found_count,
        "errors": error_count,
        "details": results,
    }


def sync_with_verification(
    pending_records: list[dict],
    table_leads,
    *,
    secrets: dict,
    debug_mode: bool,
    max_records: int = 5000,
    status,
) -> dict:
    """Orchestrator: verify emails then sync good / clean up bad.

    This replaces the direct ``sync_pending_leads()`` call when a
    MillionVerifier API key is available.

    Flow
    ----
    1. Verify pending records (hybrid: trust existing status / call API).
    2. Split into *good* (``ok``) and *bad* (everything else).
    3. Sync good leads via existing ``sync_pending_leads()``.
    4. Clean up bad leads via ``cleanup_bad_leads()``.
    5. Batch-update Airtable ``verification_status`` for API-verified good leads.
    6. Merge and return combined results.
    """
    mv_key = secrets.get("millionverifier_key", "")
    timestamp_now = pd.Timestamp.now(tz="UTC").isoformat()

    # ── Cap records ──────────────────────────────────────────────────────
    total_all = len(pending_records)
    skipped = 0
    if max_records and total_all > max_records:
        skipped = total_all - max_records
        pending_records = pending_records[:max_records]
        status.write(
            f"🧭 Sync cap: processing {max_records} of {total_all} leads "
            f"({skipped} deferred to next refresh)."
        )

    # ── Step 1: Verification ─────────────────────────────────────────────
    status.write(f"🔍 Verifying {len(pending_records)} emails with MillionVerifier...")
    progress_bar = st.progress(0, text="Verifying emails...")

    def _on_verify_progress(done: int, total: int):
        progress_bar.progress(
            min(done / total, 1.0) if total else 1.0,
            text=f"Verified {done}/{total} emails",
        )

    verified = verify_pending_leads(
        pending_records,
        mv_key,
        max_workers=15,
        on_progress=_on_verify_progress,
    )

    # ── Step 2: Split good / bad ─────────────────────────────────────────
    good_leads: list[dict] = []
    good_api_verified_ids: list[str] = []  # Airtable IDs needing verification_status='ok'
    bad_leads: list[tuple[dict, str]] = []  # (record, status)

    for rec, v_status, was_api in verified:
        if v_status in GOOD_STATUSES:
            good_leads.append(rec)
            if was_api:
                airtable_id = rec.get("id")
                if airtable_id:
                    good_api_verified_ids.append(airtable_id)
        else:
            bad_leads.append((rec, v_status))

    status.write(
        f"📊 Verification results: {len(good_leads)} good (ok) / "
        f"{len(bad_leads)} bad (blocked)"
    )

    # ── Step 3: Sync good leads ──────────────────────────────────────────
    sync_result: dict = {}
    if good_leads:
        status.write(f"🚀 Syncing {len(good_leads)} verified leads to Instantly...")
        sync_result = sync_pending_leads(
            good_leads,
            table_leads,
            secrets=secrets,
            debug_mode=debug_mode,
            max_records=max_records,
            status=status,
        )
        if sync_result.get("error"):
            return sync_result  # propagate fatal Airtable error
    else:
        status.write("ℹ️ No good leads to sync to Instantly.")

    # ── Step 4: Clean up bad leads ───────────────────────────────────────
    cleanup_result: dict = {}
    if bad_leads:
        cleanup_result = cleanup_bad_leads(
            bad_leads,
            table_leads,
            secrets=secrets,
            debug_mode=debug_mode,
            status=status,
        )
    else:
        status.write("ℹ️ No bad leads to clean up.")

    # ── Step 5: Persist verification_status for API-verified good leads ──
    if good_api_verified_ids:
        mv_updates = [
            {"id": aid, "fields": {"verification_status": "ok"}}
            for aid in good_api_verified_ids
        ]
        status.write(
            f"📝 Saving verification_status='ok' for {len(mv_updates)} "
            f"newly verified leads..."
        )
        if not batch_update_leads(table_leads, mv_updates):
            status.write("⚠️ Failed to persist verification_status for good leads.")

    # ── Step 6: Merge results ────────────────────────────────────────────
    sync_details = sync_result.get("details", []) if sync_result else []
    bad_details = [
        {
            "id": r["id"],
            "status": "Blocked",
            "op": "Blocked",
            "error": r.get("error"),
            "verification_status": r.get("verification_status"),
        }
        for r in cleanup_result.get("details", [])
    ]

    all_details = sync_details + bad_details

    status.update(label="✅ Sync Complete!", state="complete", expanded=False)

    return {
        "timestamp": pd.Timestamp.now().strftime("%H:%M:%S"),
        "count": sync_result.get("count", 0),
        "failures": sync_result.get("failures", 0),
        "skipped": skipped,
        "blocked": len(bad_leads),
        "blocked_deleted": cleanup_result.get("deleted", 0),
        "failure_counts": sync_result.get("failure_counts", {}),
        "failure_samples": sync_result.get("failure_samples", {}),
        "details": all_details,
    }
