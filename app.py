import pandas as pd
import streamlit as st

from leadgen.airtable_utils import (
    get_industry_options,
    init_airtable,
    log_transaction,
    fetch_all_leads,
    batch_update_leads,
)
from leadgen.config import get_secrets
from leadgen.credits import display_credit_dashboard
from leadgen.dashboard import StatusDashboard
from leadgen.runner import execute_with_credit_tracking
from leadgen.instantly import (
    export_leads_to_instantly,
    find_or_create_instantly_campaign,
    update_lead_in_instantly,
    delete_lead_from_instantly,
    is_valid_uuid,
)


# --- CONSTANTS & CONFIGURATION ---
AIRTABLE_LEADS_TABLE = "tblKrC9hOxCuMMyZT"
AIRTABLE_LOG_TABLE = "log"
SCRAPPING_TOOL_ID = "maps_apify_apollo"


st.set_page_config(page_title="Lead Generation Engine", page_icon="🚀", layout="wide")


def main():
    secrets = get_secrets()

    st.sidebar.header("🔌 Connection Status")
    try:
        table_leads, table_log = init_airtable(
            secrets["airtable_key"],
            secrets["airtable_base"],
            leads_table=AIRTABLE_LEADS_TABLE,
            log_table=AIRTABLE_LOG_TABLE,
        )
        st.sidebar.success("Airtable Connected")
    except Exception as e:
        st.sidebar.error(f"Airtable Connection Failed: {e}")
        st.stop()

    if secrets["apify_token"]:
        st.sidebar.success("Apify Token Found")
    if secrets["apollo_key"]:
        st.sidebar.success("Apollo Key Found")
    if secrets["instantly_key"]:
        st.sidebar.success("Instantly Key Found")
    else:
        st.sidebar.warning("Instantly Key Missing")

    if secrets.get("gemini_key"):
        st.sidebar.success("Gemini Key Found")
    else:
        st.sidebar.warning("Gemini Key Missing (split disabled)")

    debug_mode = st.sidebar.checkbox("🔍 Debug Mode", value=False, help="Show API response details")
    st.session_state["debug_mode"] = debug_mode

    use_gemini_split_default = bool(secrets.get("gemini_key"))
    use_gemini_split = st.sidebar.checkbox(
        "🧩 Use Gemini Split (10 zones)",
        value=use_gemini_split_default,
        help="Splits the location into 10 sub-zones to increase Google Maps coverage. Falls back to single query on any Gemini error.",
        disabled=not bool(secrets.get("gemini_key")),
    )
    st.session_state["use_gemini_split"] = use_gemini_split

    display_credit_dashboard(
        secrets["apify_token"],
        secrets["apollo_key"],
        secrets["instantly_key"],
        debug=debug_mode,
    )

    if "industry_options" not in st.session_state:
        st.session_state["industry_options"] = get_industry_options(
            secrets["airtable_key"], secrets["airtable_base"], AIRTABLE_LEADS_TABLE
        )

    st.title("🚀 Lead Generation Engine")

    # --- TABS ---
    tab_gen, tab_man = st.tabs(["🚀 Lead Generator", "📝 Lead Manager"])

    # --- TAB 1: GENERATOR ---
    with tab_gen:
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            def on_industry_change():
                st.session_state["search_query_input"] = st.session_state["industry_selector"]

            industry = st.selectbox(
                "Industry",
                st.session_state["industry_options"] or ["Generic"],
                key="industry_selector",
                on_change=on_industry_change
            )

            # Initialize search query if not present
            if "search_query_input" not in st.session_state:
                st.session_state["search_query_input"] = industry

            search_query = st.text_input("Search Query", key="search_query_input")

        with col2:
            city_input = st.text_input("City", placeholder="e.g. New York")
        with col3:
            max_leads = st.number_input("Max Leads", min_value=1, max_value=500, value=10, step=1)

        enrich_emails = st.checkbox("✨ Enrich with Emails? (Apollo)", value=True)

        if st.button("Find & Sync Leads", type="primary"):
            if not city_input:
                st.warning("Please enter a city.")
                # We cannot just return here because we are in a with block inside main
            else:
                status_dashboard = StatusDashboard()

                result_data, credit_used_apify, credit_used_apollo, credit_used_instantly = execute_with_credit_tracking(
                    secrets,
                    table_leads,
                    industry,
                    city_input,
                    max_leads,
                    enrich_emails,
                    status_dashboard,
                    leads_table_id=AIRTABLE_LEADS_TABLE,
                    scrapping_tool_id=SCRAPPING_TOOL_ID,
                    search_query=search_query,
                )

                status_dashboard.update_status("Execution Complete!", 100)

                log_transaction(
                    table_log,
                    industry,
                    city_input,
                    result_data["total_scraped"],
                    result_data["new_added"],
                    enrich_emails,
                    result_data["status"],
                    error_msg=result_data["error_msg"],
                    credit_used_apify=credit_used_apify,
                    credit_used_apollo=credit_used_apollo,
                    credit_used_instantly=credit_used_instantly,
                    instantly_added=result_data.get("instantly_added"),
                    search_query=search_query,
                )

                if result_data["status"] in ("Success", "Zero Results"):
                    credit_info = []
                    if credit_used_apify is not None:
                        credit_info.append(f"Apify: ${credit_used_apify:.4f}")
                    if credit_used_apollo is not None:
                        credit_info.append(f"Apollo: {credit_used_apollo} credits")
                    credit_str = f" ({', '.join(credit_info)})" if credit_info else ""

                    st.success(f"✅ Scraped {result_data['total_scraped']}, Added {result_data['new_added']}.{credit_str}")

                    # Invalidate cached lead_df to force reload on next visit to Manager
                    if "lead_df" in st.session_state:
                        del st.session_state["lead_df"]

                    if result_data["new_records"]:
                        st.dataframe(pd.DataFrame(result_data["new_records"]))
                    else:
                        st.info("No new unique leads found.")
                else:
                    st.error(f"❌ Execution failed: {result_data['error_msg']}")

                with st.sidebar:
                    st.markdown("---")
                    st.caption("🔄 Refreshing credit dashboard...")
                display_credit_dashboard(
                    secrets["apify_token"],
                    secrets["apollo_key"],
                    secrets["instantly_key"],
                    debug=st.session_state.get("debug_mode", False),
                )

    # --- TAB 2: SYNC MANAGER (AIRTABLE FIRST) ---
    with tab_man:
        st.info("💡 **Sync Manager**: Edit data in Airtable. When you save there, changes appear here as 'Pending'. Click Sync to push to Instantly.")

        if st.button("🔄 Refresh Data"):
            if "lead_df" in st.session_state:
                del st.session_state["lead_df"]
            st.rerun()

        if "lead_df" not in st.session_state:
            with st.spinner("Fetching leads from Airtable..."):
                raw_leads = fetch_all_leads(table_leads)
                st.session_state["lead_df"] = pd.DataFrame(raw_leads)

        # Base DataFrame
        df = st.session_state["lead_df"].copy()
        
        # Ensure timestamp columns exist
        if "last_modified_at" not in df.columns:
            df["last_modified_at"] = None
        if "last_synced_at" not in df.columns:
            df["last_synced_at"] = None
            
        # Convert to datetime for comparison
        df["last_modified_at"] = pd.to_datetime(df["last_modified_at"], errors='coerce', utc=True)
        df["last_synced_at"] = pd.to_datetime(df["last_synced_at"], errors='coerce', utc=True)

        # --- FILTER PENDING UPDATES ---
        # Logic: last_modified > last_synced OR last_synced is NaT (Never synced)
        # Note: We must handle NaT carefully.
        # If synced is NaT -> Pending.
        # If modified is NaT (shouldn't happen for valid records) -> Not pending? Or Pending? Assume Modified exists.
        
        def is_pending(row):
            mod = row["last_modified_at"]
            syn = row["last_synced_at"]
            if pd.isna(mod): return False # Should not happen if Airtable tracks it
            if pd.isna(syn): return True # Never synced
            return mod > syn

        # Apply pending filter
        # We need a mask
        pending_mask = df.apply(is_pending, axis=1)
        pending_df = df[pending_mask]
        
        pending_count = len(pending_df)
        
        col_header, col_btn = st.columns([3, 1])
        with col_header:
            st.subheader(f"⏳ Pending Updates: {pending_count} Leads")
        
        with col_btn:
             # Sync Button (Active only if pending items exist)
             if st.button(f"🚀 Sync {pending_count} Updates", type="primary", disabled=pending_count == 0):
                 with st.status("Syncing to Instantly...", expanded=True) as status:
                     # 1. Prepare Data
                     pending_records = pending_df.to_dict("records")
                     airtable_updates = []
                     timestamp_now = pd.Timestamp.now(tz="UTC").isoformat()
                     
                     from concurrent.futures import ThreadPoolExecutor, as_completed
                     
                     def process_single_lead(lead):
                         lead_id_airtable = lead.get("id")
                         lead_id_instantly = lead.get("instantly_lead_id")
                         email_avail = lead.get("email_available")
                         has_email_mark = str(email_avail) == "1" or email_avail is True
                         email = lead.get("key_contact_email")
                         
                         industry = lead.get("industry")
                         if not industry:
                             industry = "Generic"
                         if isinstance(industry, list): 
                             industry = industry[0]
                         
                         # Data cleaning for Instantly
                         clean_data = {}
                         for k, v in lead.items():
                             if isinstance(v, float) and v != v: clean_data[k] = "[undefined]"
                             elif v in (None, ""): clean_data[k] = "[undefined]"
                             elif isinstance(v, pd.Timestamp): clean_data[k] = v.isoformat()
                             else: clean_data[k] = v
                         
                         if clean_data.get("key_contact_email") == "[undefined]":
                             clean_data["key_contact_email"] = None
                             
                         # Campaign ID
                         camp_name = f"{industry} - Cold Outreach"
                         c_id = find_or_create_instantly_campaign(secrets["instantly_key"], camp_name)
                         
                         # Result tracker
                         new_instantly_id = lead_id_instantly
                         operation = "Skip"
                         
                         # Instantly Logic
                         if lead_id_instantly and is_valid_uuid(lead_id_instantly):
                             # Scenario: Existing Sync (Valid UUID)
                             if has_email_mark and clean_data.get("key_contact_email"):
                                 # PATCH
                                 operation = "Update"
                                 raw_name = clean_data.get("key_contact_name", "")
                                 name_parts = str(raw_name).split(" ")
                                 first_name = name_parts[0] if name_parts else "[undefined]"
                                 last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else "[undefined]"
                                 
                                 patch_payload = {
                                     "email": clean_data.get("key_contact_email"),
                                     "first_name": first_name,
                                     "last_name": last_name,
                                     "company_name": clean_data.get("company_name"),
                                     "website": clean_data.get("website"),
                                     "phone": clean_data.get("generic_phone"),
                                     "custom_variables": {
                                         "postalCode": clean_data.get("postal_code"),
                                         "jobTitle": clean_data.get("key_contact_position"),
                                         "address": clean_data.get("postal_address"),
                                         "City": clean_data.get("city"),
                                         "state": clean_data.get("state")
                                     }
                                 }
                                 success, err = update_lead_in_instantly(secrets["instantly_key"], lead_id_instantly, patch_payload)
                                 if not success:
                                     # Fallback: if lead not found (404), try Create instead
                                     if "404" in str(err) or "not found" in str(err).lower():
                                         operation = "Create"
                                         cnt, created, _, create_err = export_leads_to_instantly(
                                             secrets["instantly_key"], c_id, [lead]
                                         )
                                         if cnt > 0 and created:
                                             new_instantly_id = created[0].get("id")
                                         else:
                                             return {"id": lead_id_airtable, "status": "Failed", "error": f"Update failed (404), Create failed: {create_err}"}
                                     else:
                                         return {"id": lead_id_airtable, "status": "Failed", "error": err}
                             else:
                                 # DELETE
                                 operation = "Delete"
                                 success, err = delete_lead_from_instantly(secrets["instantly_key"], lead_id_instantly)
                                 new_instantly_id = None # Clear it
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
                                     secrets["instantly_key"], c_id, [lead]
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
                             "campaign_id": c_id if operation in ("Create", "Update") else None
                         }

                     # 2. Parallel Execution
                     results = []
                     with ThreadPoolExecutor(max_workers=5) as executor:
                         futures = [executor.submit(process_single_lead, lead) for lead in pending_records]
                         for future in as_completed(futures):
                             results.append(future.result())
                     
                     # 3. Process Results and Update Airtable
                     success_count = 0
                     for res in results:
                         fields = {"last_synced_at": timestamp_now}
                         
                         if res.get("status") == "Success":
                             success_count += 1
                             op = res.get("op")
                             
                             if op in ("Create", "Update"):
                                 fields["instantly_statuts"] = "Success"
                             elif op == "Delete":
                                 fields["instantly_statuts"] = None # Clear status on delete
                             else: # Skip
                                 fields["instantly_statuts"] = None 
                             
                             # Manage IDs
                             fields["instantly_lead_id"] = res.get("new_instantly_id")
                             fields["instantly_campaign_id"] = res.get("campaign_id")
                         else:
                             fields["instantly_statuts"] = "Failed"
                             status.write(f"⚠️ Row {res['id']}: {res.get('error')}")
                         
                         airtable_updates.append({"id": res["id"], "fields": fields})
                     
                     # 4. Final Airtable Batch Update
                     if airtable_updates:
                         status.write(f"📝 Finalizing {len(airtable_updates)} updates in Airtable...")
                         if batch_update_leads(table_leads, airtable_updates):
                             status.write(f"✅ Airtable updated ({success_count} leads successfully synced).")
                         else:
                             status.write("❌ Airtable update failed.")
                             st.error("Failed to update Airtable labels.")
                             st.stop()
                     else:
                         status.write("ℹ️ No updates to push to Airtable.")
                     
                     status.update(label="✅ Sync Complete!", state="complete", expanded=False)
                     
                     # 4. Refresh
                     del st.session_state["lead_df"]
                     st.rerun()

        # Display Pending Table (Read-Only)
        if pending_count > 0:
            st.dataframe(
                pending_df,
                use_container_width=True,
                column_config={
                    "last_modified_at": st.column_config.DatetimeColumn("Modified", format="D MMM HH:mm"),
                    "last_synced_at": st.column_config.DatetimeColumn("Last Synced", format="D MMM HH:mm"),
                    "id": None,
                    "createdTime": None
                },
                column_order=[
                    "company_name", 
                    "industry", 
                    "key_contact_email", 
                    "last_modified_at", 
                    "last_synced_at",
                     "key_contact_name", 
                    "key_contact_position"
                ] 
            )
        else:
            st.success("🎉 Everything is up to date! Modify records in Airtable to see them here.")


if __name__ == "__main__":
    main()
