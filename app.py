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
from leadgen.sync_manager import sync_pending_leads, sync_with_verification


# --- CONSTANTS & CONFIGURATION ---
AIRTABLE_LEADS_TABLE = "tblKrC9hOxCuMMyZT"
AIRTABLE_LOG_TABLE = "log"
SCRAPPING_TOOL_ID = "maps_apify_apollo"
MAX_SYNC_PER_RUN = 5000


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

    if secrets.get("millionverifier_key"):
        st.sidebar.success("MillionVerifier Key Found")
    else:
        st.sidebar.warning("MillionVerifier Key Missing (email verification disabled)")

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
        if pending_count > MAX_SYNC_PER_RUN:
            st.warning(
                f"Sync is capped at {MAX_SYNC_PER_RUN} leads per run. "
                f"{pending_count - MAX_SYNC_PER_RUN} will remain pending for the next refresh."
            )
        
        col_header, col_btn = st.columns([3, 1])
        with col_header:
            st.subheader(f"⏳ Pending Updates: {pending_count} Leads")
        
        with col_btn:
             # Sync Button (Active only if pending items exist)
             if st.button(f"🚀 Sync {pending_count} Updates", type="primary", disabled=pending_count == 0):
                 with st.status("Syncing to Instantly...", expanded=True) as status:
                     pending_records = pending_df.to_dict("records")

                     if secrets.get("millionverifier_key"):
                         # Smart Verification & Delta Cleanup workflow
                         sync_result = sync_with_verification(
                             pending_records,
                             table_leads,
                             secrets=secrets,
                             debug_mode=debug_mode,
                             max_records=MAX_SYNC_PER_RUN,
                             status=status,
                         )
                     else:
                         # Fallback: no verification, sync directly
                         st.warning("⚠️ MillionVerifier key missing – syncing without email verification.")
                         sync_result = sync_pending_leads(
                             pending_records,
                             table_leads,
                             secrets=secrets,
                             debug_mode=debug_mode,
                             max_records=MAX_SYNC_PER_RUN,
                             status=status,
                         )

                     if sync_result.get("error"):
                         st.error("Failed to update Airtable labels.")
                         st.stop()

                     # Save results to session state for persistent logging
                     st.session_state["last_sync_results"] = sync_result

                     # Refresh
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
                    "verification_status",
                    "last_modified_at", 
                    "last_synced_at",
                    "key_contact_name", 
                    "key_contact_position",
                ] 
            )
        else:
            st.success("🎉 Everything is up to date! Modify records in Airtable to see them here.")

        # --- PERSISTENT LOGS AT BOTTOM ---
        if "last_sync_results" in st.session_state:
            res = st.session_state["last_sync_results"]
            st.divider()
            failures = res.get("failures", 0)
            skipped = res.get("skipped", 0)
            blocked = res.get("blocked", 0)
            deferred_text = f" / {skipped} Deferred" if skipped else ""
            blocked_text = f" / {blocked} Blocked" if blocked else ""
            with st.expander(
                f"📋 Last Sync Log ({res['timestamp']}) - {res['count']} Successes / {failures} Failures{blocked_text}{deferred_text}",
                expanded=True,
            ):
                # Blocked leads summary (from MillionVerifier)
                if blocked:
                    blocked_deleted = res.get("blocked_deleted", 0)
                    st.write(
                        f"**🛡️ Email Verification**: {blocked} leads blocked "
                        f"({blocked_deleted} removed from Instantly)"
                    )
                    st.divider()

                fc = res.get("failure_counts") or {}
                if fc:
                    st.write("**Failure breakdown**")
                    for k, v in sorted(fc.items(), key=lambda kv: kv[1], reverse=True):
                        samples = ", ".join((res.get("failure_samples") or {}).get(k, [])[:5])
                        st.write(f"- **{k}**: {v} (sample rows: {samples})")
                    st.write(
                        "**Notes**: `nan_json` means a NaN/Infinity sneaked into a payload; "
                        "`rate_limited` is Instantly 429; `missing_config` usually means missing Instantly key or campaign creation failed."
                    )
                    st.divider()
                for item in res["details"]:
                    if item.get("status") == "Success":
                        op = item.get("op", "Sync")
                        st.write(f"✅ **{item['id']}**: {op} successful")
                    elif item.get("status") == "Blocked":
                        v_status = item.get("verification_status", "bad")
                        st.write(f"🛡️ **{item['id']}**: Blocked ({v_status})")
                    else:
                        st.error(f"❌ **{item['id']}**: {item.get('error') or item.get('status')}")
                
                if st.button("🧹 Clear Logs"):
                    del st.session_state["last_sync_results"]
                    st.rerun()



if __name__ == "__main__":
    main()
