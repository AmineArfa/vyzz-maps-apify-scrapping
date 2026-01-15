import pandas as pd
import streamlit as st

from leadgen.airtable_utils import get_industry_options, init_airtable, log_transaction
from leadgen.config import get_secrets
from leadgen.credits import display_credit_dashboard
from leadgen.dashboard import StatusDashboard
from leadgen.runner import execute_with_credit_tracking


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

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        # Default behavior: When industry changes, update the search query to match,
        # unless user has manually typed something else (optional).
        # Requirement: "default being the industry selected (switching when another industry selected)"
        # This implies we should listen to industry change.

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
            return

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


if __name__ == "__main__":
    main()
