import streamlit as st
import pandas as pd
import requests
from apify_client import ApifyClient
from pyairtable import Api
from datetime import datetime
import time
import re

# --- CONSTANTS & CONFIGURATION ---
AIRTABLE_TABLE_NAME = "Leads_Scrapping"
AIRTABLE_LOG_TABLE_NAME = "log"
SCRAPPING_TOOL_ID = "maps_apify_apollo"

# Setup Page
st.set_page_config(
    page_title="Lead Generation Engine",
    page_icon="🚀",
    layout="wide"
)

# --- HELPER FUNCTIONS ---

def get_apify_credits(token, debug=False):
    """
    Fetch Apify monthly usage and limit using two endpoints.
    Returns: (usage_usd, limit_usd) or (None, None) on error.
    """
    try:
        headers = {"Authorization": f"Bearer {token}"}
        
        # 1. Get Limit from /users/me
        limit_usd = 0
        try:
            url_me = "https://api.apify.com/v2/users/me"
            resp_me = requests.get(url_me, headers=headers, timeout=10)
            if resp_me.status_code == 200:
                data_me = resp_me.json()
                if debug:
                    st.write("🔍 Apify /users/me data:", data_me)
                limit_usd = data_me.get("data", {}).get("plan", {}).get("maxMonthlyUsageUsd", 0)
        except Exception as e:
            if debug: st.write(f"⚠️ Failed to fetch Apify limit: {e}")

        # 2. Get Usage from /usage/monthly
        usage_usd = 0
        try:
            url_usage = "https://api.apify.com/v2/users/me/usage/monthly"
            resp_usage = requests.get(url_usage, headers=headers, timeout=10)
            if resp_usage.status_code == 200:
                data_usage = resp_usage.json()
                if debug:
                    st.write("🔍 Apify /usage/monthly data:", data_usage)
                
                # Try getting total usage (after discount is more accurate for billing)
                usage_usd = data_usage.get("data", {}).get("totalUsageCreditsUsdAfterVolumeDiscount")
                if usage_usd is None:
                     usage_usd = data_usage.get("data", {}).get("totalUsageCreditsUsdBeforeVolumeDiscount", 0)
        except Exception as e:
            if debug: st.write(f"⚠️ Failed to fetch Apify usage: {e}")

        return float(usage_usd), float(limit_usd)

    except Exception as e:
        if debug:
            st.warning(f"⚠️ Failed to fetch Apify credits: {e}")
        return None, None

def get_apollo_credits(api_key, debug=False):
    """
    Fetch Apollo credits from auth/health endpoint.
    Returns: (credits_left, credits_limit, credits_used) or (None, None, None) on error.
    """
    try:
        url = "https://api.apollo.io/v1/auth/health"
        headers = {"X-Api-Key": api_key}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if debug:
            st.write("🔍 Apollo API Response:", data)
            
        # Since logic to parse credits varies wildly or is unavailable, we just return None calmly
        # unless we find the exact structure we look for.
        user = data.get("user", {})
        team = user.get("team", {})
        
        email_credits_left = team.get("email_credits_left")
        email_credits_limit = team.get("email_credits_limit")
        period_email_credits_usage = team.get("period_email_credits_usage")
        
        if email_credits_left is not None:
             return int(email_credits_left), int(email_credits_limit or 0), int(period_email_credits_usage or 0)
        
        # If we reached here, we didn't find the credits.
        if debug:
            st.warning("⚠️ Apollo credits structure match failed.")
        
        return None, None, None

    except Exception as e:
        if debug:
            st.warning(f"⚠️ Failed to fetch Apollo credits: {e}")
        return None, None, None

def display_credit_dashboard(apify_token, apollo_key, debug=False):
    """
    Display credit dashboard with color-coded metrics.
    Shows Apify USD usage and Apollo credits remaining.
    """
    st.sidebar.markdown("---")
    st.sidebar.header("💰 Credit Dashboard")
    
    # Fetch Apify credits
    apify_usage, apify_limit = get_apify_credits(apify_token, debug=debug)
    
    # Fetch Apollo credits
    apollo_left, apollo_limit, apollo_used = get_apollo_credits(apollo_key, debug=debug)
    
    # Apify Display
    if apify_usage is not None and apify_limit is not None:
        apify_remaining = apify_limit - apify_usage
        apify_percent = (apify_remaining / apify_limit * 100) if apify_limit > 0 else 0
        
        # Color coding: red if < 20%, orange if < 40%, else normal
        if apify_percent < 20:
            apify_color = "🔴"
        elif apify_percent < 40:
            apify_color = "🟠"
        else:
            apify_color = "🟢"
        
        st.sidebar.metric(
            label=f"{apify_color} Apify Usage",
            value=f"${apify_usage:.2f}",
            delta=f"${apify_limit:.2f} limit"
        )
        st.sidebar.caption(f"Remaining: ${apify_remaining:.2f} ({apify_percent:.1f}%)")
    else:
        st.sidebar.metric(
            label="🔴 Apify Usage",
            value="N/A",
            delta="Unable to fetch"
        )
    
    # Apollo Display
    if apollo_left is not None and apollo_limit is not None:
        apollo_percent = (apollo_left / apollo_limit * 100) if apollo_limit > 0 else 0
        
        # Color coding: red if < 20%, orange if < 40%, else normal
        if apollo_percent < 20:
            apollo_color = "🔴"
        elif apollo_percent < 40:
            apollo_color = "🟠"
        else:
            apollo_color = "🟢"
        
        st.sidebar.metric(
            label=f"{apollo_color} Apollo Credits",
            value=f"{apollo_left:,}",
            delta=f"{apollo_limit:,} total"
        )
        st.sidebar.caption(f"Used: {apollo_used:,} ({100 - apollo_percent:.1f}%)")
    else:
        st.sidebar.metric(
            label="🔴 Apollo Credits",
            value="N/A",
            delta="Unable to fetch"
        )

def get_secrets():
    """Safely retrieve secrets or show error."""
    try:
        return {
            "airtable_key": st.secrets["AIRTABLE_API_KEY"],
            "airtable_base": st.secrets["AIRTABLE_BASE_ID"],
            "apify_token": st.secrets["APIFY_TOKEN"],
            "apollo_key": st.secrets["APOLLO_API_KEY"],
        }
    except FileNotFoundError:
        st.error("Secrets file not found. Please create `.streamlit/secrets.toml`.")
        st.stop()
    except KeyError as e:
        st.error(f"Missing secret key: {e}")
        st.stop()

def init_airtable(api_key, base_id):
    """Initialize Airtable API connection."""
    api = Api(api_key)
    table_leads = api.table(base_id, AIRTABLE_TABLE_NAME)
    table_log = api.table(base_id, AIRTABLE_LOG_TABLE_NAME)
    return table_leads, table_log

def get_industry_options(api_key, base_id):
    """Fetch industry dropdown options from Airtable Metadata API."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = response.json().get("tables", [])
        
        for table in tables:
            if table["name"] == AIRTABLE_TABLE_NAME:
                for field in table["fields"]:
                    if field["name"] == "industry":
                        # Check for singleSelect or multipleSelect options
                        options = field.get("options", {}).get("choices", [])
                        return [opt["name"] for opt in options]
        return [] # Fallback if not found
    except Exception as e:
        # Fallback list if API fails
        return ["Marketing", "Software", "Real Estate", "Consulting", "Other"]

def fetch_existing_leads(table_leads):
    """Fetch existing websites and phones for deduplication."""
    try:
        # Fetch only necessary fields to optimize
        records = table_leads.all(fields=["website", "generic_phone"])
        existing_websites = set()
        existing_phones = set()
        
        for r in records:
            fields = r.get("fields", {})
            web = fields.get("website")
            phone = fields.get("generic_phone")
            
            if web:
                existing_websites.add(str(web).strip().lower())
            if phone:
                # Simple normalization: remove non-digits
                p = "".join(filter(str.isdigit, str(phone)))
                if p:
                    existing_phones.add(p)
                    
        return existing_websites, existing_phones
    except Exception as e:
        st.error(f"Error fetching existing leads: {e}")
        return set(), set()

def parse_address_components(address, fallback_city):
    """
    Extracts City and State/Country from address string.
    Normalizes 'Manhattan', 'Brooklyn', etc. to 'New York'.
    Returns (city, state).
    """
    if not address:
        return fallback_city.title(), None
        
    parts = [p.strip() for p in str(address).split(',')]
    
    city = fallback_city.title()
    state = None
    
    # Heuristic for reliable Google Maps addresses:
    # Format usually: "Street, City, WA 98052, Country" or "Street, City, State Zip"
    
    if len(parts) >= 3:
        # Try to parse the second to last part (State Zip)
        state_zip_part = parts[-2]
        possible_city = parts[-3]
        
        # Regex to grab the state code (2 letters uppercase)
        match = re.search(r'\b([A-Z]{2})\b', state_zip_part)
        if match:
            state = match.group(1)
            city = possible_city
        else:
            # Maybe international? Use country as state/region
            # Last part is usually country
            state = parts[-1]
            city = parts[-2]
    elif len(parts) == 2:
        city = parts[0]
        state = parts[1] # Might be country
        
    # --- City Normalization Rules ---
    # 1. Clean up City (remove trailing state if accidentally caught)
    if state and city.endswith(f", {state}"):
        city = city.replace(f", {state}", "").strip()

    # 2. NYC Normalization
    nyc_boroughs = ["Manhattan", "Brooklyn", "Queens", "The Bronx", "Bronx", "Staten Island"]
    if any(b.lower() in city.lower() for b in nyc_boroughs):
        city = "New York"
        if not state: state = "NY"
        
    return city, state

def scrape_apify(token, industry, city, max_leads):
    """Run Apify Google Maps Scraper."""
    client = ApifyClient(token)
    
    # Construct search query
    search_term = f"{industry} in {city}"
    
    run_input = {
        "searchStringsArray": [search_term],
        "maxCrawledPlacesPerSearch": max_leads,
        "language": "en",
        "maxImages": 0, # Optimization
        "oneReviewPerPlace": False, # Optimization
        "skipClosedPlaces": True,
    }
    
    with st.spinner(f"Scraping '{search_term}' via Apify..."):
        run = client.actor("compass/crawler-google-places").call(run_input=run_input)
        
    # Fetch results
    dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items
    return dataset_items

def enrich_apollo(api_key, domain):
    """
    Two-Step Enrichment:
    1. Search to find the best person (Name).
    2. Match to unlock their Email using Name + Domain.
    Returns (name, email, position).
    """
    search_url = "https://api.apollo.io/v1/mixed_people/search"
    match_url = "https://api.apollo.io/v1/people/match"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key
    }
    
    # Step 1: Search for ANY relevant contact
    search_data = {
        "q_organization_domains": domain,
        "page": 1,
        "per_page": 1, 
        "person_titles": ["owner", "founder", "ceo", "director", "partner", "president", "manager"],
        "contact_email_status": ["verified"]
    }
    
    best_name = None
    best_title = None
    best_id = None
    
    try:
        st.write(f"🔎 Debug: 1. Searching for contacts at {domain}...") 
        resp1 = requests.post(search_url, headers=headers, json=search_data)
        
        if resp1.status_code == 200:
            people = resp1.json().get("people", [])
            if people:
                best_name = people[0].get("name")
                best_title = people[0].get("title")
                best_id = people[0].get("id")
                st.write(f"👉 found: {best_name} ({best_title}) [ID: {best_id}]")
            else:
                st.write("⚠️ Search found 0 results.")
                return None, None, None
        else:
            return None, None, None
            
    except Exception:
        return None, None, None

    # Step 2: Unlock with Match using the Exact Person ID
    if best_id:
        match_data = {
            "id": best_id, # FIX: Use ID ensures we get the EXACT person from Step 1
            "reveal_personal_emails": True,
            # "reveal_phone_number": enrich_phones
        }
        
        try:
            st.write(f"🔓 Debug: 2. Unlocking ID {best_id}...")
            resp2 = requests.post(match_url, headers=headers, json=match_data)
            
            if resp2.status_code == 200:
                json_resp = resp2.json()
                person = json_resp.get("person")
                
                if person:
                    email = person.get("email")
                    position = person.get("title") or best_title 
                    st.write(f"✅ Debug: Unlocked {email}")
                    return best_name, email, position
            else:
                st.write(f"❌ Unlock failed: {resp2.status_code}")

        except Exception:
            pass

    return best_name, None, best_title # Return partial info if unlock fails

def execute_with_credit_tracking(secrets, table_leads, table_log, industry, city_input, max_leads, enrich_emails):
    """
    Wrapper function that tracks credit usage before and after execution.
    Returns: (result_data, credit_used_apify, credit_used_apollo)
    where result_data contains: total_scraped, new_added, new_records, status, error_msg
    """
    # Step 1: Snapshot Pre-Credits
    debug_mode = st.session_state.get("debug_mode", False)
    apify_usage_pre, apify_limit_pre = get_apify_credits(secrets["apify_token"], debug=debug_mode)
    apollo_left_pre, apollo_limit_pre, apollo_used_pre = get_apollo_credits(secrets["apollo_key"], debug=debug_mode)
    
    # Initialize result tracking
    result_data = {
        "total_scraped": 0,
        "new_added": 0,
        "new_records": [],
        "status": "Failed",
        "error_msg": ""
    }
    
    try:
        # Step 2: Execute Main Logic
        # 1. Fetch Existing (Deduplication)
        exist_webs, exist_phones = fetch_existing_leads(table_leads)
        
        # 2. Scrape Apify
        raw_leads = scrape_apify(secrets["apify_token"], industry, city_input, max_leads)
        total_scraped = len(raw_leads)
        result_data["total_scraped"] = total_scraped
        
        # 3. Process & Filter
        new_records = []
        
        for item in raw_leads:
            website = item.get("website")
            map_phone = item.get("phoneNumber") or item.get("phone") or item.get("internationalPhoneNumber")
            
            # Check Deduplication
            clean_web = str(website).strip().lower() if website else None
            clean_phone = "".join(filter(str.isdigit, str(map_phone))) if map_phone else None
            
            if (clean_web and clean_web in exist_webs) or (clean_phone and clean_phone in exist_phones):
                continue

            # Address Parsing
            parsed_city, parsed_state = parse_address_components(item.get("address"), city_input)
                
            # Schema Mapping
            record = {
                "company_name": item.get("title"),
                "industry": industry,
                "city": parsed_city,
                "state": parsed_state,
                "website": website,
                "generic_phone": map_phone,
                "rating": item.get("totalScore"),
                "postal_address": item.get("address"),
                "scrapping_tool": SCRAPPING_TOOL_ID,
                "key_contact_name": None,
                "key_contact_email": None,
                "key_contact_position": None
            }
            
            # 4. Enrichment
            if clean_web and enrich_emails:
                apollo_domain = clean_web.split('?')[0].split('#')[0]
                st.write(f"⏳ Debug: Enriching {item.get('title')} ({apollo_domain})...")
                name, email, position = enrich_apollo(secrets["apollo_key"], apollo_domain)
                
                if name: record["key_contact_name"] = name
                if email: record["key_contact_email"] = email
                if position: record["key_contact_position"] = position
            elif not clean_web:
                st.write(f"💨 Debug: Skipped enrichment for {item.get('title')} (No Website)")
                    
            new_records.append(record)
            
            # Add to local cache to prevent dupes within same run
            if clean_web: exist_webs.add(clean_web)
            if clean_phone: exist_phones.add(clean_phone)
        
        # 5. Sync to Airtable
        if new_records:
            table_leads.batch_create(new_records)
        
        result_data["new_added"] = len(new_records)
        result_data["new_records"] = new_records
        result_data["status"] = "Success" if new_records or total_scraped > 0 else "Zero Results"
        
    except Exception as e:
        result_data["status"] = "Failed"
        result_data["error_msg"] = str(e)
        st.error(f"An error occurred: {e}")
    
    # Step 3: Snapshot Post-Credits
    debug_mode = st.session_state.get("debug_mode", False)
    apify_usage_post, apify_limit_post = get_apify_credits(secrets["apify_token"], debug=debug_mode)
    apollo_left_post, apollo_limit_post, apollo_used_post = get_apollo_credits(secrets["apollo_key"], debug=debug_mode)
    
    # Step 4: Calculate Delta
    credit_used_apify = None
    credit_used_apollo = None
    
    if apify_usage_pre is not None and apify_usage_post is not None:
        credit_used_apify = apify_usage_post - apify_usage_pre
        if credit_used_apify < 0:
            credit_used_apify = 0  # Handle edge case where usage might decrease (reset)
    
    if apollo_left_pre is not None and apollo_left_post is not None:
        credit_used_apollo = apollo_left_pre - apollo_left_post
        if credit_used_apollo < 0:
            credit_used_apollo = 0  # Handle edge case
    
    return result_data, credit_used_apify, credit_used_apollo

def log_transaction(table_log, industry, city_input, total_scraped, new_added, enrich_used, status, error_msg="", credit_used_apify=None, credit_used_apollo=None):
    """Write log entry to Airtable with credit tracking."""
    try:
        log_data = {
            "Industry": industry,
            "City Input": city_input,
            "Total Scraped": total_scraped,
            "New Added": new_added,
            "Enrichment Used?": enrich_used,
            "Status": status,
            "Error Message": str(error_msg)
        }
        
        # Add credit usage if provided
        if credit_used_apify is not None:
            log_data["credit_used_apify"] = float(credit_used_apify)
        if credit_used_apollo is not None:
            log_data["credit_used_apollo"] = int(credit_used_apollo)
        
        table_log.create(log_data)
    except Exception as e:
        st.error(f"Failed to write log: {e}")

# --- MAIN APP ---

def main():
    secrets = get_secrets()
    
    # Sidebar
    st.sidebar.header("🔌 Connection Status")
    try:
        table_leads, table_log = init_airtable(secrets["airtable_key"], secrets["airtable_base"])
        st.sidebar.success("Airtable Connected")
    except Exception as e:
        st.sidebar.error(f"Airtable Connection Failed: {e}")
        st.stop()
        
    if secrets["apify_token"]:
        st.sidebar.success("Apify Token Found")
    
    if secrets["apollo_key"]:
        st.sidebar.success("Apollo Key Found")
    
    # Debug mode toggle
    debug_mode = st.sidebar.checkbox("🔍 Debug Mode", value=False, help="Show API response details")
    st.session_state["debug_mode"] = debug_mode
    
    # Display Credit Dashboard
    display_credit_dashboard(secrets["apify_token"], secrets["apollo_key"], debug=debug_mode)

    # Initialization
    if "industry_options" not in st.session_state:
        st.session_state["industry_options"] = get_industry_options(secrets["airtable_key"], secrets["airtable_base"])
        
    # UI
    st.title("🚀 Lead Generation Engine")
    
    # Row 1
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        industry = st.selectbox("Industry", st.session_state["industry_options"] or ["Generic"])
    with col2:
        city_input = st.text_input("City", placeholder="e.g. New York")
    with col3:
        max_leads = st.number_input("Max Leads", min_value=1, max_value=500, value=10, step=1)
        
    # Row 2
    enrich_emails = st.checkbox("✨ Enrich with Emails? (Apollo)", value=False)
    
    # Row 3
    if st.button("Find & Sync Leads", type="primary"):
        if not city_input:
            st.warning("Please enter a city.")
            return
            
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        # Execute with credit tracking
        status_text.text("Initializing credit tracking...")
        progress_bar.progress(5)
        
        result_data, credit_used_apify, credit_used_apollo = execute_with_credit_tracking(
            secrets, table_leads, table_log, industry, city_input, max_leads, enrich_emails
        )
        
        progress_bar.progress(95)
        
        # Log transaction with credit usage
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
            credit_used_apollo=credit_used_apollo
        )
        
        progress_bar.progress(100)
        
        # Display results
        if result_data["status"] == "Success" or result_data["status"] == "Zero Results":
            credit_info = []
            if credit_used_apify is not None:
                credit_info.append(f"Apify: ${credit_used_apify:.4f}")
            if credit_used_apollo is not None:
                credit_info.append(f"Apollo: {credit_used_apollo} credits")
            
            credit_str = f" ({', '.join(credit_info)})" if credit_info else ""
            status_text.success(
                f"✅ Scraped {result_data['total_scraped']}, Added {result_data['new_added']}.{credit_str}"
            )
            
            if result_data["new_records"]:
                st.dataframe(pd.DataFrame(result_data["new_records"]))
            else:
                st.info("No new unique leads found.")
        else:
            status_text.error(f"❌ Execution failed: {result_data['error_msg']}")
        
        # Refresh credit dashboard after execution
        with st.sidebar:
            st.markdown("---")
            st.caption("🔄 Refreshing credit dashboard...")
        display_credit_dashboard(secrets["apify_token"], secrets["apollo_key"], debug=st.session_state.get("debug_mode", False))

if __name__ == "__main__":
    main()
