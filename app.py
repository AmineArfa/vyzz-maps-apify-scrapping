import streamlit as st
import sys
import subprocess

# DEBUG: Check if apify-client is installed
st.sidebar.write("### 🔍 Debug Info")
st.sidebar.write(f"Python: {sys.version.split()[0]}")
st.sidebar.write(f"Executable: {sys.executable}")

# Check installed packages
try:
    result = subprocess.run([sys.executable, "-m", "pip", "list"], 
                          capture_output=True, text=True, timeout=10)
    installed_packages = result.stdout.lower()
    if "apify-client" in installed_packages:
        st.sidebar.success("✅ apify-client found in pip list")
        # Extract version
        import re
        match = re.search(r'apify-client\s+([\d.]+)', installed_packages)
        if match:
            st.sidebar.write(f"Version: {match.group(1)}")
    else:
        st.sidebar.error("❌ apify-client NOT in pip list")
        st.sidebar.code(installed_packages[:500])  # Show first 500 chars
except Exception as e:
    st.sidebar.warning(f"Could not check packages: {e}")

# Try importing (this will show in sidebar, but app will still try to import below)
try:
    import apify_client
    st.sidebar.success(f"✅ apify_client import successful (v{getattr(apify_client, '__version__', 'unknown')})")
except ImportError as e:
    st.sidebar.error(f"❌ Import failed: {e}")
    st.sidebar.write("### sys.path:")
    for p in sys.path[:5]:  # Show first 5 paths
        st.sidebar.write(f"- {p}")

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
                return None, None, None, None
        else:
            return None, None, None, None
            
    except Exception:
        return None, None, None, None

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


def log_transaction(table_log, industry, city_input, total_scraped, new_added, enrich_used, status, error_msg=""):
    """Write log entry to Airtable."""
    try:
        table_log.create({
            "Industry": industry,
            "City Input": city_input,
            "Total Scraped": total_scraped,
            "New Added": new_added,
            "Enrichment Used?": enrich_used,
            "Status": status,
            "Error Message": str(error_msg)
        })
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
        
        try:
            # 1. Fetch Existing (Deduplication)
            status_text.text("Fetching existing records for deduplication...")
            exist_webs, exist_phones = fetch_existing_leads(table_leads)
            
            # 2. Scrape Apify
            status_text.text("Scraping Google Maps via Apify...")
            raw_leads = scrape_apify(secrets["apify_token"], industry, city_input, max_leads)
            total_scraped = len(raw_leads)
            progress_bar.progress(30)
            
            # 3. Process & Filter
            new_records = []
            
            status_text.text(f"Processing {total_scraped} raw leads...")
            
            for item in raw_leads:
                website = item.get("website")
                # Scrape Map Phone
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
                    "generic_phone": map_phone, # Generic from Maps
                    "rating": item.get("totalScore"),
                    "postal_address": item.get("address"),
                    "scrapping_tool": SCRAPPING_TOOL_ID,
                    "key_contact_name": None,
                    "key_contact_email": None,
                    "key_contact_position": None
                }
                
                # 4. Enrichment
                if clean_web and enrich_emails:
                    # FIX: Strip UTM parameters from URL before sending to Apollo
                    apollo_domain = clean_web.split('?')[0].split('#')[0]
                    st.write(f"⏳ Debug: Enriching {item.get('title')} ({apollo_domain})...")
                    name, email, position = enrich_apollo(secrets["apollo_key"], apollo_domain)
                    
                    if name: record["key_contact_name"] = name
                    if email: record["key_contact_email"] = email
                    if position: record["key_contact_position"] = position # New column mapping
                elif not clean_web:
                    st.write(f"💨 Debug: Skipped enrichment for {item.get('title')} (No Website)")
                        
                new_records.append(record)
                
                # Add to local cache to prevent dupes within same run
                if clean_web: exist_webs.add(clean_web)
                if clean_phone: exist_phones.add(clean_phone)
                
            progress_bar.progress(70)
            
            # 5. Sync to Airtable
            status_text.text(f"Syncing {len(new_records)} new leads to Airtable...")
            if new_records:
                # Batch create handles chunks of 10 automatically in pyairtable usually
                table_leads.batch_create(new_records)
            
            progress_bar.progress(90)
            
            # 6. Log Success
            log_transaction(
                table_log, 
                industry, 
                city_input, 
                total_scraped, 
                len(new_records), 
                enrich_emails, 
                "Success" if new_records or total_scraped > 0 else "Zero Results"
            )
            
            progress_bar.progress(100)
            status_text.success(f"✅ Scraped {total_scraped}, Added {len(new_records)}. (Log updated).")
            
            if new_records:
                st.dataframe(pd.DataFrame(new_records))
            else:
                st.info("No new unique leads found.")
                
        except Exception as e:
            st.error(f"An error occurred: {e}")
            # Log Failure
            try:
                log_transaction(
                    table_log, 
                    industry, 
                    city_input, 
                    0, 
                    0, 
                    enrich_emails, 
                    "Failed", 
                    error_msg=str(e)
                )
            except:
                pass

if __name__ == "__main__":
    main()
