from __future__ import annotations

import requests
import streamlit as st
from pyairtable import Api


def init_airtable(api_key: str, base_id: str, leads_table: str, log_table: str):
    """Initialize Airtable API connection."""
    api = Api(api_key)
    table_leads = api.table(base_id, leads_table)
    table_log = api.table(base_id, log_table)
    return table_leads, table_log


def get_industry_options(api_key: str, base_id: str, leads_table: str):
    """Fetch industry dropdown options from Airtable Metadata API."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        tables = response.json().get("tables", [])

        for table in tables:
            if table.get("id") == leads_table or table.get("name") == leads_table:
                for field in table.get("fields", []):
                    if field.get("name") == "industry":
                        options = field.get("options", {}).get("choices", [])
                        return [opt.get("name") for opt in options if opt.get("name")]
        return []
    except Exception:
        # Fallback list if API fails
        return ["Marketing", "Software", "Real Estate", "Consulting", "Other"]


def get_airtable_table_field_names(api_key: str, base_id: str, table_id_or_name: str):
    """
    Fetch the list of field names for a given Airtable table via Metadata API.
    Returns a set of field names; empty set on failure.
    """
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        tables = response.json().get("tables", [])
        for table in tables:
            if table.get("id") == table_id_or_name or table.get("name") == table_id_or_name:
                return {f.get("name") for f in table.get("fields", []) if f.get("name")}
    except Exception:
        pass
    return set()

def get_airtable_writable_field_names(api_key: str, base_id: str, table_id_or_name: str):
    """
    Fetch writable field names for a given Airtable table via Metadata API.
    Computed/system fields are excluded (formula, rollup, lookup, autonumber, created time, etc.).
    Returns an empty set on failure (caller can decide fallback behavior).
    """
    # Airtable field types that are not writable via API.
    non_writable_types = {
        "autoNumber",
        "barcode",
        "button",
        "count",
        "createdBy",
        "createdTime",
        "externalSyncSource",
        "formula",
        "lastModifiedBy",
        "lastModifiedTime",
        "lookup",
        "multipleLookupValues",
        "rollup",
    }

    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        tables = response.json().get("tables", [])
        for table in tables:
            if table.get("id") == table_id_or_name or table.get("name") == table_id_or_name:
                writable = set()
                for f in table.get("fields", []):
                    name = f.get("name")
                    ftype = f.get("type")
                    if not name:
                        continue
                    if ftype in non_writable_types:
                        continue
                    writable.add(name)
                return writable
    except Exception:
        pass
    return set()


def filter_airtable_fields(record: dict, allowed_fields: set[str]):
    """
    Filters a record dict to only include Airtable fields that exist.
    Also drops None values to avoid Airtable type issues.
    """
    if not isinstance(record, dict):
        return {}
    out = {}
    for k, v in record.items():
        if allowed_fields and k not in allowed_fields:
            continue
        if v is None:
            continue
        out[k] = v
    return out


def fetch_existing_leads(table_leads):
    """Fetch existing websites and phones for deduplication."""
    try:
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
                p = "".join(filter(str.isdigit, str(phone)))
                if p:
                    existing_phones.add(p)

        return existing_websites, existing_phones
    except Exception as e:
        st.error(f"Error fetching existing leads: {e}")
        return set(), set()


def log_transaction(
    table_log,
    industry,
    city_input,
    total_scraped,
    new_added,
    enrich_used,
    status,
    error_msg="",
    credit_used_apify=None,
    credit_used_apollo=None,
    credit_used_instantly=None,
    instantly_added=None,
    search_query=None,
):
    """Write log entry to Airtable with credit tracking."""
    try:
        log_data = {
            "Industry": industry,
            "City Input": city_input,
            "Total Scraped": total_scraped,
            "New Added": new_added,
            "Enrichment Used?": enrich_used,
            "Status": status,
            "Error Message": str(error_msg),
        }

        if search_query:
            log_data["search_query"] = search_query

        if credit_used_apify is not None:
            log_data["credit_used_apify"] = float(credit_used_apify)
        if credit_used_apollo is not None:
            log_data["credit_used_apollo"] = int(credit_used_apollo)
        if credit_used_instantly is not None:
            log_data["credit_used_instantly"] = int(credit_used_instantly)
        if instantly_added is not None:
            log_data["Instantly Added"] = int(instantly_added)

        if st.session_state.get("debug_mode"):
            st.write("📝 Debug: Writing Log to Airtable:", log_data)

        table_log.create(log_data)
    except Exception as e:
        msg = f"Failed to write log: {e}"
        st.error(msg)
        if st.session_state.get("debug_mode"):
            st.write("❌ Log Error Details:", e)


