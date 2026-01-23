import time
import threading

import requests
import streamlit as st

from .json_sanitize import sanitize_for_json


BASE_URL = "https://api.instantly.ai"


def _headers(api_key: str):
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

_campaign_vars_lock = threading.Lock()
_campaign_vars_registered: set[str] = set()


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict,
    params: dict | None = None,
    json_payload=None,
    timeout: int = 30,
    retries: int = 4,
    backoff_s: float = 1.0,
):
    """
    Thin retry wrapper for Instantly requests.
    Retries on 429 and transient 5xx / network errors.
    """
    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_payload,
                timeout=timeout,
            )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait_s = float(retry_after) if retry_after else backoff_s * (2**attempt)
                except Exception:
                    wait_s = backoff_s * (2**attempt)
                time.sleep(min(wait_s, 30))
                continue

            if 500 <= resp.status_code < 600 and attempt < retries:
                time.sleep(min(backoff_s * (2**attempt), 10))
                continue

            return resp
        except Exception as e:
            last_exc = e
            if attempt >= retries:
                raise
            time.sleep(min(backoff_s * (2**attempt), 10))

    if last_exc:
        raise last_exc

    raise RuntimeError("request retry loop ended unexpectedly")


def _default_campaign_schedule(timezone: str = "America/Chicago"):
    """
    Minimal valid campaign_schedule per Instantly v2 OpenAPI:
    - campaign_schedule.schedules[] requires: name, timing{from,to}, days{...}, timezone
    """
    return {
        "schedules": [
            {
                "name": "Default Schedule",
                "timing": {"from": "09:00", "to": "17:00"},
                "days": {"1": True, "2": True, "3": True, "4": True, "5": True, "6": False, "0": False},
                "timezone": timezone,
            }
        ]
    }


def ensure_campaign_variables(api_key: str, campaign_id: str, variables: list[str], debug: bool = False):
    """
    Register variables on a campaign (Instantly v2: POST /api/v2/campaigns/{id}/variables).
    This helps avoid repeated variable schema churn when importing leads with custom_variables.
    Safe to call multiple times.
    """
    if not api_key or not campaign_id or not variables:
        return False, "Missing api_key/campaign_id/variables"

    url = f"{BASE_URL}/api/v2/campaigns/{campaign_id}/variables"
    headers = _headers(api_key)
    payload = {"variables": variables}

    try:
        resp = _request_with_retry("POST", url, headers=headers, json_payload=payload, timeout=20)
        if resp.status_code == 200:
            if debug:
                st.write(f"✅ Registered campaign variables ({len(variables)})")
            return True, None
        err = f"Instantly variables register failed: {resp.status_code} - {resp.text}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err
    except Exception as e:
        err = f"Instantly variables register exception: {e}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err


def find_or_create_instantly_campaign(api_key, campaign_name, debug=False):
    """Finds a campaign by name or creates it. Returns: campaign_id"""
    if not api_key:
        return None

    headers = _headers(api_key)

    try:
        # Instantly v2: GET /api/v2/campaigns
        url = f"{BASE_URL}/api/v2/campaigns"
        resp = _request_with_retry("GET", url, headers=headers, params={"limit": 100}, timeout=20)
        if resp.status_code == 200:
            payload = resp.json()
            campaigns = payload.get("items", payload if isinstance(payload, list) else [])
            for c in campaigns:
                if c.get("name") == campaign_name:
                    if debug:
                        st.write(f"✅ Found existing campaign: {campaign_name}")
                    return c.get("id")
    except Exception as e:
        if debug:
            st.write(f"⚠️ Failed to list campaigns: {e}")

    try:
        # Instantly v2: POST /api/v2/campaigns requires name + campaign_schedule
        url = f"{BASE_URL}/api/v2/campaigns"
        data = {"name": campaign_name, "campaign_schedule": _default_campaign_schedule()}
        resp = _request_with_retry("POST", url, headers=headers, json_payload=data, timeout=30)
        if resp.status_code == 200:
            new_c = resp.json()
            c_id = new_c.get("id") or new_c.get("data", {}).get("id")
            if debug:
                st.write(f"✅ Created new campaign: {campaign_name} ({c_id})")
            return c_id
        if debug:
            st.write(f"❌ Campaign create failed: {resp.status_code} - {resp.text}")
    except Exception as e:
        if debug:
            st.write(f"⚠️ Failed to create campaign: {e}")

    return None


def export_leads_to_instantly(api_key, campaign_id, leads, debug=False):
    """
    Export a batch of leads to Instantly (bulk add).
    Returns: (created_count, created_leads, raw_response_json_or_none, error_str_or_none)
    """
    if not api_key or not campaign_id or not leads:
        return 0, [], None, "Missing api_key, campaign_id, or leads"

    # Instantly v2: POST /api/v2/leads/add (NOT /leads/list which is listLeads)
    url = f"{BASE_URL}/api/v2/leads/add"
    headers = _headers(api_key)

    # Ensure campaign variables are known ahead of import (non-blocking if it fails).
    # Cache per campaign to avoid hammering the API on large runs.
    with _campaign_vars_lock:
        should_register = campaign_id not in _campaign_vars_registered
        if should_register:
            _campaign_vars_registered.add(campaign_id)
    if should_register:
        ensure_campaign_variables(
            api_key,
            campaign_id,
            variables=["postalCode", "jobTitle", "address", "City", "state"],
            debug=debug,
        )

    formatted_leads = []
    for lead in leads:
        lead = sanitize_for_json(lead)  # critical: strips NaN/NaT/Infinity before requests JSON encoding
        raw_name = lead.get("key_contact_name")
        if not isinstance(raw_name, str):
            raw_name = ""
        
        name_parts = raw_name.split(" ")
        first_name = name_parts[0] if name_parts else ""
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Instantly v2 supports custom_variables for arbitrary metadata.
        # This is the safest way to store extra fields like postalCode/jobTitle/address/city/state.
        custom_variables = {
            "postalCode": lead.get("postal_code"),
            "jobTitle": lead.get("key_contact_position"),
            "address": lead.get("postal_address"),
            "City": lead.get("city"),
            "state": lead.get("state"),
        }
        
        # Drop empty values to keep payload clean. Also drop NaNs (float) to avoid JSON errors or "nan" strings.
        # NaN != NaN is the standard python check for nan float.
        def is_valid(v):
            if v in (None, "", [], "[undefined]"):
                return False
            return True

        custom_variables = {k: v for k, v in custom_variables.items() if is_valid(v)}

        formatted_leads.append(
            {
                "email": lead.get("key_contact_email"),
                "first_name": first_name,
                "last_name": last_name,
                "company_name": lead.get("company_name"),
                "website": lead.get("website"),
                "phone": lead.get("generic_phone"),
                # v2 schema does not accept arbitrary top-level fields like job_title/location.
                # Store extras in custom_variables instead.
                "custom_variables": custom_variables or None,
            }
        )

    payload = {"campaign_id": campaign_id, "skip_if_in_campaign": True, "leads": formatted_leads}
    payload = sanitize_for_json(payload)

    if debug:
        st.write(f"📤 Debug: Sending {len(leads)} leads to Instantly...")
        st.json(payload)

    def _post(payload_to_send):
        return _request_with_retry("POST", url, headers=headers, json_payload=payload_to_send, timeout=30)

    try:
        resp = _post(payload)
        if resp.status_code == 200:
            data = resp.json()
            created = data.get("created_leads", []) or []
            if debug:
                st.write(f"✅ Instantly bulk add OK. Created: {len(created)}")
                st.json(data)
            return len(created), created, data, None
        else:
            # Fallback: retry once without custom_variables (do not block export)
            err1 = f"Instantly export failed: {resp.status_code} - {resp.text}"
            if debug:
                st.write(f"❌ {err1}")

            payload_no_custom = dict(payload)
            payload_no_custom["leads"] = [dict(l, custom_variables=None) for l in formatted_leads]
            resp2 = _post(payload_no_custom)
            if resp2.status_code == 200:
                data = resp2.json()
                created = data.get("created_leads", []) or []
                if debug:
                    st.write(f"✅ Instantly bulk add OK (fallback without custom_variables). Created: {len(created)}")
                    st.json(data)
                # return success but keep first error as warning in error_str
                return len(created), created, data, f"{err1} | Retried without custom_variables: success"

            err2 = f"Instantly export failed (fallback): {resp2.status_code} - {resp2.text}"
            if debug:
                st.write(f"❌ {err2}")
            return 0, [], None, f"{err1} | {err2}"
    except Exception as e:
        err = f"Instantly export exception: {e}"
        if debug:
            st.write(f"⚠️ {err}")
        return 0, [], None, err


def get_lead_from_instantly(api_key, lead_id, debug=False):
    """
    Retrieve lead details from Instantly.
    """
    if not api_key or not lead_id:
        return None, "Missing api_key or lead_id"

    if not is_valid_uuid(lead_id):
        return None, f"Invalid Lead ID format: {lead_id}"

    url = f"{BASE_URL}/api/v2/leads/{lead_id}"
    headers = _headers(api_key)

    try:
        resp = _request_with_retry("GET", url, headers=headers, timeout=20)
        if resp.status_code == 200:
            return resp.json(), None
        return None, f"Instantly get lead failed: {resp.status_code} - {resp.text}"
    except Exception as e:
        return None, f"Instantly get lead exception: {e}"


def is_valid_uuid(val):
    """Simple check if string looks like a UUID (Instantly requirement)."""
    if not isinstance(val, str): return False
    # Typical UUID: 8-4-4-4-12 chars
    parts = val.split("-")
    return len(parts) == 5 and len(val) == 36


def update_lead_in_instantly(api_key, lead_id, lead_data, debug=False):
    """
    Update an existing lead in Instantly using PATCH.
    lead_data should be formatted correctly for the API.
    """
    if not api_key or not lead_id or not lead_data:
        return False, "Missing api_key, lead_id, or lead_data"

    if not is_valid_uuid(lead_id):
        return False, f"Invalid Lead ID format (not a UUID): {lead_id}"

    url = f"{BASE_URL}/api/v2/leads/{lead_id}"
    headers = _headers(api_key)

    try:
        lead_data = sanitize_for_json(lead_data)
        resp = _request_with_retry("PATCH", url, headers=headers, json_payload=lead_data, timeout=20)
        if resp.status_code == 200:
            if debug:
                st.write(f"✅ Updated lead {lead_id} in Instantly.")
            return True, None
        err = f"Instantly lead update failed: {resp.status_code} - {resp.text}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err
    except Exception as e:
        err = f"Instantly lead update exception: {e}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err


def delete_lead_from_instantly(api_key, lead_id, debug=False):
    """
    Delete a lead from Instantly.
    """
    if not api_key or not lead_id:
        return False, "Missing api_key or lead_id"

    if not is_valid_uuid(lead_id):
        return False, f"Invalid Lead ID format (not a UUID): {lead_id}"

    url = f"{BASE_URL}/api/v2/leads/{lead_id}"
    # Remove Content-Type if body is empty to avoid FST_ERR_CTP_EMPTY_JSON_BODY
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        resp = _request_with_retry("DELETE", url, headers=headers, timeout=20)
        if resp.status_code == 200 or resp.status_code == 204:
            if debug:
                st.write(f"✅ Deleted lead {lead_id} from Instantly.")
            return True, None
        err = f"Instantly lead delete failed: {resp.status_code} - {resp.text}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err
    except Exception as e:
        err = f"Instantly lead delete exception: {e}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err