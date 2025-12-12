import requests
import streamlit as st


BASE_URL = "https://api.instantly.ai"


def _headers(api_key: str):
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


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
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
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
        resp = requests.get(url, headers=headers, params={"limit": 100}, timeout=20)
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
        resp = requests.post(url, headers=headers, json=data, timeout=30)
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
    ensure_campaign_variables(
        api_key,
        campaign_id,
        variables=["postalCode", "jobTitle", "address", "City", "state"],
        debug=debug,
    )

    formatted_leads = []
    for lead in leads:
        name_parts = (lead.get("key_contact_name") or "").split(" ")
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
        # Drop empty values to keep payload clean
        custom_variables = {k: v for k, v in custom_variables.items() if v not in (None, "", [])}

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

    if debug:
        st.write(f"📤 Debug: Sending {len(leads)} leads to Instantly...")
        st.json(payload)

    def _post(payload_to_send):
        return requests.post(url, headers=headers, json=payload_to_send, timeout=30)

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


