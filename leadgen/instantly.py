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

    formatted_leads = []
    for lead in leads:
        name_parts = (lead.get("key_contact_name") or "").split(" ")
        first_name = name_parts[0] if name_parts else ""
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        formatted_leads.append(
            {
                "email": lead.get("key_contact_email"),
                "first_name": first_name,
                "last_name": last_name,
                "company_name": lead.get("company_name"),
                "website": lead.get("website"),
                "phone": lead.get("generic_phone"),
                "job_title": lead.get("key_contact_position"),
                "location": f"{lead.get('city')}, {lead.get('state')}",
            }
        )

    payload = {"campaign_id": campaign_id, "skip_if_in_campaign": True, "leads": formatted_leads}

    if debug:
        st.write(f"📤 Debug: Sending {len(leads)} leads to Instantly...")
        st.json(payload)

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            created = data.get("created_leads", []) or []
            if debug:
                st.write(f"✅ Instantly bulk add OK. Created: {len(created)}")
                st.json(data)
            return len(created), created, data, None
        else:
            err = f"Instantly export failed: {resp.status_code} - {resp.text}"
            if debug:
                st.write(f"❌ {err}")
            return 0, [], None, err
    except Exception as e:
        err = f"Instantly export exception: {e}"
        if debug:
            st.write(f"⚠️ {err}")
        return 0, [], None, err


