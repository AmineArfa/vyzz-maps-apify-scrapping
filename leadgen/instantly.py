from __future__ import annotations

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

# Module-level campaign management to prevent duplicates across all threads/calls
_campaign_cache_lock = threading.Lock()
_campaign_cache: dict[str, str] = {}  # name -> id
_campaign_cache_loaded = False


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


def _list_all_campaigns(api_key, debug=False, search=None):
    """
    Fetch campaigns with cursor pagination.

    Instantly v2 uses `starting_after` (the id of the last item on the
    previous page), NOT `skip`. Passing `skip` is silently ignored, so
    every page returns the same first batch — the loop then spins until
    max_pages × per-request timeout, which looks like a hang.

    `search` (optional) narrows the listing by campaign name. The cold-
    cache lookup in find_or_create can use this fast-path to fetch only
    the few campaigns matching a known name instead of the entire account.

    Returns list of campaign dicts, or None on failure.
    """
    headers = _headers(api_key)
    url = f"{BASE_URL}/api/v2/campaigns"
    all_campaigns: list[dict] = []
    starting_after: str | None = None
    limit = 100
    max_pages = 50  # safety: ~5000 campaigns

    for _ in range(max_pages):
        try:
            params: dict = {"limit": limit}
            if starting_after:
                params["starting_after"] = starting_after
            if search:
                params["search"] = search
            resp = _request_with_retry(
                "GET", url, headers=headers, params=params, timeout=20,
            )
            if resp.status_code != 200:
                if debug:
                    st.write(f"⚠️ Campaign list failed: {resp.status_code}")
                return None

            payload = resp.json()
            items = payload.get("items", payload if isinstance(payload, list) else [])
            if not items:
                break

            all_campaigns.extend(items)

            # Page short OR no next cursor → done.
            next_cursor = payload.get("next_starting_after")
            if next_cursor:
                starting_after = next_cursor
            elif len(items) < limit:
                break
            else:
                starting_after = items[-1].get("id")
                if not starting_after:
                    break

        except Exception as e:
            if debug:
                st.write(f"⚠️ Campaign list exception: {e}")
            return None

    return all_campaigns


def _load_campaign_cache(api_key, debug=False):
    """
    Load all existing campaigns into the module-level cache.
    Should be called once at the start of a sync session.
    Thread-safe.
    """
    global _campaign_cache_loaded
    
    with _campaign_cache_lock:
        if _campaign_cache_loaded:
            return True  # Already loaded
        
        campaigns = _list_all_campaigns(api_key, debug=debug)
        if campaigns is None:
            return False  # Failed to load
        
        for c in campaigns:
            name = c.get("name")
            cid = c.get("id")
            if name and cid:
                _campaign_cache[name] = cid
        
        _campaign_cache_loaded = True
        if debug:
            st.write(f"📋 Loaded {len(_campaign_cache)} existing campaigns into cache")
        return True


def reset_campaign_cache():
    """Reset the campaign cache. Call this at the start of a new sync session."""
    global _campaign_cache_loaded
    with _campaign_cache_lock:
        _campaign_cache.clear()
        _campaign_cache_loaded = False


def _search_campaign_by_exact_name(api_key, campaign_name, log=None):
    """Single-page search for a campaign by exact name. Returns id or None.

    No pagination loop — one HTTP request bounded by the underlying
    timeout/retry policy. The pagination loop is what made the previous
    implementation appear to hang for minutes when Instantly's response
    didn't shape pagination as expected.
    """
    headers = _headers(api_key)
    url = f"{BASE_URL}/api/v2/campaigns"
    try:
        resp = _request_with_retry(
            "GET", url, headers=headers,
            params={"search": campaign_name, "limit": 50},
            timeout=15,
        )
    except Exception as e:
        if log:
            log(f"⚠️ Search failed: {e}")
        return None
    if resp.status_code != 200:
        if log:
            log(f"⚠️ Search HTTP {resp.status_code}: {resp.text[:200]}")
        return None
    payload = resp.json()
    items = payload.get("items", payload if isinstance(payload, list) else [])
    if log:
        log(f"🔎 Search returned {len(items)} candidate(s) for '{campaign_name}'.")
    for c in items:
        if c.get("name") == campaign_name:
            return c.get("id")
    return None


def find_or_create_instantly_campaign(api_key, campaign_name, debug=False, log=None):
    """Find a campaign by exact name or create it. Returns campaign_id or None.

    Bounded path:
      1. Cache hit  → instant.
      2. Single-page search by name → if exact match, cache + return.
      3. Create     → on success cache + return; on race (already exists),
                      retry the search to recover the id.

    `log` is an optional callable that receives one-line status updates so
    callers can render progress in a Streamlit st.status block. `debug` is
    kept as a backwards-compatible shortcut that routes the same messages
    to st.write when no callback is provided.
    """
    if not api_key:
        return None

    def _log(msg: str) -> None:
        if log is not None:
            try:
                log(msg)
            except Exception:
                pass
        elif debug:
            try:
                st.write(msg)
            except Exception:
                pass

    headers = _headers(api_key)

    with _campaign_cache_lock:
        # 1. Cache hit
        if campaign_name in _campaign_cache:
            _log(f"✅ Cache hit: '{campaign_name}'")
            return _campaign_cache[campaign_name]

        # 2. Search by exact name (single page)
        _log(f"🔎 Searching Instantly for '{campaign_name}'...")
        existing_id = _search_campaign_by_exact_name(api_key, campaign_name, log=_log)
        if existing_id:
            _campaign_cache[campaign_name] = existing_id
            _log(f"✅ Found existing: {existing_id}")
            return existing_id

        # 3. Create
        _log(f"➕ Creating new campaign '{campaign_name}'...")
        try:
            url = f"{BASE_URL}/api/v2/campaigns"
            data = {"name": campaign_name, "campaign_schedule": _default_campaign_schedule()}
            resp = _request_with_retry(
                "POST", url, headers=headers, json_payload=data, timeout=20,
            )
        except Exception as e:
            _log(f"⚠️ Create exception: {e}")
            return None

        if 200 <= resp.status_code < 300:
            new_c = resp.json()
            c_id = new_c.get("id") or new_c.get("data", {}).get("id")
            if c_id:
                _campaign_cache[campaign_name] = c_id
                _log(f"✅ Created: {c_id}")
                return c_id
            _log(f"⚠️ Create returned {resp.status_code} but no id: {resp.text[:200]}")
            return None

        # Race: a concurrent caller created it between our search and create.
        # Recover the id with a second search instead of returning None.
        if resp.status_code in (400, 409, 422):
            _log(f"⚠️ Create rejected ({resp.status_code}); re-searching for race recovery.")
            recovered = _search_campaign_by_exact_name(api_key, campaign_name, log=_log)
            if recovered:
                _campaign_cache[campaign_name] = recovered
                _log(f"✅ Recovered after race: {recovered}")
                return recovered

        _log(f"❌ Create failed {resp.status_code}: {resp.text[:200]}")
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
            variables=["postalCode", "jobTitle", "address", "City", "state",
                       "competitor1", "competitor2", "competitor3", "lid",
                       "industry", "ticket_tier"],
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
        industry_val = lead.get("industry")
        if isinstance(industry_val, list):
            industry_val = industry_val[0] if industry_val else None
        ticket_tier_val = lead.get("ticket_tier")
        if isinstance(ticket_tier_val, list):
            ticket_tier_val = ticket_tier_val[0] if ticket_tier_val else None
        custom_variables = {
            "postalCode": lead.get("postal_code"),
            "jobTitle": lead.get("key_contact_position"),
            "address": lead.get("postal_address"),
            "City": lead.get("city"),
            "state": lead.get("state"),
            "competitor1": lead.get("competitor1"),
            "competitor2": lead.get("competitor2"),
            "competitor3": lead.get("competitor3"),
            "industry": industry_val,
            "ticket_tier": ticket_tier_val,
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


def search_lead_by_email(api_key: str, email: str, campaign_id: str | None = None, debug: bool = False):
    """Find a lead by exact email via POST /api/v2/leads/list.

    Returns: (lead_dict, error_str) — lead_dict is None if not found.

    Critical: the v2 API request body has NO `email` field. The previous
    implementation sent `{"email": ...}` which Instantly silently ignored;
    the response was the first lead in the account regardless of what we
    asked for, so reconcile-by-email linked thousands of raw rows to the
    same arbitrary Instantly lead. The correct field is `contacts: [email]`
    (per the OpenAPI spec). We additionally verify the returned lead's
    email matches our query as a defense-in-depth check.
    """
    if not api_key or not email:
        return None, "Missing api_key or email"

    target = email.strip().lower()
    url = f"{BASE_URL}/api/v2/leads/list"
    headers = _headers(api_key)

    payload: dict = {
        "contacts": [target],
        "limit": 5,
    }
    if campaign_id:
        payload["campaign"] = campaign_id

    try:
        resp = _request_with_retry("POST", url, headers=headers, json_payload=payload, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", data if isinstance(data, list) else [])
            for lead in items:
                lead_email = (lead.get("email") or "").strip().lower()
                # Defense-in-depth: drop the response on the floor if the
                # email doesn't match. A future API rename or a partial
                # match must never silently link the wrong lead back.
                if lead_email == target:
                    if debug:
                        st.write(f"🔍 Found existing lead by email: {target} -> {lead.get('id')}")
                    return lead, None
            return None, None
        err = f"Instantly search lead failed: {resp.status_code} - {resp.text[:200]}"
        if debug:
            st.write(f"⚠️ {err}")
        return None, err
    except Exception as e:
        err = f"Instantly search lead exception: {e}"
        if debug:
            st.write(f"⚠️ {err}")
        return None, err


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


def inject_lid_to_lead(api_key, lead_id, debug=False):
    """
    Inject the lead's own Instantly system ID as a 'lid' custom variable.
    This makes {{lid}} available as a merge variable in email templates,
    enabling closed-loop click tracking: email link → ?lid={{lid}} → audit → Airtable.

    IMPORTANT: custom_variables REPLACES the entire object on update.
    We must read existing variables first, merge, then write back.
    """
    if not api_key or not lead_id or not is_valid_uuid(lead_id):
        return False, "Missing api_key or invalid lead_id"

    # Step 1: Fetch current lead to get existing custom_variables
    lead_data, err = get_lead_from_instantly(api_key, lead_id, debug=debug)
    if not lead_data:
        return False, f"Cannot fetch lead to inject lid: {err}"

    # Step 2: Merge lid into existing payload (payload = custom_variables)
    existing_vars = lead_data.get("payload") or {}
    if existing_vars.get("lid") == lead_id:
        # Already has correct lid — skip
        if debug:
            st.write(f"⏭️ Lead {lead_id} already has lid set, skipping.")
        return True, None

    merged_vars = {**existing_vars, "lid": lead_id}

    # Step 3: Update lead with merged custom_variables
    patch_payload = {"custom_variables": merged_vars}
    success, update_err = update_lead_in_instantly(api_key, lead_id, patch_payload, debug=debug)
    if success:
        if debug:
            st.write(f"✅ Injected lid={lead_id[:12]}... into lead custom_variables")
        return True, None
    return False, f"Failed to inject lid: {update_err}"


def list_contacted_unreplied_leads(api_key, *, limit_per_page=100, log=None, on_progress=None):
    """Iterate ALL Instantly leads that have been contacted at least once
    and have no replies. Returns the full list.

    Uses POST /api/v2/leads/list with `filter=FILTER_VAL_CONTACTED` (server-
    side narrowing to leads that received at least one email), then filters
    each page in Python for `email_reply_count == 0`. Account-wide pagination
    via `starting_after` cursor.

    `on_progress(loaded_so_far)` is called after each page so the UI can
    refresh a counter. `log(msg)` receives one-line status updates.
    """
    if not api_key:
        return []

    def _log(msg: str) -> None:
        if log is not None:
            try: log(msg)
            except Exception: pass

    url = f"{BASE_URL}/api/v2/leads/list"
    headers = _headers(api_key)
    starting_after: str | None = None
    out: list[dict] = []
    page = 0
    max_pages = 500  # ~50k leads cap; adjust if account is larger

    while page < max_pages:
        body: dict = {
            "limit": int(limit_per_page),
            "filter": "FILTER_VAL_CONTACTED",
        }
        if starting_after:
            body["starting_after"] = starting_after
        try:
            resp = _request_with_retry("POST", url, headers=headers, json_payload=body, timeout=30)
        except Exception as e:
            _log(f"⚠️ list_contacted_unreplied exception: {e}")
            break
        if resp.status_code != 200:
            _log(f"⚠️ list_contacted_unreplied HTTP {resp.status_code}: {resp.text[:200]}")
            break
        payload = resp.json()
        items = payload.get("items", payload if isinstance(payload, list) else [])
        if not items:
            break

        kept = 0
        for c in items:
            try:
                if int(c.get("email_reply_count") or 0) == 0:
                    out.append(c)
                    kept += 1
            except (TypeError, ValueError):
                continue

        page += 1
        if on_progress is not None:
            try: on_progress(len(out))
            except Exception: pass

        next_cursor = payload.get("next_starting_after")
        if next_cursor:
            starting_after = next_cursor
        elif len(items) < limit_per_page:
            break
        else:
            starting_after = items[-1].get("id")
            if not starting_after:
                break

    _log(f"📊 Listed {len(out)} contacted-unreplied leads across {page} page(s).")
    return out


def bulk_move_leads_to_campaign(
    api_key, lead_ids, to_campaign_id, *, from_campaign_id, debug=False,
):
    """Move many leads at once via Instantly's bulk move endpoint.

    POST /api/v2/leads/move requires BOTH:
      - `ids`: lead ids to act on (acts as a FILTER, not a standalone selector)
      - `campaign`: the source campaign the ids must currently sit in
      - `to_campaign_id`: the destination

    Per the OpenAPI spec: "When using `ids`, you must provide either
    `campaign` or `list_id` to specify which campaign or list to filter
    the leads from. This parameter acts as a filter within the specified
    campaign or list, not as a standalone way to select leads."

    The earlier impl omitted `campaign`, so Instantly filtered against
    "no source" → empty set → silent zero moves on every call.

    Custom variables (lid, industry, ticket_tier, etc.) are preserved —
    the call only changes campaign membership server-side. Returns
    (success: bool, error: str | None).
    """
    if not api_key or not lead_ids or not to_campaign_id or not from_campaign_id:
        return False, "Missing api_key, lead_ids, from_campaign_id, or to_campaign_id"
    if not is_valid_uuid(to_campaign_id):
        return False, f"Invalid to_campaign_id format: {to_campaign_id}"
    if not is_valid_uuid(from_campaign_id):
        return False, f"Invalid from_campaign_id format: {from_campaign_id}"

    # Dedup ids — the same Instantly lead can be referenced by many
    # raw.scraped_leads rows (duplicate-email imports). Sending dups
    # may cause partial-success ambiguity.
    seen: set[str] = set()
    valid_ids: list[str] = []
    for lid in lead_ids:
        if isinstance(lid, str) and is_valid_uuid(lid) and lid not in seen:
            seen.add(lid)
            valid_ids.append(lid)
    if not valid_ids:
        return False, "No valid lead UUIDs in batch"

    url = f"{BASE_URL}/api/v2/leads/move"
    headers = _headers(api_key)
    payload = {
        "ids": valid_ids,
        "campaign": from_campaign_id,       # SOURCE filter (required when ids is set)
        "to_campaign_id": to_campaign_id,   # DESTINATION
    }

    try:
        resp = _request_with_retry(
            "POST", url, headers=headers, json_payload=payload, timeout=30,
        )
        if 200 <= resp.status_code < 300:
            if debug:
                st.write(
                    f"➡️ Bulk-moved {len(valid_ids)} leads "
                    f"{from_campaign_id[:8]}… → {to_campaign_id[:8]}…"
                )
            return True, None
        err = f"Bulk move failed: {resp.status_code} - {resp.text[:200]}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err
    except Exception as e:
        err = f"Bulk move exception: {e}"
        if debug:
            st.write(f"⚠️ {err}")
        return False, err


def move_lead_to_campaign(api_key, lead_id, to_campaign_id, *, from_campaign_id=None, debug=False):
    """Move a single Instantly lead from `from_campaign_id` to `to_campaign_id`.

    The move endpoint REQUIRES the source campaign even for a single id.
    If `from_campaign_id` is None we look it up first via GET /leads/{id};
    callers that already have it (the per-lead fallback inside
    push_leads_to_campaign) should pass it explicitly to avoid the extra
    round trip.
    """
    if not api_key or not lead_id or not to_campaign_id:
        return False, "Missing api_key, lead_id, or to_campaign_id"
    if not is_valid_uuid(lead_id):
        return False, f"Invalid Lead ID format: {lead_id}"
    if not is_valid_uuid(to_campaign_id):
        return False, f"Invalid to_campaign_id format: {to_campaign_id}"

    if from_campaign_id is None:
        existing, get_err = get_lead_from_instantly(api_key, lead_id, debug=debug)
        if not existing:
            return False, f"Cannot resolve source campaign: {get_err}"
        from_campaign_id = existing.get("campaign")
    if not from_campaign_id or not is_valid_uuid(from_campaign_id):
        return False, "Lead has no resolvable source campaign id"

    return bulk_move_leads_to_campaign(
        api_key, [lead_id], to_campaign_id,
        from_campaign_id=from_campaign_id, debug=debug,
    )


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