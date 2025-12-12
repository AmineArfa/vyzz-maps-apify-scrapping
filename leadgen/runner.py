from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st

from .airtable_utils import fetch_existing_leads, filter_airtable_fields, get_airtable_writable_field_names
from .apollo import enrich_apollo
from .apify_scraper import scrape_apify
from .credits import get_apify_credits
from .instantly import export_leads_to_instantly, find_or_create_instantly_campaign
from .parsing import parse_address_components


def execute_with_credit_tracking(
    secrets,
    table_leads,
    industry,
    city_input,
    max_leads,
    enrich_emails,
    dashboard,
    *,
    leads_table_id: str,
    scrapping_tool_id: str,
):
    """
    Wrapper function that tracks credit usage before and after execution.
    Returns: (result_data, credit_used_apify, credit_used_apollo, credit_used_instantly)
    """
    dashboard.update_status("Snapshotting API Credits...", 5)
    debug_mode = st.session_state.get("debug_mode", False)
    apify_usage_pre, _ = get_apify_credits(secrets["apify_token"], debug=debug_mode)

    result_data = {
        "total_scraped": 0,
        "new_added": 0,
        "new_records": [],
        "status": "Failed",
        "error_msg": "",
        "apollo_calls_estimated": 0,
        "instantly_added": 0,
    }

    try:
        # Only allow fields that Airtable metadata reports as writable.
        # If metadata lookup fails, we still proceed but rely on safe defaults (we omit known computed fields).
        allowed_leads_fields = get_airtable_writable_field_names(
            secrets["airtable_key"], secrets["airtable_base"], leads_table_id
        )

        def set_if_allowed(record: dict, field_name: str, value):
            """Set a field only if the Airtable schema contains it (or schema lookup failed)."""
            if value is None:
                return
            if allowed_leads_fields and field_name not in allowed_leads_fields:
                return
            record[field_name] = value

        dashboard.update_status("Fetching existing leads for deduplication...", 10)
        exist_webs, exist_phones = fetch_existing_leads(table_leads)

        dashboard.update_status(f"Scraping '{industry} in {city_input}' via Apify...", 20)
        raw_leads = scrape_apify(secrets["apify_token"], industry, city_input, max_leads)
        total_scraped = len(raw_leads)

        result_data["total_scraped"] = total_scraped
        dashboard.stats["Total Scraped"] = total_scraped
        dashboard.refresh_metrics()

        if total_scraped == 0:
            dashboard.update_status("No leads found from scraper.", 100)
            return result_data, 0, 0, 0

        dashboard.update_status(f"Processing {total_scraped} leads...", 30)

        leads_to_enrich = []
        processed_records = []

        for item in raw_leads:
            website = item.get("website")
            title = item.get("title", "Unknown Company")
            map_phone = item.get("phoneNumber") or item.get("phone") or item.get("internationalPhoneNumber")

            clean_web = str(website).strip().lower() if website else None
            clean_phone = "".join(filter(str.isdigit, str(map_phone))) if map_phone else None

            if (clean_web and clean_web in exist_webs) or (clean_phone and clean_phone in exist_phones):
                dashboard.update_metric("Skipped")
                continue

            parsed_city, parsed_state = parse_address_components(item.get("address"), city_input)

            record = {
                "company_name": title,
                "industry": industry,
                "city": parsed_city,
                "state": parsed_state,
                "website": website,
                "generic_phone": map_phone,
                "rating": item.get("totalScore"),
                "postal_address": item.get("address"),
                "scrapping_tool": scrapping_tool_id,
                "key_contact_name": None,
                "key_contact_email": None,
                "key_contact_position": None,
            }

            record = filter_airtable_fields(record, allowed_leads_fields)
            processed_records.append(record)

            if clean_web and enrich_emails:
                apollo_domain = clean_web.split("?")[0].split("#")[0]
                leads_to_enrich.append((len(processed_records) - 1, apollo_domain, title))

            if clean_web:
                exist_webs.add(clean_web)
            if clean_phone:
                exist_phones.add(clean_phone)

        if leads_to_enrich:
            total_enrich = len(leads_to_enrich)
            dashboard.update_status(f"Enriching {total_enrich} leads in parallel...", 40)

            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_idx = {
                    executor.submit(enrich_apollo, secrets["apollo_key"], domain): (idx, title)
                    for idx, domain, title in leads_to_enrich
                }

                completed_count = 0
                for future in as_completed(future_to_idx):
                    idx, title = future_to_idx[future]
                    completed_count += 1

                    prog = 40 + int((completed_count / total_enrich) * 45)
                    dashboard.progress_bar.progress(prog)
                    dashboard.status_container.markdown(f"**Enriched:** {title}")

                    try:
                        name, email, position = future.result()
                        if name and email:
                            result_data["apollo_calls_estimated"] += 1

                        if name:
                            set_if_allowed(processed_records[idx], "key_contact_name", name)
                            set_if_allowed(processed_records[idx], "key_contact_email", email)
                            set_if_allowed(processed_records[idx], "key_contact_position", position)
                            dashboard.update_metric("Enriched")
                            dashboard.update_metric("Success")
                        else:
                            dashboard.update_metric("Success")
                    except Exception as exc:
                        dashboard.log(f"Enrichment error for {title}: {exc}", level="error")
                        dashboard.update_metric("Errors")
        else:
            dashboard.update_status("Skipping enrichment (no valid websites or disabled)...", 80)
            for _ in processed_records:
                dashboard.update_metric("Success")

        # Instantly export
        instantly_key = secrets.get("instantly_key")
        instantly_added_count = 0

        if instantly_key and enrich_emails:
            dashboard.update_status("Checking Instantly Campaign...", 82)
            campaign_name = f"{industry} - Cold Outreach"
            campaign_id = find_or_create_instantly_campaign(instantly_key, campaign_name, debug=debug_mode)

            if campaign_id:
                valid_leads = [r for r in processed_records if r.get("key_contact_email")]
                if valid_leads:
                    dashboard.update_status(f"Exporting {len(valid_leads)} leads to Instantly...", 84)
                    created_count, created_leads, _, export_err = export_leads_to_instantly(
                        instantly_key, campaign_id, valid_leads, debug=debug_mode
                    )
                    instantly_added_count = created_count

                    # Default all exported candidates to Pending; mark Success for created leads.
                    # created_leads include an "index" field which refers to the index in the request leads array.
                    for r in processed_records:
                        if r.get("key_contact_email"):
                            set_if_allowed(r, "instantly_statuts", "Pending")
                            set_if_allowed(r, "instantly_campaign_id", campaign_id)

                    if export_err:
                        result_data["error_msg"] += f" | {export_err}"
                        dashboard.log(export_err, level="error")
                    else:
                        # Map created lead IDs back to Airtable rows
                        for created in created_leads or []:
                            try:
                                idx = int(created.get("index"))
                            except Exception:
                                continue
                            if 0 <= idx < len(valid_leads):
                                r = valid_leads[idx]
                                set_if_allowed(r, "instantly_statuts", "Success")
                                set_if_allowed(r, "instantly_lead_id", created.get("id"))
                else:
                    dashboard.log("No leads with emails to export.")
            else:
                dashboard.log(f"Could not find/create campaign '{campaign_name}'", level="error")
        elif instantly_key and not enrich_emails:
            dashboard.log("Skipping Instantly export: Verification (Enrichment) disabled.", level="warning")
        elif not instantly_key:
            dashboard.log("Skipping Instantly export: No API Key.", level="warning")

        result_data["instantly_added"] = instantly_added_count

        # Airtable sync
        new_records = processed_records
        if new_records:
            dashboard.update_status(f"Syncing {len(new_records)} records to Airtable...", 85)
            try:
                table_leads.batch_create(new_records, typecast=True)
                dashboard.update_status("Sync Complete!", 90)
            except Exception as e:
                # One-shot recovery for common Airtable case: trying to write to computed/read-only fields.
                err_str = str(e)
                dropped_field = None
                if "INVALID_VALUE_FOR_COLUMN" in err_str and 'Field "' in err_str:
                    try:
                        dropped_field = err_str.split('Field "', 1)[1].split('"', 1)[0]
                    except Exception:
                        dropped_field = None

                if dropped_field:
                    cleaned = []
                    for r in new_records:
                        if isinstance(r, dict) and dropped_field in r:
                            r = dict(r)
                            r.pop(dropped_field, None)
                        cleaned.append(r)
                    try:
                        table_leads.batch_create(cleaned, typecast=True)
                        dashboard.update_status(f"Sync Complete! (Dropped computed field: {dropped_field})", 90)
                        new_records = cleaned
                    except Exception as e2:
                        msg = f"Airtable Sync Failed: {e2}"
                        dashboard.log(msg, level="error")
                        result_data["error_msg"] += f" | {msg}"
                        st.error(msg)
                else:
                    msg = f"Airtable Sync Failed: {e}"
                    dashboard.log(msg, level="error")
                    result_data["error_msg"] += f" | {msg}"
                    st.error(msg)

        result_data["new_added"] = len(new_records)
        result_data["new_records"] = new_records
        result_data["status"] = "Success" if new_records or total_scraped > 0 else "Zero Results"

    except Exception as e:
        result_data["status"] = "Failed"
        result_data["error_msg"] = str(e)
        st.error(f"An error occurred: {e}")

    dashboard.update_status("Calculating final credit usage...", 95)
    apify_usage_post, _ = get_apify_credits(secrets["apify_token"], debug=st.session_state.get("debug_mode", False))

    credit_used_apify = None
    if apify_usage_pre is not None and apify_usage_post is not None:
        credit_used_apify = apify_usage_post - apify_usage_pre
        if credit_used_apify < 0:
            credit_used_apify = 0

    credit_used_apollo = result_data["apollo_calls_estimated"]
    credit_used_instantly = 0

    return result_data, credit_used_apify, credit_used_apollo, credit_used_instantly


