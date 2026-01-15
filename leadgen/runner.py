from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st

from .airtable_utils import fetch_existing_leads, filter_airtable_fields, get_airtable_writable_field_names
from .apollo import enrich_apollo
from .apify_scraper import scrape_apify
from .credits import get_apify_credits
from .gemini_zones import generate_zones_with_gemini
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
    search_query: str,
):
    """
    Wrapper function that tracks credit usage before and after execution.
    Batches the process (Scrape -> Enrich -> Instantly -> Airtable) per zone to ensure data is saved incrementally.
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

        # Prepare zones for batch loop
        # Optional: Gemini-based location splitting (10 zones). Fallback is single query on ANY error/invalid output.
        split_enabled = bool(st.session_state.get("use_gemini_split", True))
        zones = None
        if split_enabled and secrets.get("gemini_key"):
            dashboard.update_status("Generating 10 split zones with Gemini...", 18)
            zones_res = generate_zones_with_gemini(
                city_input,
                api_key=secrets.get("gemini_key"),
                debug=debug_mode,
            )
            zones = zones_res.zones if zones_res else None
        
        # If no zones (split disabled or failed), treat as 1 "zone" which is the full city
        target_zones = zones if zones else [city_input]
        is_split_run = bool(zones)

        per_zone_cap = max(1, int((max_leads + len(target_zones) - 1) / len(target_zones))) if is_split_run else max_leads
        # Cap per zone to avoid over-fetching if many zones
        if is_split_run:
             per_zone_cap = min(max_leads, max(per_zone_cap, min(75, max_leads)))

        dashboard.init_split_view(zones=target_zones if is_split_run else [], per_zone_cap=per_zone_cap, max_leads=max_leads, enabled=split_enabled)

        # Global trackers
        total_scraped_global = 0
        collected_unique_leads = [] # stores dicts
        seen_keys_global = set() # dedupe across batches
        
        instantly_key = secrets.get("instantly_key")
        
        # Pre-fetch instanly campaign if needed
        campaign_id = None
        if instantly_key and enrich_emails:
             campaign_name = f"{industry} - Cold Outreach"
             # We try once at start; if fails, we might try inside loop or just skip. 
             # Better to try once to avoid spamming if API down.
             dashboard.update_status("Checking Instantly Campaign...", 15)
             campaign_id = find_or_create_instantly_campaign(instantly_key, campaign_name, debug=debug_mode)
             if not campaign_id:
                  dashboard.log(f"Could not find/create campaign '{campaign_name}'", level="error")

        # BATCH LOOP
        for i, zone in enumerate(target_zones):
             if len(collected_unique_leads) >= max_leads:
                  if is_split_run:
                       dashboard.set_split_stop_reason(
                            f"Stopped before zone '{zone}' because we reached the limit: **{len(collected_unique_leads)} / {max_leads}** unique leads."
                       )
                  break
             
             zone_label = zone if is_split_run else f"{search_query} in {city_input}"
             dashboard.update_status(f"Batch {i+1}/{len(target_zones)}: Scraping '{zone_label}'...", 20 + int((i/len(target_zones))*10))
             
             # INIT ROW
             current_zone_stats = {
                  "scraped": 0,
                  "enriched": 0,
                  "instantly": 0,
                  "synced": 0
             }
             if is_split_run:
                  dashboard.update_split_row(
                       zone_index=i,
                       zone=zone,
                       query=f"{search_query} in {zone}",
                       scraped_count=0,
                       cumulative_unique=len(collected_unique_leads),
                       status="Running",
                       enriched_count=0,
                       instantly_count=0,
                       synced_count=0
                  )

             # 1. SCRAPE
             # We use scrape_apify in "no split" mode (passing None for zones) effectively, 
             # but here we want to re-use the function.
             # Actually, scrape_apify with zones=None does full scrape.
             # We want to scrape JUST THIS ZONE.
             # The existing scrape_apify is a bit hybrid. 
             # Let's call it with zones=[zone] if split, or zones=None if not split (but loop size is 1).
             
             # To be cleaner, we can just call the logic for one zone. 
             # But 'scrape_apify' encapsulates client creation etc.
             # If we pass zones=[zone], it will loop once.
             batch_leads = scrape_apify(
                  secrets["apify_token"],
                  search_query,
                  city_input, # ignored if zones is set
                  max_leads=per_zone_cap, # fetch enough for this batch
                  zones=[zone] if is_split_run else None, 
                  dashboard=None, # We handle dashboard updates here to granularly track batch steps
                  debug=debug_mode
             )
             
             # Deduplicate Batch vs Global
             batch_unique = []
             for item in batch_leads:
                  # Use same key logic as apify_scraper internal, or duplicate here?
                  # apify_scraper internal deduplication is per-run. 
                  # We need check against global.
                  # Re-implementing _lead_key here or import it? 
                  # It is private in apify_scraper. Let's do a quick local check or just rely on existing fields.
                  # The 'raw_leads' usage below did 'exist_webs' check.
                  
                  website = item.get("website")
                  map_phone = item.get("phoneNumber") or item.get("phone") or item.get("internationalPhoneNumber")
                  
                  clean_web = str(website).strip().lower() if website else None
                  clean_phone = "".join(filter(str.isdigit, str(map_phone))) if map_phone else None

                  if (clean_web and clean_web in exist_webs) or (clean_phone and clean_phone in exist_phones):
                       # dashboard.update_metric("Skipped") # Optional to noisy
                       continue
                  
                  if clean_web: exist_webs.add(clean_web)
                  if clean_phone: exist_phones.add(clean_phone)
                  
                  batch_unique.append(item)
                  if len(collected_unique_leads) + len(batch_unique) >= max_leads:
                       break
             
             total_scraped_global += len(batch_leads)
             current_zone_stats["scraped"] = len(batch_leads)
             dashboard.stats["Total Scraped"] += len(batch_leads)
             dashboard.refresh_metrics()
             
             if not batch_unique:
                  if is_split_run:
                       dashboard.update_split_row(
                            zone_index=i,
                            zone=zone,
                            query=f"{search_query} in {zone}",
                            scraped_count=len(batch_leads),
                            cumulative_unique=len(collected_unique_leads),
                            status="Done (No new)",
                            enriched_count=0,
                            instantly_count=0,
                            synced_count=0
                       )
                  continue

             # 2. PROCESS & FILTER
             dashboard.update_status(f"Batch {i+1}: Processing {len(batch_unique)} new leads...", 30)
             
             processed_batch = []
             leads_to_enrich_indices = [] # list of (index_in_processed_batch, domain, title)
             
             for item in batch_unique:
                  title = item.get("title", "Unknown Company")
                  website = item.get("website")
                  map_phone = item.get("phoneNumber") or item.get("phone") or item.get("internationalPhoneNumber")
                  
                  parsed_city, parsed_state, parsed_postal_code = parse_address_components(item.get("address"), city_input)
                  
                  clean_web = str(website).strip().lower() if website else None

                  record = {
                    "company_name": title,
                    "industry": industry,
                    "city": parsed_city,
                    "state": parsed_state,
                    "postal_code": parsed_postal_code,
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
                  processed_batch.append(record)
                  
                  if clean_web and enrich_emails:
                       apollo_domain = clean_web.split("?")[0].split("#")[0]
                       leads_to_enrich_indices.append((len(processed_batch)-1, apollo_domain, title))
             
             # 3. ENRICH
             if leads_to_enrich_indices:
                  total_enrich = len(leads_to_enrich_indices)
                  dashboard.update_status(f"Batch {i+1}: Enriching {total_enrich} leads...", 40)
                  
                  params = [(secrets["apollo_key"], domain) for _, domain, _ in leads_to_enrich_indices]
                  
                  with ThreadPoolExecutor(max_workers=5) as executor:
                       future_to_idx = {
                            executor.submit(enrich_apollo, secrets["apollo_key"], domain): (idx, title)
                            for idx, domain, title in leads_to_enrich_indices
                       }
                       
                       completed_count = 0
                       for future in as_completed(future_to_idx):
                            try:
                                 p_idx, p_title = future_to_idx[future]
                                 name, email, position = future.result()
                                 completed_count += 1
                                 
                                 if name and email:
                                      result_data["apollo_calls_estimated"] += 1
                                      current_zone_stats["enriched"] += 1
                                 
                                 if name:
                                      set_if_allowed(processed_batch[p_idx], "key_contact_name", name)
                                      set_if_allowed(processed_batch[p_idx], "key_contact_email", email)
                                      set_if_allowed(processed_batch[p_idx], "key_contact_position", position)
                                      dashboard.update_metric("Enriched")
                                      dashboard.update_metric("Success")
                                 else:
                                      dashboard.update_metric("Success") # Count as success even if no enrichment found, we found the lead

                                 # Live update of table row during enrichment? 
                                 # Can do, but might be too frequent. Let's do it every 5 items or at end.
                                 if is_split_run and completed_count % 5 == 0:
                                      dashboard.update_split_row(
                                           zone_index=i, zone=zone, query=f"{search_query} in {zone}",
                                           scraped_count=current_zone_stats["scraped"],
                                           cumulative_unique=len(collected_unique_leads),
                                           status="Enriching...",
                                           enriched_count=current_zone_stats["enriched"],
                                           instantly_count=0, synced_count=0
                                      )

                            except Exception as exc:
                                 dashboard.log(f"Enrichment error: {exc}", level="error")
                                 dashboard.update_metric("Errors")
             else:
                  for _ in processed_batch:
                       dashboard.update_metric("Success")
             
             # 4. INSTANTLY EXPORT
             if campaign_id and enrich_emails:
                  valid_leads_instantly = [r for r in processed_batch if r.get("key_contact_email")]
                  if valid_leads_instantly:
                       dashboard.update_status(f"Batch {i+1}: Exporting {len(valid_leads_instantly)} to Instantly...", 80)
                       created_cnt, created_list, _, exp_err = export_leads_to_instantly(
                            instantly_key, campaign_id, valid_leads_instantly, debug=debug_mode
                       )
                       result_data["instantly_added"] += created_cnt
                       current_zone_stats["instantly"] = created_cnt
                       
                       # Update local records with status
                       for r in processed_batch:
                            if r.get("key_contact_email"):
                                 set_if_allowed(r, "instantly_statuts", "Pending")
                                 set_if_allowed(r, "instantly_campaign_id", campaign_id)
                       
                       if exp_err:
                            result_data["error_msg"] += f" | Batch {i}: {exp_err}"
                            dashboard.log(exp_err, level="error")
                       else:
                            # Map back IDs
                            for created in created_list or []:
                                 try:
                                      c_idx = int(created.get("index"))
                                      if 0 <= c_idx < len(valid_leads_instantly):
                                           set_if_allowed(valid_leads_instantly[c_idx], "instantly_statuts", "Success")
                                           set_if_allowed(valid_leads_instantly[c_idx], "instantly_lead_id", created.get("id"))
                                 except: pass

             # 5. AIRTABLE SYNC (Intermediate Save)
             dashboard.update_status(f"Batch {i+1}: Syncing {len(processed_batch)} to Airtable...", 90)
             try:
                  if processed_batch:
                       table_leads.batch_create(processed_batch, typecast=True)
                       current_zone_stats["synced"] = len(processed_batch)
             except Exception as e:
                  # Retry logic for computed fields
                  err_str = str(e)
                  dropped_field = None
                  if "INVALID_VALUE_FOR_COLUMN" in err_str and 'Field "' in err_str:
                       try:
                            dropped_field = err_str.split('Field "', 1)[1].split('"', 1)[0]
                       except: dropped_field = None
                  
                  if dropped_field:
                       cleaned = []
                       for r in processed_batch:
                            if isinstance(r, dict) and dropped_field in r:
                                 r = dict(r)
                                 r.pop(dropped_field, None)
                            cleaned.append(r)
                       try:
                            table_leads.batch_create(cleaned, typecast=True)
                            current_zone_stats["synced"] = len(cleaned)
                            processed_batch = cleaned # keep cleaned for history
                       except Exception as e2:
                            msg = f"Batch {i} Sync Failed: {e2}"
                            dashboard.log(msg, level="error")
                            result_data["error_msg"] += f" | {msg}"
                  else:
                       msg = f"Batch {i} Sync Failed: {e}"
                       dashboard.log(msg, level="error")
                       result_data["error_msg"] += f" | {msg}"

             # Batch Complete
             collected_unique_leads.extend(processed_batch)
             result_data["new_records"].extend(processed_batch)
             
             if is_split_run:
                  dashboard.update_split_row(
                       zone_index=i,
                       zone=zone,
                       query=f"{industry} in {zone}",
                       scraped_count=current_zone_stats["scraped"],
                       cumulative_unique=len(collected_unique_leads),
                       status="Done",
                       enriched_count=current_zone_stats["enriched"],
                       instantly_count=current_zone_stats["instantly"],
                       synced_count=current_zone_stats["synced"]
                  )
        
        # End Loop
        result_data["total_scraped"] = total_scraped_global
        result_data["new_added"] = len(collected_unique_leads)
        
        if is_split_run:
             dashboard.set_split_stop_reason(
                 f"Finished all zones. Collected **{len(collected_unique_leads)} / {max_leads}** total unique leads."
             )
        else:
             dashboard.update_status("Execution Complete!", 100)

        result_data["status"] = "Success" if collected_unique_leads or total_scraped_global > 0 else "Zero Results"


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


