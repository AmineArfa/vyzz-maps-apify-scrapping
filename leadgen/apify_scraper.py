from apify_client import ApifyClient
import streamlit as st


def _lead_key(item: dict) -> str:
    """Best-effort stable key for deduping scraped places across zones."""
    if not isinstance(item, dict):
        return str(item)

    for k in ("placeId", "place_id", "place_id_in_google", "cid", "id"):
        v = item.get(k)
        if v:
            return f"{k}:{v}"

    website = (item.get("website") or "").strip().lower()
    phone = (item.get("phoneNumber") or item.get("phone") or item.get("internationalPhoneNumber") or "").strip()
    title = (item.get("title") or "").strip().lower()
    address = (item.get("address") or "").strip().lower()

    if website:
        return f"website:{website}"
    if phone:
        return f"phone:{phone}"
    return f"title_addr:{title}|{address}"


def scrape_apify(
    token,
    query,
    city,
    max_leads,
    *,
    zones: list[str] | None = None,
    dashboard=None,
    debug: bool = False,
):
    """Run Apify Google Maps Scraper.

    If zones is provided, runs sequentially per zone and stops early once max_leads unique items are collected.
    """
    client = ApifyClient(token)

    # No-split fallback: previous behavior
    if not zones:
        search_term = f"{query} in {city}"

        run_input = {
            "searchStringsArray": [search_term],
            "maxCrawledPlacesPerSearch": max_leads,
            "language": "en",
            "maxImages": 0,
            "oneReviewPerPlace": False,
            "skipClosedPlaces": True,
        }

        with st.spinner(f"Scraping '{search_term}' via Apify..."):
            run = client.actor("compass/crawler-google-places").call(run_input=run_input)

        dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items
        return dataset_items

    # Split mode: sequential runs with early stop
    per_zone_cap = max(1, int((max_leads + len(zones) - 1) / len(zones)))  # ceil(max_leads / len(zones))
    # Allow a bit of overfetch per zone to compensate for duplicates, without exploding cost.
    per_zone_cap = min(max_leads, max(per_zone_cap, min(75, max_leads)))

    collected: list[dict] = []
    seen_keys = set()

    for i, zone in enumerate(zones):
        search_term = f"{query} in {zone}"
        if dashboard:
            dashboard.update_split_row(
                zone_index=i,
                zone=zone,
                query=query,
                scraped_count=0,
                cumulative_unique=len(collected),
                status="Running",
            )

        run_input = {
            "searchStringsArray": [search_term],
            "maxCrawledPlacesPerSearch": per_zone_cap,
            "language": "en",
            "maxImages": 0,
            "oneReviewPerPlace": False,
            "skipClosedPlaces": True,
        }

        try:
            run = client.actor("compass/crawler-google-places").call(run_input=run_input)
            items = client.dataset(run["defaultDatasetId"]).list_items().items or []
        except Exception as e:
            if dashboard:
                dashboard.update_split_row(
                    zone_index=i,
                    zone=zone,
                    query=search_term,
                    scraped_count=0,
                    cumulative_unique=len(collected),
                    status=f"Error: {e}",
                )
            if debug and dashboard:
                dashboard.log(f"Apify split query failed for '{search_term}': {e}", level="error")
            continue

        added_this_zone = 0
        for it in items:
            k = _lead_key(it if isinstance(it, dict) else {})
            if k in seen_keys:
                continue
            seen_keys.add(k)
            if isinstance(it, dict):
                collected.append(it)
            added_this_zone += 1
            if len(collected) >= max_leads:
                break

        if dashboard:
            dashboard.update_split_row(
                zone_index=i,
                zone=zone,
                query=search_term,
                scraped_count=len(items),
                cumulative_unique=len(collected),
                status="Done" if len(collected) < max_leads else "Stopped (limit reached)",
            )

        if len(collected) >= max_leads:
            if dashboard:
                dashboard.set_split_stop_reason(
                    f"Stopped after zone #{i+1} because we reached the limit: **{len(collected)} / {max_leads}** unique leads."
                )
            break

    if dashboard and len(collected) < max_leads:
        dashboard.set_split_stop_reason(
            f"Finished all zones. Collected **{len(collected)} / {max_leads}** unique leads (Google Maps likely has limited results per zone/query)."
        )

    return collected


