from apify_client import ApifyClient
import streamlit as st


def scrape_apify(token, industry, city, max_leads):
    """Run Apify Google Maps Scraper."""
    client = ApifyClient(token)
    search_term = f"{industry} in {city}"

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


