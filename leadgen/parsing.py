import re


def parse_address_components(address, fallback_city):
    """
    Extracts City and State/Country from address string.
    Normalizes 'Manhattan', 'Brooklyn', etc. to 'New York'.
    Returns (city, state).
    """
    if not address:
        return fallback_city.title(), None

    parts = [p.strip() for p in str(address).split(",")]

    city = fallback_city.title()
    state = None

    if len(parts) >= 3:
        state_zip_part = parts[-2]
        possible_city = parts[-3]

        match = re.search(r"\b([A-Z]{2})\b", state_zip_part)
        if match:
            state = match.group(1)
            city = possible_city
        else:
            state = parts[-1]
            city = parts[-2]
    elif len(parts) == 2:
        city = parts[0]
        state = parts[1]

    if state and city.endswith(f", {state}"):
        city = city.replace(f", {state}", "").strip()

    nyc_boroughs = ["Manhattan", "Brooklyn", "Queens", "The Bronx", "Bronx", "Staten Island"]
    if any(b.lower() in city.lower() for b in nyc_boroughs):
        city = "New York"
        if not state:
            state = "NY"

    return city, state


