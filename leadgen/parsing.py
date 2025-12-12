import re


def parse_address_components(address, fallback_city):
    """
    Extracts City and State/Country from address string.
    Normalizes 'Manhattan', 'Brooklyn', etc. to 'New York'.
    Also extracts postal/zip code when present.
    Returns (city, state, postal_code).
    """
    if not address:
        return fallback_city.title(), None, None

    parts = [p.strip() for p in str(address).split(",")]

    city = fallback_city.title()
    state = None
    postal_code = None

    state_code_re = re.compile(r"\b([A-Z]{2})\b")
    zip_re = re.compile(r"\b(\d{5}(?:-\d{4})?)\b")

    if len(parts) >= 3:
        state_zip_part = parts[-2]
        possible_city = parts[-3]

        # Pull zip first (if present)
        zip_match = zip_re.search(state_zip_part)
        if zip_match:
            postal_code = zip_match.group(1)

        match = state_code_re.search(state_zip_part)
        if match:
            state = match.group(1)
            city = possible_city
        else:
            state = parts[-1]
            city = parts[-2]
    elif len(parts) == 2:
        city = parts[0]
        state_zip_part = parts[1]

        zip_match = zip_re.search(state_zip_part)
        if zip_match:
            postal_code = zip_match.group(1)

        match = state_code_re.search(state_zip_part)
        if match:
            state = match.group(1)
        else:
            # Strip zip if it's glued to the state region text
            state = zip_re.sub("", state_zip_part).strip() or state_zip_part.strip()

    if state and city.endswith(f", {state}"):
        city = city.replace(f", {state}", "").strip()

    nyc_boroughs = ["Manhattan", "Brooklyn", "Queens", "The Bronx", "Bronx", "Staten Island"]
    if any(b.lower() in city.lower() for b in nyc_boroughs):
        city = "New York"
        if not state:
            state = "NY"

    # Final normalization pass:
    # - If state accidentally contains a ZIP (e.g. "CA 94105"), split it out.
    # - If state is not a 2-letter code, try to capture a 2-letter code inside it.
    if state:
        # Extract ZIP from state string if present
        zip_match = zip_re.search(state)
        if zip_match and not postal_code:
            postal_code = zip_match.group(1)
        # Normalize state to 2-letter code when possible
        code_match = state_code_re.search(state)
        if code_match:
            state = code_match.group(1)
        else:
            # Otherwise strip zip and trim
            state = zip_re.sub("", state).strip()

    return city, state, postal_code


