import requests


def enrich_apollo(api_key, domain):
    """
    Two-Step Enrichment:
    1. Search to find the best person (Name).
    2. Match to unlock their Email using ID.
    Returns (name, email, position).
    """
    search_url = "https://api.apollo.io/v1/mixed_people/search"
    match_url = "https://api.apollo.io/v1/people/match"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }

    search_data = {
        "q_organization_domains": domain,
        "page": 1,
        "per_page": 1,
        "person_titles": ["owner", "founder", "ceo", "director", "partner", "president", "manager"],
        "contact_email_status": ["verified"],
    }

    best_name = None
    best_title = None
    best_id = None

    try:
        resp1 = requests.post(search_url, headers=headers, json=search_data, timeout=20)
        if resp1.status_code == 200:
            people = resp1.json().get("people", [])
            if people:
                best_name = people[0].get("name")
                best_title = people[0].get("title")
                best_id = people[0].get("id")
            else:
                return None, None, None
    except Exception:
        return None, None, None

    if best_id:
        match_data = {
            "id": best_id,
            "reveal_personal_emails": True,
        }

        try:
            resp2 = requests.post(match_url, headers=headers, json=match_data, timeout=20)
            if resp2.status_code == 200:
                json_resp = resp2.json()
                person = json_resp.get("person")
                if person:
                    email = person.get("email")
                    position = person.get("title") or best_title
                    return best_name, email, position
        except Exception:
            pass

    return best_name, None, best_title


