import requests
import streamlit as st

# Retrieve fresh secrets explicitly
try:
    import toml
    secrets = toml.load(".streamlit/secrets.toml")
    api_key = secrets["APOLLO_API_KEY"]
except Exception as e:
    print(f"❌ Error loading secrets: {e}")
    exit()

print(f"🔑 Using API Key: {api_key[:10]}...")

url = "https://api.apollo.io/v1/people/match"
headers = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "X-Api-Key": api_key
}

# Test with a known company
target_domain = "openai.com"
print(f"🔎 Testing Apollo Enrichment for: {target_domain}")

data = {
    "domain": target_domain,
    "organization_titles": ["ceo", "founder"],
    "reveal_personal_emails": True,
    "reveal_phone_number": False
}

try:
    response = requests.post(url, headers=headers, json=data)
    print(f"📡 Status Code: {response.status_code}")
    print(f"📄 Raw Response: {response.text[:500]}...") # Print first 500 chars

    if response.status_code == 200:
        json_resp = response.json()
        person = json_resp.get("person")
        if person:
            print(f"✅ SUCCESS! Found: {person.get('name')}")
            print(f"🔑 Top Level Keys: {list(json_resp.keys())}")
            # print(f"📄 Full JSON: {json_resp}")
            print(f"📧 Email: {person.get('email')}")
        else:
            print("⚠️ Response valid 200 OK, but NO Person object found.")
    else:
        print("❌ API Request Failed.")
except Exception as e:
    print(f"❌ Exception: {e}")
