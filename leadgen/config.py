import streamlit as st
import os


def get_secrets():
    """Safely retrieve secrets or show error."""
    try:
        return {
            "airtable_key": st.secrets["AIRTABLE_API_KEY"],
            "airtable_base": st.secrets["AIRTABLE_BASE_ID"],
            "apify_token": st.secrets["APIFY_TOKEN"],
            "apollo_key": st.secrets["APOLLO_API_KEY"],
            "instantly_key": st.secrets.get("INSTANTLY_API_KEY", ""),
            # Optional: Gemini key for zone splitting. Prefer Streamlit secrets; fallback to env var.
            "gemini_key": (st.secrets.get("GOOGLE_GENERATIVE_AI_API_KEY", "") or os.getenv("GOOGLE_GENERATIVE_AI_API_KEY", "")).strip(),
            # Optional: MillionVerifier key for email verification before Instantly sync.
            "millionverifier_key": st.secrets.get("MILLIONVERIFIER_API_KEY", ""),
        }
    except FileNotFoundError:
        st.error("Secrets file not found. Please create `.streamlit/secrets.toml`.")
        st.stop()
    except KeyError as e:
        st.error(f"Missing secret key: {e}")
        st.stop()


