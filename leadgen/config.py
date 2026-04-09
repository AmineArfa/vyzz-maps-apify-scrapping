import streamlit as st
import os


def get_secrets():
    """Safely retrieve secrets or show error."""
    try:
        return {
            "airtable_key": st.secrets.get("AIRTABLE_API_KEY", ""),
            "airtable_base": st.secrets.get("AIRTABLE_BASE_ID", ""),
            "apify_token": st.secrets["APIFY_TOKEN"],
            "apollo_key": st.secrets["APOLLO_API_KEY"],
            "instantly_key": st.secrets.get("INSTANTLY_API_KEY", ""),
            # Optional: Gemini key for zone splitting. Prefer Streamlit secrets; fallback to env var.
            "gemini_key": (st.secrets.get("GOOGLE_GENERATIVE_AI_API_KEY", "") or os.getenv("GOOGLE_GENERATIVE_AI_API_KEY", "")).strip(),
            # Optional: MillionVerifier key for email verification before Instantly sync.
            "millionverifier_key": st.secrets.get("MILLIONVERIFIER_API_KEY", ""),
            # Backend toggle (Step 3.3): "supabase" or "airtable"
            "data_backend": st.secrets.get("DATA_BACKEND", "airtable").strip().lower(),
            # Supabase connection (restricted scraper_app role — raw.* only)
            "supabase_db_url": st.secrets.get("SUPABASE_DB_URL", ""),
        }
    except FileNotFoundError:
        st.error("Secrets file not found. Please create `.streamlit/secrets.toml`.")
        st.stop()
    except KeyError as e:
        st.error(f"Missing secret key: {e}")
        st.stop()


