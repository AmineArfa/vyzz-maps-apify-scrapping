import streamlit as st


def get_secrets():
    """Safely retrieve secrets or show error."""
    try:
        return {
            "airtable_key": st.secrets["AIRTABLE_API_KEY"],
            "airtable_base": st.secrets["AIRTABLE_BASE_ID"],
            "apify_token": st.secrets["APIFY_TOKEN"],
            "apollo_key": st.secrets["APOLLO_API_KEY"],
            "instantly_key": st.secrets.get("INSTANTLY_API_KEY", ""),
        }
    except FileNotFoundError:
        st.error("Secrets file not found. Please create `.streamlit/secrets.toml`.")
        st.stop()
    except KeyError as e:
        st.error(f"Missing secret key: {e}")
        st.stop()


