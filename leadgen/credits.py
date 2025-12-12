import requests
import streamlit as st


def get_apify_credits(token, debug=False):
    """
    Fetch Apify monthly usage and limit using two endpoints.
    Returns: (usage_usd, limit_usd) or (None, None) on error.
    """
    try:
        headers = {"Authorization": f"Bearer {token}"}

        limit_usd = 0
        try:
            url_me = "https://api.apify.com/v2/users/me"
            resp_me = requests.get(url_me, headers=headers, timeout=10)
            if resp_me.status_code == 200:
                data_me = resp_me.json()
                if debug:
                    st.write("🔍 Apify /users/me data:", data_me)
                limit_usd = data_me.get("data", {}).get("plan", {}).get("maxMonthlyUsageUsd", 0)
        except Exception as e:
            if debug:
                st.write(f"⚠️ Failed to fetch Apify limit: {e}")

        usage_usd = 0
        try:
            url_usage = "https://api.apify.com/v2/users/me/usage/monthly"
            resp_usage = requests.get(url_usage, headers=headers, timeout=10)
            if resp_usage.status_code == 200:
                data_usage = resp_usage.json()
                if debug:
                    st.write("🔍 Apify /usage/monthly data:", data_usage)

                usage_usd = data_usage.get("data", {}).get("totalUsageCreditsUsdAfterVolumeDiscount")
                if usage_usd is None:
                    usage_usd = data_usage.get("data", {}).get("totalUsageCreditsUsdBeforeVolumeDiscount", 0)
        except Exception as e:
            if debug:
                st.write(f"⚠️ Failed to fetch Apify usage: {e}")

        return float(usage_usd), float(limit_usd)

    except Exception as e:
        if debug:
            st.warning(f"⚠️ Failed to fetch Apify credits: {e}")
        return None, None


def get_instantly_credits(api_key, debug=False):
    """
    Fetch Instantly plan details.
    Note: Exact credit field names depend on API response structure.
    """
    if not api_key:
        return None, None

    try:
        url = "https://api.instantly.ai/api/v2/workspace-billing/plan-details"
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()
            return data, None
        elif debug:
            st.write(f"⚠️ Instantly API Error: {response.status_code} - {response.text}")

    except Exception as e:
        if debug:
            st.write(f"⚠️ Failed to fetch Instantly credits: {e}")

    return None, None


def display_credit_dashboard(apify_token, apollo_key, instantly_key, debug=False):
    """
    Display credit dashboard with color-coded metrics.
    """
    st.sidebar.markdown("---")
    st.sidebar.header("💰 Credit Dashboard")

    apify_usage, apify_limit = get_apify_credits(apify_token, debug=debug)
    instantly_data, _ = get_instantly_credits(instantly_key, debug=debug)

    if apify_usage is not None and apify_limit is not None:
        apify_remaining = apify_limit - apify_usage
        apify_percent = (apify_remaining / apify_limit * 100) if apify_limit > 0 else 0

        if apify_percent < 20:
            apify_color = "🔴"
        elif apify_percent < 40:
            apify_color = "🟠"
        else:
            apify_color = "🟢"

        st.sidebar.metric(
            label=f"{apify_color} Apify Usage",
            value=f"${apify_usage:.2f}",
            delta=f"${apify_limit:.2f} limit",
        )
        st.sidebar.caption(f"Remaining: ${apify_remaining:.2f} ({apify_percent:.1f}%)")
    else:
        st.sidebar.metric(label="🔴 Apify Usage", value="N/A", delta="Unable to fetch")

    if instantly_data:
        subs = instantly_data.get("subscriptions", {})
        outreach = subs.get("outreach", {})
        plan_name = outreach.get("plan_name") or instantly_data.get("name") or "Unknown"

        limit = outreach.get("total_lead_limit", 0)
        current = outreach.get("current_lead_count", 0)
        remaining = limit - current

        st.sidebar.metric(label="⚡ Instantly Plan", value=plan_name, delta=f"{remaining} leads left")
        st.sidebar.caption(f"Used: {current}/{limit}")
    else:
        st.sidebar.metric(label="⚡ Instantly", value="N/A", delta="Check API Key")


