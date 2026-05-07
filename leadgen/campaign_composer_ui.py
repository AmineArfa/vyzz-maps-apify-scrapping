"""Streamlit UI for the campaign composer.

Operator picks an industry / ticket_tier filter, sees a live matched-lead
count and a sample, then either creates a new Instantly campaign or moves
the leads into an existing one. The chosen filter_spec is persisted on
raw.campaigns so the audit view can show "who got into what campaign and
why".

Only available on the Supabase backend — Airtable doesn't have ticket_tier
or the campaigns table.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from .campaign_filter import FilterSpecError, describe, spec_from_picker
from .instantly import (
    _list_all_campaigns,
    export_leads_to_instantly,
    find_or_create_instantly_campaign,
    inject_lid_to_lead,
    is_valid_uuid,
    reset_campaign_cache,
)
from .ticket_tier import TIERS


_TIER_PICKER_OPTIONS = ("(any)", *TIERS)
_PREVIEW_LIMIT = 20


def _spec_from_inputs(industry: str, tier: str):
    """Build a filter_spec from the picker, or return (None, error_message)."""
    try:
        return spec_from_picker(industry, tier), None
    except FilterSpecError as e:
        return None, str(e)


def _campaign_name_default(spec: dict) -> str:
    """Derive a sensible default campaign name from the filter spec."""
    t = spec["type"]
    if t == "industry":
        return f"{spec['value']} - Cold Outreach"
    if t == "ticket_tier":
        return f"{spec['value'].title()} Tier - Cold Outreach"
    return f"{spec['industry']} - {spec['tier'].title()} - Cold Outreach"


def _render_filter_picker(backend) -> tuple[str, str, bool]:
    industries = ["(any)"] + backend.fetch_distinct_industries()
    col_ind, col_tier, col_excl = st.columns([3, 2, 3])
    with col_ind:
        industry = st.selectbox(
            "Industry",
            options=industries,
            key="composer_industry",
            help="Distinct industries from raw.scraped_leads",
        )
    with col_tier:
        tier = st.selectbox(
            "Ticket tier",
            options=_TIER_PICKER_OPTIONS,
            key="composer_tier",
        )
    with col_excl:
        st.write("")  # vertical alignment
        exclude_active = st.checkbox(
            "Exclude leads already in an active Instantly campaign",
            value=True,
            key="composer_exclude_active",
            help="Default on so we don't double-message people who are already being contacted.",
        )
    return industry, tier, exclude_active


def _push_to_campaign(
    backend,
    secrets: dict,
    *,
    leads: list[dict],
    spec: dict,
    campaign_name: str,
    existing_campaign_id: str | None,
    operator: str | None,
    debug: bool,
) -> dict:
    """Create or pick an Instantly campaign, push leads, then persist the row."""
    api_key = secrets.get("instantly_key")
    if not api_key:
        return {"ok": False, "error": "Instantly API key missing"}

    if existing_campaign_id:
        if not is_valid_uuid(existing_campaign_id):
            return {"ok": False, "error": f"Invalid existing campaign id: {existing_campaign_id}"}
        c_id = existing_campaign_id
    else:
        c_id = find_or_create_instantly_campaign(api_key, campaign_name, debug=debug)
        if not c_id:
            return {"ok": False, "error": f"Failed to create Instantly campaign '{campaign_name}'"}

    cnt, created, _, err = export_leads_to_instantly(api_key, c_id, leads, debug=debug)
    if err and cnt == 0:
        return {"ok": False, "error": err}

    for c in created or []:
        new_id = c.get("id")
        if new_id and is_valid_uuid(new_id):
            inject_lid_to_lead(api_key, new_id, debug=debug)

    record_id = backend.create_campaign_record(
        name=campaign_name,
        filter_spec=spec,
        instantly_campaign_id=c_id,
        status="active",
        created_by=operator,
    )

    return {
        "ok": True,
        "instantly_campaign_id": c_id,
        "instantly_added": cnt,
        "campaign_record_id": record_id,
        "warning": err,
    }


def render(backend, secrets: dict, *, active_mode: str, debug_mode: bool) -> None:
    st.subheader("📣 Campaign Composer")
    st.caption(
        "Segment `raw.scraped_leads` by industry and/or ticket_tier, then "
        "create a new Instantly campaign or move the matched leads into an "
        "existing one. The filter is saved on `raw.campaigns.filter_spec`."
    )

    if active_mode != "supabase":
        st.info("Campaign composer requires the Supabase backend.")
        return

    industry, tier, exclude_active = _render_filter_picker(backend)
    spec, picker_err = _spec_from_inputs(industry, tier)

    if picker_err:
        st.error(picker_err)
        return

    assert spec is not None  # for type checker
    count = backend.count_leads_by_filter(spec, exclude_in_active_campaign=exclude_active)
    st.metric(
        label=f"Matched leads ({describe(spec)})",
        value=count,
    )

    if count == 0:
        st.info("No leads match this filter.")
        return

    sample = backend.fetch_leads_by_filter(
        spec, limit=_PREVIEW_LIMIT, exclude_in_active_campaign=exclude_active,
    )
    if sample:
        st.caption(f"Sample (first {min(_PREVIEW_LIMIT, count)} of {count}):")
        st.dataframe(
            pd.DataFrame(sample),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    target_mode = st.radio(
        "Push to:",
        options=["Create new campaign", "Move to existing campaign"],
        horizontal=True,
        key="composer_target_mode",
    )

    default_name = _campaign_name_default(spec)
    new_name = ""
    existing_id: str | None = None

    if target_mode == "Create new campaign":
        new_name = st.text_input(
            "New campaign name",
            value=default_name,
            key="composer_new_name",
        )
    else:
        api_key = secrets.get("instantly_key")
        if not api_key:
            st.error("Instantly API key missing — cannot list existing campaigns.")
            return
        try:
            campaigns = _list_all_campaigns(api_key, debug=debug_mode) or []
        except Exception as e:
            st.error(f"Failed to load Instantly campaigns: {e}")
            return
        labels = {c.get("id"): (c.get("name") or "(unnamed)") for c in campaigns if c.get("id")}
        if not labels:
            st.info("No existing Instantly campaigns found.")
            return
        existing_id = st.selectbox(
            "Existing campaign",
            options=list(labels.keys()),
            format_func=lambda cid: f"{labels[cid]} ({cid[:8]}…)",
            key="composer_existing_id",
        )
        new_name = labels.get(existing_id, "")

    operator = secrets.get("operator_email") or "operator"

    if st.button(
        f"🚀 Push {count} leads to '{new_name or '(pick a campaign)'}'",
        type="primary",
        disabled=not new_name,
        key="composer_push_btn",
    ):
        with st.status("Pushing to Instantly...", expanded=True) as status:
            status.write(f"Loading {count} leads matching filter...")
            leads = backend.fetch_leads_by_filter(
                spec, limit=None, exclude_in_active_campaign=exclude_active,
            )
            status.write(f"Loaded {len(leads)} leads.")

            reset_campaign_cache()
            result = _push_to_campaign(
                backend, secrets,
                leads=leads,
                spec=spec,
                campaign_name=new_name,
                existing_campaign_id=existing_id,
                operator=operator,
                debug=debug_mode,
            )

        if result.get("ok"):
            warn = result.get("warning")
            msg = (
                f"✅ Added **{result['instantly_added']}** leads to Instantly "
                f"campaign `{result['instantly_campaign_id']}`. "
                f"Filter recorded as `raw.campaigns.id = {result['campaign_record_id']}`."
            )
            if warn:
                st.warning(f"{msg}\n\nNon-fatal warning: {warn}")
            else:
                st.success(msg)
        else:
            st.error(f"❌ {result.get('error')}")

    st.divider()
    st.subheader("📚 Recorded campaigns")
    records = backend.list_campaign_records()
    if not records:
        st.caption("No campaigns recorded yet.")
        return
    rows = []
    for r in records:
        rows.append({
            "name": r.get("name"),
            "filter": describe(r["filter_spec"]) if r.get("filter_spec") else "—",
            "status": r.get("status"),
            "instantly_campaign_id": (
                (r["instantly_campaign_id"][:8] + "…")
                if r.get("instantly_campaign_id") else "—"
            ),
            "created_by": r.get("created_by") or "—",
            "created_at": r.get("created_at"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
