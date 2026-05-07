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
from .campaign_push import push_leads_to_campaign, recategorize_all_by_tier
from .instantly import (
    _list_all_campaigns,
    find_or_create_instantly_campaign,
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
    """Resolve the Instantly campaign id, push, then persist the campaign row.

    Splits the work between MOVE (existing instantly_lead_id) and CREATE
    (no id yet), preserves the lead id in both cases, and writes the
    resulting id back to raw.scraped_leads so the row stays linked.
    """
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

    push_result = push_leads_to_campaign(
        backend, api_key=api_key, leads=leads, campaign_id=c_id, debug=debug,
    )

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
        "campaign_record_id": record_id,
        **{k: v for k, v in push_result.items() if k != "details"},
        "details": push_result["details"],
    }


def _render_recategorize_section(backend, secrets: dict, debug: bool) -> None:
    """One-click: re-route every lead into a tier-segmented campaign.

    Existing leads are MOVED via Instantly's bulk move endpoint, which
    preserves `instantly_lead_id`, `lid`, and every other custom_variable.
    New leads are CREATED with the merge fields and the new id is written
    back to `raw.scraped_leads`. Leads with NULL `ticket_tier` are not in
    any tier filter and stay where they are.
    """
    st.subheader("⚡ Recategorize all leads by tier")
    st.caption(
        "Push every lead with a `ticket_tier` into its tier-segmented "
        "campaign in one click. Existing leads are moved (lid preserved); "
        "new leads are created with the merge fields. Leads without a tier "
        "are left untouched."
    )

    name_template = st.text_input(
        "Campaign name template",
        value="{tier_title} Tier - Cold Outreach",
        help="`{tier}` = low/mid/high lowercase, `{tier_title}` = capitalized.",
        key="recat_name_template",
    )

    # Build per-tier names. Format errors fall back to the default template
    # rather than blanking the section, so a stray brace in the input box
    # never makes the run button disappear.
    tier_names: dict[str, str] = {}
    for tier in TIERS:
        try:
            tier_names[tier] = name_template.format(
                tier=tier, tier_title=tier.title(),
            )
        except (KeyError, IndexError, ValueError):
            tier_names[tier] = f"{tier.title()} Tier - Cold Outreach"
            st.warning(
                f"Template error for `{tier}`. Falling back to "
                f"`{tier_names[tier]}`. Use only `{{tier}}` and `{{tier_title}}`."
            )

    # Per-tier counts. Each query is wrapped so a Postgres error in one
    # tier doesn't blank the whole section — the operator still sees the
    # other tiers and can re-try.
    tier_counts: dict[str, int] = {}
    counts_errored = False
    with st.spinner("Counting leads per tier…"):
        for tier in TIERS:
            try:
                tier_counts[tier] = backend.count_leads_by_filter(
                    {"type": "ticket_tier", "value": tier},
                    exclude_in_active_campaign=False,
                )
            except Exception as e:
                tier_counts[tier] = 0
                counts_errored = True
                st.error(f"Could not count `{tier}` leads: {e}")

    try:
        tier_less = backend.count_leads_without_tier()
    except Exception as e:
        tier_less = 0
        st.warning(f"Could not count tier-less leads: {e}")

    # Force every tier to render as a metric so the operator visually
    # confirms the section is alive, even if the DB returned zeros.
    cols = st.columns(len(TIERS) + 1)
    for i, tier in enumerate(TIERS):
        cols[i].metric(
            label=f"{tier.title()} → {tier_names[tier]}",
            value=int(tier_counts.get(tier, 0)),
        )
    cols[-1].metric(label="No tier (skipped)", value=int(tier_less))

    if tier_less:
        st.caption(
            f"⚠️ {tier_less} leads have NULL `ticket_tier` and won't be "
            "recategorized. Set their `industry` to populate the tier first."
        )

    if counts_errored:
        st.error(
            "One or more tier counts failed. The button stays available so "
            "you can still attempt the run, but expect the same error to "
            "surface during the push."
        )
    elif sum(tier_counts.values()) == 0:
        st.info(
            "No leads to recategorize — every lead with a `ticket_tier` is "
            "already in its target campaign, or the column is unset."
        )

    confirmed = st.checkbox(
        "I understand this will move leads across campaigns in Instantly.",
        key="recat_confirm",
    )

    if st.button(
        "🚀 Run recategorization",
        type="primary",
        disabled=not confirmed,
        key="recat_run_btn",
    ):
        api_key = secrets.get("instantly_key")
        if not api_key:
            st.error("Instantly API key missing.")
            return
        operator = secrets.get("operator_email") or "operator"

        with st.status("Recategorizing all leads by tier...", expanded=True) as status:
            reset_campaign_cache()

            def _resolve(tier: str) -> str | None:
                name = tier_names[tier]
                status.write(f"Resolving Instantly campaign '{name}'...")
                return find_or_create_instantly_campaign(api_key, name, debug=debug)

            result = recategorize_all_by_tier(
                backend, api_key=api_key, resolve_campaign_id=_resolve, debug=debug,
            )

            for tier, r in result["by_tier"].items():
                if r.get("campaign_id"):
                    backend.create_campaign_record(
                        name=tier_names[tier],
                        filter_spec={"type": "ticket_tier", "value": tier},
                        instantly_campaign_id=r["campaign_id"],
                        status="active",
                        created_by=operator,
                    )

        cols = st.columns(5)
        cols[0].metric("Moved", result["moved"])
        cols[1].metric("Created", result["created"])
        cols[2].metric("Already in place", result.get("already_in_place", 0))
        cols[3].metric("Skipped", result["skipped"])
        cols[4].metric("Failed", result["failed"])

        per_tier_rows = [{
            "tier": t,
            "campaign": tier_names[t],
            "moved": result["by_tier"][t].get("moved", 0),
            "created": result["by_tier"][t].get("created", 0),
            "already_in_place": result["by_tier"][t].get("already_in_place", 0),
            "skipped": result["by_tier"][t].get("skipped", 0),
            "failed": result["by_tier"][t].get("failed", 0),
            "error": result["by_tier"][t].get("error") or "",
        } for t in TIERS]
        st.dataframe(pd.DataFrame(per_tier_rows), use_container_width=True, hide_index=True)
        st.caption(
            "ℹ️ `already_in_place` rows were skipped at the SQL filter — leads "
            "already in the right tier campaign aren't re-moved. Safe to re-run "
            "this if a previous attempt was interrupted."
        )

        if result["failed"]:
            failures = [
                d for r in result["by_tier"].values()
                for d in r.get("details", []) if d.get("op") == "failed"
            ]
            with st.expander(f"❌ {result['failed']} failures", expanded=False):
                rows = [{
                    "tier": d.get("ticket_tier"),
                    "email": d.get("email"),
                    "error": d.get("error"),
                } for d in failures]
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_recorded_campaigns(backend) -> None:
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


def render(backend, secrets: dict, *, active_mode: str, debug_mode: bool) -> None:
    st.subheader("📣 Campaign Composer")
    st.caption(
        "Two flows: a one-click bulk recategorization that routes every lead "
        "into its tier campaign, and a single-segment picker for ad-hoc pushes."
    )

    if active_mode != "supabase":
        st.info("Campaign composer requires the Supabase backend.")
        return

    # ── 1. Bulk recategorization (always visible) ────────────────────────
    _render_recategorize_section(backend, secrets, debug_mode)
    st.divider()

    # ── 2. Single-segment picker (ad-hoc) ────────────────────────────────
    st.subheader("🎯 Push a single segment")
    st.caption(
        "Pick an industry and/or tier to push just that slice into a new or "
        "existing Instantly campaign. The filter is saved on `raw.campaigns.filter_spec`."
    )

    industry, tier, exclude_active = _render_filter_picker(backend)
    spec, picker_err = _spec_from_inputs(industry, tier)

    if picker_err:
        # Soft notice instead of an early return so the recorded-campaigns
        # list (and the recategorize section above) stay visible.
        st.info(picker_err)
        st.divider()
        _render_recorded_campaigns(backend)
        return

    assert spec is not None  # for type checker
    count = backend.count_leads_by_filter(spec, exclude_in_active_campaign=exclude_active)
    st.metric(
        label=f"Matched leads ({describe(spec)})",
        value=count,
    )

    if count == 0:
        st.info("No leads match this filter.")
        st.divider()
        _render_recorded_campaigns(backend)
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
            cols = st.columns(5)
            cols[0].metric("Moved", result.get("moved", 0))
            cols[1].metric("Created", result.get("created", 0))
            cols[2].metric("Already in place", result.get("already_in_place", 0))
            cols[3].metric("Skipped", result.get("skipped", 0))
            cols[4].metric("Failed", result.get("failed", 0))
            st.success(
                f"Pushed to Instantly campaign `{result['instantly_campaign_id']}`. "
                f"Filter recorded as `raw.campaigns.id = {result['campaign_record_id']}`."
            )
            details = result.get("details") or []
            if details:
                with st.expander("Per-lead details", expanded=False):
                    rows = [{
                        "op": d["op"],
                        "email": d.get("email") or "—",
                        "company": d.get("company_name") or "—",
                        "industry": d.get("industry") or "—",
                        "tier": d.get("ticket_tier") or "—",
                        "instantly_lead_id": (
                            (d["instantly_lead_id"][:8] + "…")
                            if d.get("instantly_lead_id") else "—"
                        ),
                        "error": d.get("error") or "",
                    } for d in details]
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            st.caption(
                "ℹ️ Industry change is **not** auto-recomputed on `ticket_tier`. "
                "If an operator updates `industry` on a row that already has a "
                "`ticket_tier`, the existing tier is preserved — use a future "
                "explicit 'recompute tier' admin action to override."
            )
        else:
            st.error(f"❌ {result.get('error')}")

    st.divider()
    _render_recorded_campaigns(backend)
