"""Campaign filter spec — segment raw.scraped_leads by industry, tier, or both.

A `filter_spec` is the JSON we persist on raw.campaigns.filter_spec so it is
clear which segment a campaign represents. Three shapes are supported (mirrors
the TS type in the spec):

    {"type": "industry",          "value": "Med Spa"}
    {"type": "ticket_tier",       "value": "low"}                          # 'high' | 'mid' | 'low'
    {"type": "industry_and_tier", "industry": "Med Spa", "tier": "low"}

This module is pure (no DB / Streamlit), so it can be unit-tested without
spinning up Postgres.
"""
from __future__ import annotations

from typing import Any

from .ticket_tier import TIERS


class FilterSpecError(ValueError):
    """Raised when a filter_spec dict is malformed or has empty selectors."""


def validate_filter_spec(spec: dict | None) -> dict:
    """Validate and return a normalized copy of `spec`.

    Rejects "everything" (no industry + no tier — that's not a single
    campaign) and any unknown shape.
    """
    if not isinstance(spec, dict):
        raise FilterSpecError("filter_spec must be a dict")

    t = spec.get("type")
    if t == "industry":
        value = (spec.get("value") or "").strip()
        if not value:
            raise FilterSpecError("industry filter requires a non-empty value")
        return {"type": "industry", "value": value}

    if t == "ticket_tier":
        value = (spec.get("value") or "").strip()
        if value not in TIERS:
            raise FilterSpecError(f"ticket_tier must be one of {TIERS}")
        return {"type": "ticket_tier", "value": value}

    if t == "industry_and_tier":
        industry = (spec.get("industry") or "").strip()
        tier = (spec.get("tier") or "").strip()
        if not industry:
            raise FilterSpecError("industry_and_tier requires industry")
        if tier not in TIERS:
            raise FilterSpecError(f"industry_and_tier requires tier in {TIERS}")
        return {"type": "industry_and_tier", "industry": industry, "tier": tier}

    raise FilterSpecError(f"unknown filter_spec.type: {t!r}")


def spec_from_picker(
    industry: str | None,
    tier: str | None,
) -> dict:
    """Build a validated filter_spec from picker inputs.

    Either input may be None / "" / "(any)" to mean "no constraint on this
    axis". If both are unconstrained the spec is rejected — that would be
    "all leads", which the spec says shouldn't be a single campaign.
    """
    industry_clean = (industry or "").strip()
    if industry_clean.lower() in ("", "(any)", "any"):
        industry_clean = ""
    tier_clean = (tier or "").strip().lower()
    if tier_clean in ("", "(any)", "any"):
        tier_clean = ""

    if industry_clean and tier_clean:
        return validate_filter_spec({
            "type": "industry_and_tier",
            "industry": industry_clean,
            "tier": tier_clean,
        })
    if industry_clean:
        return validate_filter_spec({"type": "industry", "value": industry_clean})
    if tier_clean:
        return validate_filter_spec({"type": "ticket_tier", "value": tier_clean})
    raise FilterSpecError(
        "Pick at least one of industry / ticket_tier — '(any) + (any)' would "
        "match every lead and shouldn't be a single campaign."
    )


def build_where(
    spec: dict,
    *,
    exclude_in_active_campaign: bool = True,
) -> tuple[str, list[Any]]:
    """Return a parameterized WHERE clause + params for `raw.scraped_leads`.

    The clause is composed with `AND`; callers prepend ``WHERE``. Identifiers
    are hard-coded — no user-supplied column names. Values are bound through
    psycopg2 placeholders, never string-formatted.
    """
    spec = validate_filter_spec(spec)
    parts: list[str] = []
    params: list[Any] = []

    t = spec["type"]
    if t == "industry":
        parts.append("industry = %s")
        params.append(spec["value"])
    elif t == "ticket_tier":
        parts.append("ticket_tier = %s")
        params.append(spec["value"])
    elif t == "industry_and_tier":
        parts.append("industry = %s")
        params.append(spec["industry"])
        parts.append("ticket_tier = %s")
        params.append(spec["tier"])

    if exclude_in_active_campaign:
        parts.append("instantly_campaign_id IS NULL")

    return " AND ".join(parts), params


def describe(spec: dict) -> str:
    """Human-readable one-liner for a filter_spec — used in lists."""
    spec = validate_filter_spec(spec)
    t = spec["type"]
    if t == "industry":
        return f"industry = {spec['value']}"
    if t == "ticket_tier":
        return f"tier = {spec['value']}"
    return f"industry = {spec['industry']} AND tier = {spec['tier']}"
