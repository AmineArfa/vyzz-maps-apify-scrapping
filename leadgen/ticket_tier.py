"""Canonical industry → ticket_tier mapping.

`ticket_tier` is a sales/marketing classification independent from `industry`.
It is computed from `industry` on INSERT only — once a row has a `ticket_tier`,
it is treated as operator-set and is not silently re-derived on UPDATE.

When the marketing team adds a new industry value, this map and the canonical
industry list must be updated together.
"""
from __future__ import annotations


TIERS = ("high", "mid", "low")
Tier = str  # one of TIERS, or None when unknown


LOW: frozenset[str] = frozenset({
    "Restaurants and Bars",
    "Hotels and Leisure",
    "Med Spa",
    "Retail Stores",
    "Food and Beverage Brands",
    "Home Goods",
    "Personal Care Products",
    "Apparel and Footwear",
    "Consumer Products",
    "Consumer Services",
})

MID: frozenset[str] = frozenset({
    "Clinic Services",
    "Medical Laboratories",
    "Dentist",
})


def compute_ticket_tier(industry: str | None) -> Tier | None:
    """Return the canonical tier for an industry.

    Returns None when industry is null/empty so the caller can leave the
    column NULL (the spec: "industry is null/empty → ticket_tier is null").
    Anything not in LOW or MID falls into "high".
    """
    if not industry or not isinstance(industry, str):
        return None
    name = industry.strip()
    if not name:
        return None
    if name in LOW:
        return "low"
    if name in MID:
        return "mid"
    return "high"
