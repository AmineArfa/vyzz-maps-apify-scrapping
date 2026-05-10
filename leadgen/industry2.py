"""Canonical industry → industry2 mapping.

`industry2` is an email-friendlier rendering of `industry` for use as a
merge variable in cold-email copy ("the leading {{industry2}} in {{City}}"
reads better than "the leading Business Consulting in {{City}}").

It is computed from `industry` deterministically. NULL `industry` stays NULL
`industry2`. Any future industry value not present in this map returns None
so the operator can spot the gap and add it explicitly — we never
fabricate a label.

When marketing adds a new industry, this map AND the database
backfill SQL must be updated together (see migrations/2026-05-10_add_industry2_to_scraped_leads.sql).
"""
from __future__ import annotations


INDUSTRY2_BY_INDUSTRY: dict[str, str] = {
    "Business Consulting":          "Consulting firm",
    "Clinic Services":              "Medical Clinic",
    "Restaurants and Bars":         "Restaurant",
    "Med Spa":                      "Med spa",
    "Hotels and Leisure":           "Hotel",
    "Elder and Disabled Care":      "Senior care home",
    "Consumer Services":            "Local service provider",
    "Healthcare Software":          "Healthcare software",
    "Hospitals":                    "Hospital",
    "Retail Stores":                "Retail store",
    "Business Software":            "Software",
    "Dentist":                      "Dentist",
    "Medical Devices":              "Medical device supplier",
    "Real Estate Services":         "Real estate agency",
    "Food and Beverage Brands":     "F&B brand",
    "Media and Publishing":         "Media company",
    "Business Services":            "Business service provider",
    "Home Goods":                   "Home goods store",
    "Education and Training":       "Training center",
    "Personal Care Products":       "Personal care products",
    "Wholesale Distributors":       "Wholesale distributor",
    "Immigration Lawyer":           "Immigration lawyer",
    "Industrial Manufacturers":     "Manufacturer",
    "Construction and Engineering": "Contractor",
    "Medical Laboratories":         "Medical lab",
    "HR and Staffing":              "Staffing agency",
    "Pharma and Biotech":           "Biotech company",
    "Interior Design":              "Interior designer",
    "Electronics and Hardware":     "Electronics store",
    "Financial Services":           "Financial advisor",
    "Consumer Products":            "Consumer products",
    "Managed Care":                 "Managed care provider",
    "Agriculture and Farming":      "Farm",
    "Logistics and Supply Chain":   "Logistics company",
    "Insurance Providers":          "Insurance company",
    "Medical Supply Distributors":  "Medical supplier",
    "Automotive":                   "Car dealership",
    "Packaging Suppliers":          "Packaging supplier",
    "Real Estate and Trust Lawyer": "Estate lawyer",
    "Apparel and Footwear":         "Clothing brand",
    "Holding Companies":            "Holding company",
    "Legal Services":               "Lawyer",
    "Family Lawyer":                "Family lawyer",
    "Transportation Services":      "Transportation company",
    "Environmental Services":       "Environmental company",
    "Chemicals and Materials":      "Chemical supplier",
    "Personal Injury Lawyer":       "Personal injury lawyer",
    "Security Services":            "Security Services",
    "Aerospace and Defense":        "Aerospace company",
    "Energy and Utilities":         "Energy provider",
    "Government Services":          "Administrative services company",
    "Accounting and Tax Services":  "Accounting services",
    "Telecommunications":           "Telecom company",
}


def compute_industry2(industry: str | None) -> str | None:
    """Return the canonical industry2 label for an industry name.

    Returns None when industry is null/empty or unmapped — never returns a
    best-guess label. Unmapped industries should be added explicitly to
    INDUSTRY2_BY_INDUSTRY (and the matching DB backfill).
    """
    if not industry or not isinstance(industry, str):
        return None
    name = industry.strip()
    if not name:
        return None
    return INDUSTRY2_BY_INDUSTRY.get(name)
