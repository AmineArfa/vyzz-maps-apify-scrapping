"""Campaign filter spec — validation and SQL building."""
from __future__ import annotations

import unittest

from leadgen.campaign_filter import (
    FilterSpecError,
    build_where,
    describe,
    spec_from_picker,
    validate_filter_spec,
)


class ValidateTests(unittest.TestCase):
    def test_industry_shape(self):
        self.assertEqual(
            validate_filter_spec({"type": "industry", "value": "Med Spa"}),
            {"type": "industry", "value": "Med Spa"},
        )

    def test_industry_strips_whitespace(self):
        self.assertEqual(
            validate_filter_spec({"type": "industry", "value": "  Med Spa  "}),
            {"type": "industry", "value": "Med Spa"},
        )

    def test_industry_rejects_empty(self):
        with self.assertRaises(FilterSpecError):
            validate_filter_spec({"type": "industry", "value": ""})

    def test_ticket_tier_shape(self):
        for tier in ["high", "mid", "low"]:
            with self.subTest(tier=tier):
                self.assertEqual(
                    validate_filter_spec({"type": "ticket_tier", "value": tier}),
                    {"type": "ticket_tier", "value": tier},
                )

    def test_ticket_tier_rejects_invalid(self):
        with self.assertRaises(FilterSpecError):
            validate_filter_spec({"type": "ticket_tier", "value": "premium"})

    def test_industry_and_tier_shape(self):
        self.assertEqual(
            validate_filter_spec({
                "type": "industry_and_tier",
                "industry": "Med Spa",
                "tier": "low",
            }),
            {"type": "industry_and_tier", "industry": "Med Spa", "tier": "low"},
        )

    def test_unknown_type_rejected(self):
        with self.assertRaises(FilterSpecError):
            validate_filter_spec({"type": "city", "value": "Austin"})

    def test_non_dict_rejected(self):
        with self.assertRaises(FilterSpecError):
            validate_filter_spec(None)
        with self.assertRaises(FilterSpecError):
            validate_filter_spec("industry=Med Spa")


class SpecFromPickerTests(unittest.TestCase):
    def test_industry_only(self):
        self.assertEqual(
            spec_from_picker("Med Spa", "(any)"),
            {"type": "industry", "value": "Med Spa"},
        )

    def test_tier_only(self):
        self.assertEqual(
            spec_from_picker("(any)", "low"),
            {"type": "ticket_tier", "value": "low"},
        )

    def test_both_axes(self):
        self.assertEqual(
            spec_from_picker("Med Spa", "low"),
            {"type": "industry_and_tier", "industry": "Med Spa", "tier": "low"},
        )

    def test_both_any_rejected(self):
        with self.assertRaises(FilterSpecError):
            spec_from_picker("(any)", "(any)")
        with self.assertRaises(FilterSpecError):
            spec_from_picker("", None)


class BuildWhereTests(unittest.TestCase):
    def test_industry_filter_sql(self):
        where, params = build_where(
            {"type": "industry", "value": "Med Spa"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(where, "industry = %s")
        self.assertEqual(params, ["Med Spa"])

    def test_ticket_tier_filter_sql(self):
        where, params = build_where(
            {"type": "ticket_tier", "value": "low"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(where, "ticket_tier = %s")
        self.assertEqual(params, ["low"])

    def test_combined_filter_sql(self):
        where, params = build_where(
            {"type": "industry_and_tier", "industry": "Med Spa", "tier": "low"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(where, "industry = %s AND ticket_tier = %s")
        self.assertEqual(params, ["Med Spa", "low"])

    def test_exclude_active_campaign_appends_clause(self):
        where, params = build_where(
            {"type": "industry", "value": "Med Spa"},
            exclude_in_active_campaign=True,
        )
        self.assertEqual(where, "industry = %s AND instantly_campaign_id IS NULL")
        # No new params — IS NULL is parameter-free.
        self.assertEqual(params, ["Med Spa"])

    def test_values_bound_not_interpolated(self):
        # Sanity: even if a value contains SQL meta-characters it's a bound
        # parameter, never an embedded string.
        where, params = build_where(
            {"type": "industry", "value": "'; DROP TABLE leads; --"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(where, "industry = %s")
        self.assertEqual(params, ["'; DROP TABLE leads; --"])

    def test_describe_each_shape(self):
        self.assertEqual(
            describe({"type": "industry", "value": "Med Spa"}),
            "industry = Med Spa",
        )
        self.assertEqual(
            describe({"type": "ticket_tier", "value": "low"}),
            "tier = low",
        )
        self.assertEqual(
            describe({"type": "industry_and_tier", "industry": "Med Spa", "tier": "low"}),
            "industry = Med Spa AND tier = low",
        )


if __name__ == "__main__":
    unittest.main()
