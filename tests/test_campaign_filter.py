"""Campaign filter spec — validation and SQL building."""
from __future__ import annotations

import unittest

from leadgen.campaign_filter import (
    FilterSpecError,
    PROTECTED_INSTANTLY_CAMPAIGN_IDS,
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
    # build_where now always appends a protected-campaigns guard AND an
    # `excluded_at IS NULL` so soft-deleted rows and hand-curated nurture
    # campaign members never come through any filter path.
    _PROTECTED_CLAUSE = (
        " AND (instantly_campaign_id IS NULL "
        "OR instantly_campaign_id <> ALL(%s))"
    )
    _EXCLUDED_TAIL = _PROTECTED_CLAUSE + " AND excluded_at IS NULL"
    _PROTECTED_PARAM = list(PROTECTED_INSTANTLY_CAMPAIGN_IDS)

    def test_industry_filter_sql(self):
        where, params = build_where(
            {"type": "industry", "value": "Med Spa"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(where, "industry = %s" + self._EXCLUDED_TAIL)
        self.assertEqual(params, ["Med Spa", self._PROTECTED_PARAM])

    def test_ticket_tier_filter_sql(self):
        where, params = build_where(
            {"type": "ticket_tier", "value": "low"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(where, "ticket_tier = %s" + self._EXCLUDED_TAIL)
        self.assertEqual(params, ["low", self._PROTECTED_PARAM])

    def test_combined_filter_sql(self):
        where, params = build_where(
            {"type": "industry_and_tier", "industry": "Med Spa", "tier": "low"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(
            where,
            "industry = %s AND ticket_tier = %s" + self._EXCLUDED_TAIL,
        )
        self.assertEqual(params, ["Med Spa", "low", self._PROTECTED_PARAM])

    def test_exclude_active_campaign_appends_clause(self):
        where, params = build_where(
            {"type": "industry", "value": "Med Spa"},
            exclude_in_active_campaign=True,
        )
        self.assertEqual(
            where,
            "industry = %s AND instantly_campaign_id IS NULL"
            + self._EXCLUDED_TAIL,
        )
        # No new params from the IS NULL clause — protected list is the
        # only extra param appended.
        self.assertEqual(params, ["Med Spa", self._PROTECTED_PARAM])

    def test_values_bound_not_interpolated(self):
        # Sanity: even if a value contains SQL meta-characters it's a bound
        # parameter, never an embedded string.
        where, params = build_where(
            {"type": "industry", "value": "'; DROP TABLE leads; --"},
            exclude_in_active_campaign=False,
        )
        self.assertEqual(where, "industry = %s" + self._EXCLUDED_TAIL)
        self.assertEqual(
            params,
            ["'; DROP TABLE leads; --", self._PROTECTED_PARAM],
        )

    def test_exclude_already_in_campaign_id(self):
        target = "12345678-1234-1234-1234-123456789012"
        where, params = build_where(
            {"type": "ticket_tier", "value": "low"},
            exclude_in_active_campaign=False,
            exclude_already_in_campaign_id=target,
        )
        self.assertEqual(
            where,
            "ticket_tier = %s AND "
            "(instantly_campaign_id IS NULL OR instantly_campaign_id <> %s)"
            + self._EXCLUDED_TAIL,
        )
        self.assertEqual(params, ["low", target, self._PROTECTED_PARAM])

    def test_excluded_rows_always_filtered(self):
        # Confirm the tail is appended even with all toggles off.
        where, _ = build_where(
            {"type": "industry", "value": "Med Spa"},
            exclude_in_active_campaign=False,
        )
        self.assertTrue(where.endswith(" AND excluded_at IS NULL"))

    def test_protected_campaign_always_filtered(self):
        # The Free Audit Completers guard must show up in EVERY filter
        # path — recategorize, segment-push, count, fetch. Asserting on
        # the SQL clause is the cheapest way to lock that in.
        where, params = build_where(
            {"type": "ticket_tier", "value": "high"},
            exclude_in_active_campaign=False,
        )
        self.assertIn(
            "instantly_campaign_id <> ALL(%s)",
            where,
            "protected-campaign clause must be present",
        )
        self.assertIn(self._PROTECTED_PARAM, params)
        # Sanity: the actual Free Audit Completers id is in the protected
        # list — guards against accidental deletion of the constant.
        self.assertIn(
            "54b4cd61-9cc5-4542-a6d0-5dd4764026ec",
            self._PROTECTED_PARAM,
        )

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
