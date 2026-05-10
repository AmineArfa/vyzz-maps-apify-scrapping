"""Tests for the `industry2` mapping + propagation to Instantly payloads.

industry2 is a deterministic relabel of industry for email copy. It is
computed on INSERT in raw.scraped_leads and then sent to Instantly as a
custom variable on both create (POST /leads/add) and update (PATCH /leads/{id}).
"""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch


# Stub `streamlit` for headless test runs (same pattern as test_instantly_industry).
if "streamlit" not in sys.modules:
    stub = types.ModuleType("streamlit")
    stub.write = lambda *a, **k: None
    stub.json = lambda *a, **k: None
    stub.progress = lambda *a, **k: types.SimpleNamespace(progress=lambda *a, **k: None)
    sys.modules["streamlit"] = stub


from leadgen import instantly  # noqa: E402
from leadgen.industry2 import compute_industry2, INDUSTRY2_BY_INDUSTRY  # noqa: E402
from leadgen.sync_manager import _build_patch_payload  # noqa: E402


class _FakeResp:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json = json_data or {"created_leads": [{"id": "00000000-0000-0000-0000-000000000001"}]}
        self.text = ""

    def json(self):
        return self._json


class ComputeIndustry2Tests(unittest.TestCase):
    def test_known_mappings(self):
        cases = {
            "Business Consulting": "Consulting firm",
            "Clinic Services": "Medical Clinic",
            "Restaurants and Bars": "Restaurant",
            "Med Spa": "Med spa",
            "Hotels and Leisure": "Hotel",
            "Telecommunications": "Telecom company",
        }
        for src, expected in cases.items():
            with self.subTest(industry=src):
                self.assertEqual(compute_industry2(src), expected)

    def test_null_or_empty_returns_none(self):
        for v in (None, "", "   ", []):
            with self.subTest(value=v):
                self.assertIsNone(compute_industry2(v))

    def test_unknown_industry_returns_none(self):
        # Never best-guess — unknown industries surface as None so the
        # operator notices the gap and adds the mapping explicitly.
        self.assertIsNone(compute_industry2("Some Industry That Does Not Exist"))

    def test_whitespace_is_stripped_before_lookup(self):
        self.assertEqual(compute_industry2("  Med Spa  "), "Med spa")

    def test_map_covers_all_53_known_industries(self):
        # Sanity: the constant has at least the 53 distinct values from
        # raw.scraped_leads. Counted from the migration backfill.
        self.assertGreaterEqual(len(INDUSTRY2_BY_INDUSTRY), 53)


class CreatePayloadTests(unittest.TestCase):
    """`export_leads_to_instantly` must include industry2 in custom_variables."""

    def _capture_create_payload(self, lead: dict) -> dict:
        captured = {}

        def fake_request(method, url, *, headers=None, params=None, json_payload=None, timeout=30, **_):
            captured["method"] = method
            captured["url"] = url
            captured["payload"] = json_payload
            return _FakeResp()

        with patch.object(instantly, "_request_with_retry", side_effect=fake_request), \
                patch.object(instantly, "ensure_campaign_variables", return_value=(True, None)):
            cnt, created, _, err = instantly.export_leads_to_instantly(
                api_key="test-key",
                campaign_id="cmp-123",
                leads=[lead],
                debug=False,
            )
        self.assertIsNone(err)
        self.assertEqual(cnt, 1)
        return captured["payload"]

    def test_industry2_present_when_provided(self):
        payload = self._capture_create_payload({
            "key_contact_email": "alice@example.com",
            "industry": "Med Spa",
            "industry2": "Med spa",
        })
        cv = payload["leads"][0].get("custom_variables") or {}
        self.assertEqual(cv.get("industry2"), "Med spa")

    def test_industry2_omitted_when_null(self):
        # Empty / null industry2 must be dropped — Instantly would render
        # the empty literal in templates.
        for missing in [None, "", [], "[undefined]"]:
            with self.subTest(value=missing):
                payload = self._capture_create_payload({
                    "key_contact_email": "x@y.com",
                    "industry": "Dentist",
                    "industry2": missing,
                })
                cv = payload["leads"][0].get("custom_variables") or {}
                self.assertNotIn("industry2", cv)

    def test_industry_and_industry2_both_present(self):
        # When both are set on the source row, both end up in the payload.
        payload = self._capture_create_payload({
            "key_contact_email": "alice@example.com",
            "industry": "Restaurants and Bars",
            "industry2": "Restaurant",
            "ticket_tier": "low",
        })
        cv = payload["leads"][0]["custom_variables"]
        self.assertEqual(cv["industry"], "Restaurants and Bars")
        self.assertEqual(cv["industry2"], "Restaurant")
        self.assertEqual(cv["ticket_tier"], "low")


class PatchPayloadTests(unittest.TestCase):
    """`_build_patch_payload` propagates industry2 to PATCH custom_variables."""

    def test_industry2_included_when_present(self):
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com", "industry": "Dentist", "industry2": "Dentist"},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
        )
        self.assertEqual(payload["custom_variables"]["industry2"], "Dentist")

    def test_industry2_omitted_when_empty(self):
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com", "industry": "Dentist", "industry2": ""},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
        )
        self.assertNotIn("industry2", payload["custom_variables"] or {})

    def test_existing_industry2_preserved_when_source_missing(self):
        # If the source row has no industry2 (e.g. legacy unmapped industry),
        # we must not blow away whatever Instantly already has.
        existing = {"industry2": "Restaurant"}
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com"},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
            existing_custom_variables=existing,
        )
        self.assertEqual(payload["custom_variables"]["industry2"], "Restaurant")

    def test_source_industry2_wins_over_existing(self):
        existing = {"industry2": "Old Label"}
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com", "industry2": "New Label"},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
            existing_custom_variables=existing,
        )
        self.assertEqual(payload["custom_variables"]["industry2"], "New Label")


if __name__ == "__main__":
    unittest.main()
