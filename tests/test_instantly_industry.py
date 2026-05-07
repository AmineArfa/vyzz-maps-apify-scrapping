"""Tests for the `industry` custom variable on Instantly create/update payloads.

These tests cover the two code paths that send custom_variables to Instantly:
- Create: leadgen.instantly.export_leads_to_instantly (POST /leads/add)
- Update: leadgen.sync_manager._build_patch_payload (PATCH /leads/{id})

For the create path we patch the HTTP layer and capture the outgoing JSON; for
the update path we call the helper directly since it is a pure function.
"""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Stub `streamlit` so the leadgen modules can be imported without the real
# dependency. The functions under test only use st.write/st.json for debug
# output, which we can safely no-op.
# ---------------------------------------------------------------------------
if "streamlit" not in sys.modules:
    stub = types.ModuleType("streamlit")
    stub.write = lambda *a, **k: None
    stub.json = lambda *a, **k: None
    stub.progress = lambda *a, **k: types.SimpleNamespace(progress=lambda *a, **k: None)
    sys.modules["streamlit"] = stub


from leadgen import instantly  # noqa: E402
from leadgen.sync_manager import _build_patch_payload  # noqa: E402


class _FakeResp:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json = json_data or {"created_leads": [{"id": "00000000-0000-0000-0000-000000000001"}]}
        self.text = ""

    def json(self):
        return self._json


class CreatePayloadTests(unittest.TestCase):
    """`export_leads_to_instantly` must include industry in custom_variables."""

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

    def test_industry_present_when_source_has_value(self):
        lead = {
            "key_contact_email": "alice@example.com",
            "key_contact_name": "Alice Doe",
            "company_name": "Acme",
            "industry": "Med Spa",
            "city": "Austin",
        }
        payload = self._capture_create_payload(lead)
        leads = payload["leads"]
        self.assertEqual(len(leads), 1)
        cv = leads[0].get("custom_variables") or {}
        self.assertEqual(cv.get("industry"), "Med Spa")

    def test_industry_passed_verbatim(self):
        # Canonical names must round-trip unchanged (no casing / whitespace fixes).
        for name in [
            "Business Consulting",
            "Restaurants and Bars",
            "Hotels and Leisure",
            "Elder and Disabled Care",
            "Personal Injury Lawyer",
        ]:
            with self.subTest(industry=name):
                payload = self._capture_create_payload({
                    "key_contact_email": "x@y.com",
                    "industry": name,
                })
                cv = payload["leads"][0].get("custom_variables") or {}
                self.assertEqual(cv.get("industry"), name)

    def test_industry_omitted_when_null(self):
        # Empty / null industry must be dropped, not sent as "" — Instantly
        # would render the empty literal in templates.
        for missing in [None, "", [], "[undefined]"]:
            with self.subTest(value=missing):
                payload = self._capture_create_payload({
                    "key_contact_email": "x@y.com",
                    "industry": missing,
                })
                cv = payload["leads"][0].get("custom_variables") or {}
                self.assertNotIn("industry", cv)


class PatchPayloadTests(unittest.TestCase):
    """`_build_patch_payload` is the canonical builder for PATCH updates."""

    def test_industry_included_when_present(self):
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com", "industry": "Dentist"},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
        )
        self.assertEqual(payload["custom_variables"]["industry"], "Dentist")

    def test_industry_omitted_when_empty(self):
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com", "industry": ""},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
        )
        self.assertNotIn("industry", payload["custom_variables"] or {})

    def test_existing_custom_variables_are_merged(self):
        # Keys outside the managed set must survive the PATCH (Instantly
        # replaces the entire custom_variables dict — caller fetches first
        # and we merge here).
        existing = {"some_other_var": "keep-me", "industry": "Old Value"}
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com", "industry": "Med Spa"},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
            existing_custom_variables=existing,
        )
        cv = payload["custom_variables"]
        self.assertEqual(cv["some_other_var"], "keep-me")
        # Our value wins on conflict.
        self.assertEqual(cv["industry"], "Med Spa")

    def test_existing_industry_preserved_when_source_missing(self):
        # If the source row has no industry, we must not blow away whatever
        # Instantly already has (could have been set via the one-off backfill).
        existing = {"industry": "Med Spa"}
        payload = _build_patch_payload(
            {"key_contact_email": "a@b.com"},
            instantly_lead_id="00000000-0000-0000-0000-000000000001",
            existing_custom_variables=existing,
        )
        self.assertEqual(payload["custom_variables"]["industry"], "Med Spa")


if __name__ == "__main__":
    unittest.main()
