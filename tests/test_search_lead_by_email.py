"""search_lead_by_email — verify the v2 API contract is honored.

The previous implementation sent {"email": ...} which Instantly silently
ignored, causing every reconcile call to receive an arbitrary first lead.
That linked thousands of raw rows to the same Instantly id. These tests
lock in the correct behavior.
"""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock, patch


if "streamlit" not in sys.modules:
    stub = types.ModuleType("streamlit")
    stub.write = lambda *a, **k: None
    sys.modules["streamlit"] = stub


from leadgen import instantly  # noqa: E402


class _FakeResp:
    def __init__(self, status_code=200, items=None):
        self.status_code = status_code
        self._items = items or []
        self.text = ""

    def json(self):
        return {"items": self._items}


class SearchLeadByEmailTests(unittest.TestCase):
    def _capture_payload(self, items):
        captured = {}

        def fake_request(method, url, *, headers=None, params=None, json_payload=None, timeout=20, **_):
            captured["method"] = method
            captured["url"] = url
            captured["payload"] = json_payload
            return _FakeResp(items=items)

        return fake_request, captured

    def test_uses_contacts_array_not_email_field(self):
        # The OpenAPI spec says the body field is `contacts: [email]`.
        # Sending `email` (the old shape) is silently ignored by the API.
        items = [{"id": "abc", "email": "alice@example.com"}]
        fake_request, captured = self._capture_payload(items)
        with patch.object(instantly, "_request_with_retry", side_effect=fake_request):
            lead, err = instantly.search_lead_by_email("k", "alice@example.com")
        self.assertIsNone(err)
        self.assertEqual(lead["id"], "abc")
        # Critical assertion: payload uses `contacts`, not `email`.
        payload = captured["payload"]
        self.assertIn("contacts", payload)
        self.assertEqual(payload["contacts"], ["alice@example.com"])
        self.assertNotIn("email", payload)

    def test_email_normalised_to_lowercase(self):
        fake_request, captured = self._capture_payload([])
        with patch.object(instantly, "_request_with_retry", side_effect=fake_request):
            instantly.search_lead_by_email("k", "  ALICE@Example.com  ")
        self.assertEqual(captured["payload"]["contacts"], ["alice@example.com"])

    def test_drops_response_when_email_does_not_match(self):
        # Defense-in-depth: even if the API returns a lead, we ignore it
        # unless its email matches our query exactly. Prevents silent
        # mis-linking if Instantly ever changes the param name again.
        items = [{"id": "wrong", "email": "someone-else@example.com"}]
        fake_request, _ = self._capture_payload(items)
        with patch.object(instantly, "_request_with_retry", side_effect=fake_request):
            lead, err = instantly.search_lead_by_email("k", "alice@example.com")
        self.assertIsNone(lead)
        self.assertIsNone(err)  # Not an error — just not found.

    def test_picks_matching_lead_among_several(self):
        # Limit is 5 and the API may return adjacent leads with similar
        # emails. We must scan all returned items and only take the exact
        # match, never assume items[0] is the right one.
        items = [
            {"id": "X", "email": "almost-alice@example.com"},
            {"id": "right", "email": "alice@example.com"},
        ]
        fake_request, _ = self._capture_payload(items)
        with patch.object(instantly, "_request_with_retry", side_effect=fake_request):
            lead, err = instantly.search_lead_by_email("k", "alice@example.com")
        self.assertEqual(lead["id"], "right")

    def test_campaign_filter_uses_correct_key(self):
        # When filtering by campaign, the body field is `campaign`,
        # not `campaign_id`. Lock that in.
        fake_request, captured = self._capture_payload([])
        with patch.object(instantly, "_request_with_retry", side_effect=fake_request):
            instantly.search_lead_by_email("k", "a@b.com", campaign_id="c-123")
        self.assertEqual(captured["payload"]["campaign"], "c-123")
        self.assertNotIn("campaign_id", captured["payload"])


if __name__ == "__main__":
    unittest.main()
