"""Push flow: MOVE existing leads, CREATE new ones, write back the id."""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch


# Stub streamlit before any leadgen imports.
if "streamlit" not in sys.modules:
    stub = types.ModuleType("streamlit")
    stub.write = lambda *a, **k: None
    stub.json = lambda *a, **k: None
    stub.error = lambda *a, **k: None
    sys.modules["streamlit"] = stub


from leadgen import campaign_push, instantly  # noqa: E402


class _CapturingBackend:
    """Captures batch_update calls so tests can assert the writeback shape."""

    def __init__(self):
        self.updates: list[dict] = []

    def batch_update(self, updates):
        self.updates.extend(updates)
        return True


def _uuid(n: int) -> str:
    """Build a syntactically valid Instantly UUID for test use."""
    h = f"{n:032x}"
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


CAMPAIGN_ID = _uuid(1)


class PushFlowTests(unittest.TestCase):
    def test_existing_lead_is_moved_not_created(self):
        backend = _CapturingBackend()
        existing_id = _uuid(2)
        leads = [{
            "id": "raw-1",
            "key_contact_email": "alice@example.com",
            "company_name": "Acme",
            "industry": "Med Spa",
            "ticket_tier": "low",
            "instantly_lead_id": existing_id,
        }]

        with patch.object(campaign_push, "move_lead_to_campaign", return_value=(True, None)) as m_move, \
                patch.object(campaign_push, "export_leads_to_instantly") as m_create:
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        m_move.assert_called_once_with("k", existing_id, CAMPAIGN_ID, debug=False)
        m_create.assert_not_called()
        self.assertEqual(result["moved"], 1)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["failed"], 0)
        # Writeback: instantly_lead_id stays the same, campaign_id is the target.
        self.assertEqual(len(backend.updates), 1)
        upd = backend.updates[0]
        self.assertEqual(upd["id"], "raw-1")
        self.assertEqual(upd["fields"]["instantly_lead_id"], existing_id)
        self.assertEqual(upd["fields"]["instantly_campaign_id"], CAMPAIGN_ID)

    def test_new_lead_is_created_and_written_back(self):
        backend = _CapturingBackend()
        new_id = _uuid(3)
        leads = [{
            "id": "raw-2",
            "key_contact_email": "bob@example.com",
            "company_name": "Bob Co",
            "industry": "Dentist",
            "ticket_tier": "mid",
            "instantly_lead_id": None,
        }]

        with patch.object(campaign_push, "move_lead_to_campaign") as m_move, \
                patch.object(
                    campaign_push, "export_leads_to_instantly",
                    return_value=(1, [{"id": new_id, "email": "bob@example.com"}], {}, None),
                ) as m_create, \
                patch.object(campaign_push, "inject_lid_to_lead", return_value=(True, None)):
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        m_move.assert_not_called()
        m_create.assert_called_once()
        # The lead dict and campaign id were forwarded.
        args, kwargs = m_create.call_args
        self.assertEqual(args[1], CAMPAIGN_ID)  # campaign_id positional
        self.assertEqual(args[2][0]["key_contact_email"], "bob@example.com")
        # Writeback links the new id back to raw.
        self.assertEqual(result["created"], 1)
        self.assertEqual(len(backend.updates), 1)
        upd = backend.updates[0]
        self.assertEqual(upd["id"], "raw-2")
        self.assertEqual(upd["fields"]["instantly_lead_id"], new_id)
        self.assertEqual(upd["fields"]["instantly_campaign_id"], CAMPAIGN_ID)

    def test_lead_without_email_is_skipped(self):
        backend = _CapturingBackend()
        leads = [{"id": "raw-3", "key_contact_email": None, "instantly_lead_id": None}]
        with patch.object(campaign_push, "move_lead_to_campaign") as m_move, \
                patch.object(campaign_push, "export_leads_to_instantly") as m_create:
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )
        m_move.assert_not_called()
        m_create.assert_not_called()
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["created"], 0)
        self.assertEqual(backend.updates, [])  # no writeback for skipped

    def test_create_returning_zero_falls_back_to_search_and_move(self):
        # Lead has no instantly_lead_id locally, but Instantly already has
        # one under that email (e.g. created manually). Create returns 0;
        # we search by email and move the found lead instead. This keeps
        # us at exactly one Instantly lead per email — never duplicates.
        backend = _CapturingBackend()
        found_id = _uuid(4)
        leads = [{
            "id": "raw-4",
            "key_contact_email": "carol@example.com",
            "instantly_lead_id": None,
        }]

        with patch.object(campaign_push, "export_leads_to_instantly", return_value=(0, [], {}, None)), \
                patch.object(
                    campaign_push, "search_lead_by_email",
                    return_value=({"id": found_id, "email": "carol@example.com"}, None),
                ), \
                patch.object(campaign_push, "move_lead_to_campaign", return_value=(True, None)) as m_move, \
                patch.object(campaign_push, "inject_lid_to_lead", return_value=(True, None)):
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        m_move.assert_called_once_with("k", found_id, CAMPAIGN_ID, debug=False)
        self.assertEqual(result["moved"], 1)
        self.assertEqual(result["created"], 0)
        # Writeback links the found id back to raw-4.
        self.assertEqual(backend.updates[0]["fields"]["instantly_lead_id"], found_id)
        self.assertEqual(backend.updates[0]["fields"]["instantly_campaign_id"], CAMPAIGN_ID)

    def test_failed_move_does_not_writeback(self):
        backend = _CapturingBackend()
        existing_id = _uuid(5)
        leads = [{
            "id": "raw-5",
            "key_contact_email": "dave@example.com",
            "instantly_lead_id": existing_id,
        }]
        with patch.object(campaign_push, "move_lead_to_campaign", return_value=(False, "boom")):
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["moved"], 0)
        self.assertEqual(backend.updates, [])

    def test_mixed_batch_split_correctly(self):
        backend = _CapturingBackend()
        existing_id = _uuid(6)
        new_id = _uuid(7)
        leads = [
            {"id": "raw-A", "key_contact_email": "a@x.com", "instantly_lead_id": existing_id},
            {"id": "raw-B", "key_contact_email": "b@x.com", "instantly_lead_id": None},
            {"id": "raw-C", "key_contact_email": None, "instantly_lead_id": None},
        ]
        with patch.object(campaign_push, "move_lead_to_campaign", return_value=(True, None)), \
                patch.object(
                    campaign_push, "export_leads_to_instantly",
                    return_value=(1, [{"id": new_id, "email": "b@x.com"}], {}, None),
                ), \
                patch.object(campaign_push, "inject_lid_to_lead", return_value=(True, None)):
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )
        self.assertEqual(result["moved"], 1)
        self.assertEqual(result["created"], 1)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["failed"], 0)
        # Two writebacks: the moved row and the created row.
        ids_written = sorted(u["id"] for u in backend.updates)
        self.assertEqual(ids_written, ["raw-A", "raw-B"])


class RecategorizeAllTests(unittest.TestCase):
    """recategorize_all_by_tier loops the three tiers and aggregates."""

    def _make_backend(self, leads_by_tier):
        captured = {"updates": [], "fetched": []}

        class _B:
            def fetch_leads_by_filter(self, spec, *, limit=None, exclude_in_active_campaign=True):
                captured["fetched"].append((spec, exclude_in_active_campaign))
                return list(leads_by_tier.get(spec["value"], []))

            def batch_update(self, updates):
                captured["updates"].extend(updates)
                return True

        return _B(), captured

    def test_iterates_three_tiers_with_full_set(self):
        # exclude_in_active_campaign must be False so already-in-campaign
        # leads get moved. That's the whole point of recategorization.
        leads_by_tier = {
            "low": [{"id": "l1", "key_contact_email": "l@x.com", "instantly_lead_id": _uuid(10)}],
            "mid": [{"id": "m1", "key_contact_email": "m@x.com", "instantly_lead_id": _uuid(11)}],
            "high": [{"id": "h1", "key_contact_email": "h@x.com", "instantly_lead_id": _uuid(12)}],
        }
        backend, captured = self._make_backend(leads_by_tier)
        camp_ids = {"low": _uuid(20), "mid": _uuid(21), "high": _uuid(22)}

        with patch.object(campaign_push, "move_lead_to_campaign", return_value=(True, None)):
            result = campaign_push.recategorize_all_by_tier(
                backend, api_key="k",
                resolve_campaign_id=lambda t: camp_ids[t],
                max_workers=1,
            )

        # All three tier filters were applied.
        seen_specs = [s for s, _ in captured["fetched"]]
        self.assertEqual(
            sorted(s["value"] for s in seen_specs),
            ["high", "low", "mid"],
        )
        # Each filter call disabled exclude_in_active_campaign.
        self.assertTrue(all(not excl for _, excl in captured["fetched"]))

        self.assertEqual(result["moved"], 3)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(set(result["by_tier"].keys()), {"low", "mid", "high"})
        for tier, c_id in camp_ids.items():
            self.assertEqual(result["by_tier"][tier]["campaign_id"], c_id)
            self.assertEqual(result["by_tier"][tier]["moved"], 1)

    def test_unresolved_campaign_records_error_does_not_block_others(self):
        # If a tier's campaign can't be resolved, we record the error but
        # keep going with the remaining tiers — partial recategorization
        # is better than nothing.
        leads_by_tier = {
            "low": [{"id": "l1", "key_contact_email": "l@x.com", "instantly_lead_id": _uuid(13)}],
            "mid": [],
            "high": [{"id": "h1", "key_contact_email": "h@x.com", "instantly_lead_id": _uuid(14)}],
        }
        backend, _ = self._make_backend(leads_by_tier)
        camp_ids = {"low": _uuid(23), "mid": None, "high": _uuid(24)}

        with patch.object(campaign_push, "move_lead_to_campaign", return_value=(True, None)):
            result = campaign_push.recategorize_all_by_tier(
                backend, api_key="k",
                resolve_campaign_id=lambda t: camp_ids.get(t),
                max_workers=1,
            )

        self.assertEqual(result["moved"], 2)
        self.assertIsNone(result["by_tier"]["mid"]["campaign_id"])
        self.assertIn("could not resolve campaign", result["by_tier"]["mid"]["error"])
        # Other two tiers still completed.
        self.assertEqual(result["by_tier"]["low"]["moved"], 1)
        self.assertEqual(result["by_tier"]["high"]["moved"], 1)


class MoveLeadHelperTests(unittest.TestCase):
    """The move_lead_to_campaign HTTP helper — argument validation."""

    def test_invalid_uuid_returns_error(self):
        ok, err = instantly.move_lead_to_campaign("k", "not-a-uuid", _uuid(8))
        self.assertFalse(ok)
        self.assertIn("Invalid Lead ID", err)

    def test_missing_campaign_id_returns_error(self):
        ok, err = instantly.move_lead_to_campaign("k", _uuid(9), "")
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
