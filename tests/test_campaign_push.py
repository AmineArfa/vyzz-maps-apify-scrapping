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
    def test_existing_lead_is_bulk_moved_not_created(self):
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

        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(True, None)) as m_bulk, \
                patch.object(campaign_push, "export_leads_to_instantly") as m_create:
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        m_bulk.assert_called_once_with("k", [existing_id], CAMPAIGN_ID, debug=False)
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

    def test_bulk_move_chunks_at_configured_size(self):
        # 250 leads with instantly_lead_id should be moved in chunks of
        # _BULK_MOVE_CHUNK (currently 50) — five chunks of 50, not 250
        # individual API calls. Throttle sleep is patched out so tests
        # don't actually wait between chunks.
        backend = _CapturingBackend()
        leads = [{
            "id": f"raw-{i}", "key_contact_email": f"u{i}@x.com",
            "instantly_lead_id": _uuid(100 + i),
        } for i in range(250)]

        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(True, None)) as m_bulk, \
                patch.object(campaign_push.time, "sleep"):
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        expected_chunks = -(-250 // campaign_push._BULK_MOVE_CHUNK)  # ceil
        self.assertEqual(m_bulk.call_count, expected_chunks)
        chunk_sizes = [len(call.args[1]) for call in m_bulk.call_args_list]
        self.assertEqual(sum(chunk_sizes), 250)
        self.assertTrue(all(sz <= campaign_push._BULK_MOVE_CHUNK for sz in chunk_sizes))
        self.assertEqual(result["moved"], 250)
        self.assertEqual(len(backend.updates), 250)

    def test_bulk_move_failure_falls_back_to_per_lead(self):
        # When the bulk endpoint fails, fall back to per-lead so granular
        # success/failure is recorded — never lose the chunk wholesale.
        backend = _CapturingBackend()
        existing_id = _uuid(2)
        leads = [{
            "id": "raw-1",
            "key_contact_email": "alice@example.com",
            "instantly_lead_id": existing_id,
        }]

        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(False, "boom")) as m_bulk, \
                patch.object(campaign_push, "move_lead_to_campaign", return_value=(True, None)) as m_single:
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        m_bulk.assert_called_once()
        m_single.assert_called_once_with("k", existing_id, CAMPAIGN_ID, debug=False)
        self.assertEqual(result["moved"], 1)

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

    def test_already_in_target_campaign_is_skipped_no_api_call(self):
        # Defensive check: even if the SQL filter let one through, the
        # push must not call any move endpoint on a lead already in the
        # target campaign. Re-runs are then cheap and safe.
        backend = _CapturingBackend()
        existing_id = _uuid(20)
        leads = [{
            "id": "raw-already",
            "key_contact_email": "x@y.com",
            "instantly_lead_id": existing_id,
            "instantly_campaign_id": CAMPAIGN_ID,  # already in target
        }]
        with patch.object(campaign_push, "bulk_move_leads_to_campaign") as m_bulk, \
                patch.object(campaign_push, "move_lead_to_campaign") as m_move, \
                patch.object(campaign_push, "export_leads_to_instantly") as m_create:
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )
        m_bulk.assert_not_called()
        m_move.assert_not_called()
        m_create.assert_not_called()
        self.assertEqual(result["already_in_place"], 1)
        self.assertEqual(result["moved"], 0)
        self.assertEqual(backend.updates, [])

    def test_lead_in_different_campaign_is_moved(self):
        # Same shape but the lead is in a *different* campaign — it must
        # be bulk-moved into the target. The defensive skip only fires
        # for an exact campaign-id match.
        backend = _CapturingBackend()
        existing_id = _uuid(21)
        other_campaign = _uuid(99)
        leads = [{
            "id": "raw-elsewhere",
            "key_contact_email": "x@y.com",
            "instantly_lead_id": existing_id,
            "instantly_campaign_id": other_campaign,
        }]
        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(True, None)) as m_bulk:
            result = campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )
        m_bulk.assert_called_once_with("k", [existing_id], CAMPAIGN_ID, debug=False)
        self.assertEqual(result["moved"], 1)
        self.assertEqual(result["already_in_place"], 0)

    def test_failed_move_does_not_writeback(self):
        # Both bulk and per-lead move fail → recorded as failed, no writeback.
        backend = _CapturingBackend()
        existing_id = _uuid(5)
        leads = [{
            "id": "raw-5",
            "key_contact_email": "dave@example.com",
            "instantly_lead_id": existing_id,
        }]
        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(False, "boom-bulk")), \
                patch.object(campaign_push, "move_lead_to_campaign", return_value=(False, "boom-single")):
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
        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(True, None)), \
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


class ImmediateWritebackTests(unittest.TestCase):
    """Crash-safety: every successful create / move chunk persists BEFORE
    the next operation. Buffering writebacks in memory and flushing only
    at the end (the previous design) lost data on every interruption.
    """

    class _OrderTrackingBackend:
        """Records the call order of bulk_move and batch_update so we can
        prove writeback for chunk N happens before bulk_move for chunk N+1.
        """
        def __init__(self):
            self.calls: list[tuple[str, int]] = []
            self.updates: list[dict] = []

        def batch_update(self, updates):
            self.calls.append(("batch_update", len(updates)))
            self.updates.extend(updates)
            return True

    def test_writeback_flushes_per_chunk(self):
        backend = self._OrderTrackingBackend()
        leads = [{
            "id": f"raw-{i}", "key_contact_email": f"u{i}@x.com",
            "instantly_lead_id": _uuid(200 + i),
        } for i in range(120)]  # spans 3 chunks at chunk_size=50

        bulk_call_count = {"n": 0}
        def fake_bulk(api_key, ids, campaign_id, debug=False):
            bulk_call_count["n"] += 1
            backend.calls.append(("bulk_move", len(ids)))
            return True, None

        with patch.object(campaign_push, "bulk_move_leads_to_campaign", side_effect=fake_bulk), \
                patch.object(campaign_push.time, "sleep"):
            campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        # Expect call sequence: bulk, batch_update, bulk, batch_update, bulk, batch_update.
        # The previous (buggy) design was: bulk, bulk, bulk, batch_update.
        self.assertEqual(
            [name for name, _ in backend.calls],
            ["bulk_move", "batch_update",
             "bulk_move", "batch_update",
             "bulk_move", "batch_update"],
        )
        # Every writeback fires before the next bulk_move starts.
        self.assertEqual(len(backend.updates), 120)

    def test_create_writeback_is_per_lead_not_batched(self):
        backend = self._OrderTrackingBackend()
        new_id = _uuid(300)
        leads = [{
            "id": "raw-new",
            "key_contact_email": "new@x.com",
            "instantly_lead_id": None,
        }]

        with patch.object(
                    campaign_push, "export_leads_to_instantly",
                    return_value=(1, [{"id": new_id, "email": "new@x.com"}], {}, None),
                ), \
                patch.object(campaign_push, "inject_lid_to_lead", return_value=(True, None)):
            campaign_push.push_leads_to_campaign(
                backend, api_key="k", leads=leads, campaign_id=CAMPAIGN_ID, max_workers=1,
            )

        # The single create wrote back its id immediately, not at the end
        # of the function — so a crash after the API call but before the
        # next operation still persists this lead.
        self.assertEqual(len(backend.updates), 1)
        self.assertEqual(backend.updates[0]["fields"]["instantly_lead_id"], new_id)


class ReconcileUnlinkedLeadsTests(unittest.TestCase):
    """Reconciliation sweep recovers leads whose writeback was lost in a
    previous interrupted run.
    """

    class _RecBackend:
        def __init__(self, candidates):
            self.candidates = list(candidates)
            self.updates: list[dict] = []

        def fetch_unlinked_leads_with_email(self, *, limit=None):
            return list(self.candidates)

        def batch_update(self, updates):
            self.updates.extend(updates)
            return True

    def test_links_back_when_instantly_has_match(self):
        found_id = _uuid(401)
        backend = self._RecBackend([
            {"id": "raw-A", "key_contact_email": "a@x.com"},
            {"id": "raw-B", "key_contact_email": "b@x.com"},
        ])

        def fake_search(api_key, email, debug=False):
            if email == "a@x.com":
                return ({"id": found_id, "campaign": _uuid(500)}, None)
            return (None, None)

        with patch.object(campaign_push, "search_lead_by_email", side_effect=fake_search):
            result = campaign_push.reconcile_unlinked_leads(
                backend, api_key="k", max_workers=1,
            )

        self.assertEqual(result["scanned"], 2)
        self.assertEqual(result["linked"], 1)
        self.assertEqual(result["not_found"], 1)
        self.assertEqual(len(backend.updates), 1)
        upd = backend.updates[0]
        self.assertEqual(upd["id"], "raw-A")
        self.assertEqual(upd["fields"]["instantly_lead_id"], found_id)
        # If Instantly tells us the campaign too, we capture that as well.
        self.assertIn("instantly_campaign_id", upd["fields"])

    def test_no_match_records_not_found(self):
        backend = self._RecBackend([
            {"id": "raw-X", "key_contact_email": "x@x.com"},
        ])
        with patch.object(campaign_push, "search_lead_by_email", return_value=(None, None)):
            result = campaign_push.reconcile_unlinked_leads(
                backend, api_key="k", max_workers=1,
            )
        self.assertEqual(result["linked"], 0)
        self.assertEqual(result["not_found"], 1)
        self.assertEqual(backend.updates, [])

    def test_empty_candidate_set(self):
        backend = self._RecBackend([])
        # No search calls expected.
        with patch.object(campaign_push, "search_lead_by_email") as m_search:
            result = campaign_push.reconcile_unlinked_leads(
                backend, api_key="k", max_workers=1,
            )
        m_search.assert_not_called()
        self.assertEqual(result["scanned"], 0)


class RecategorizeAllTests(unittest.TestCase):
    """recategorize_all_by_tier loops the three tiers and aggregates."""

    def _make_backend(self, leads_by_tier, total_by_tier=None):
        captured = {"updates": [], "fetched": [], "counted": []}
        totals = total_by_tier or {t: len(v) for t, v in leads_by_tier.items()}

        class _B:
            def fetch_leads_by_filter(
                self, spec, *, limit=None,
                exclude_in_active_campaign=True,
                exclude_already_in_campaign_id=None,
            ):
                captured["fetched"].append({
                    "spec": spec,
                    "exclude_active": exclude_in_active_campaign,
                    "exclude_target": exclude_already_in_campaign_id,
                })
                return list(leads_by_tier.get(spec["value"], []))

            def count_leads_by_filter(
                self, spec, *,
                exclude_in_active_campaign=True,
                exclude_already_in_campaign_id=None,
            ):
                captured["counted"].append(spec)
                return totals.get(spec["value"], 0)

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

        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(True, None)):
            result = campaign_push.recategorize_all_by_tier(
                backend, api_key="k",
                resolve_campaign_id=lambda t: camp_ids[t],
                max_workers=1,
            )

        # All three tier filters were applied.
        seen_specs = [f["spec"] for f in captured["fetched"]]
        self.assertEqual(
            sorted(s["value"] for s in seen_specs),
            ["high", "low", "mid"],
        )
        # Each fetch disabled exclude_in_active_campaign...
        self.assertTrue(all(not f["exclude_active"] for f in captured["fetched"]))
        # ...and passed the resolved campaign id as exclude_already_in_campaign_id
        # so leads already in the right tier campaign are dropped at SQL level.
        for f in captured["fetched"]:
            tier = f["spec"]["value"]
            self.assertEqual(f["exclude_target"], camp_ids[tier])

        self.assertEqual(result["moved"], 3)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(set(result["by_tier"].keys()), {"low", "mid", "high"})
        for tier, c_id in camp_ids.items():
            self.assertEqual(result["by_tier"][tier]["campaign_id"], c_id)
            self.assertEqual(result["by_tier"][tier]["moved"], 1)

    def test_reports_already_in_place_per_tier(self):
        # Total in tier reported by count == 5; SQL-filtered fetch returns 2.
        # The orchestrator must report 3 already_in_place for that tier.
        leads_by_tier = {
            "low": [
                {"id": "l1", "key_contact_email": "a@x.com", "instantly_lead_id": _uuid(30)},
                {"id": "l2", "key_contact_email": "b@x.com", "instantly_lead_id": _uuid(31)},
            ],
            "mid": [],
            "high": [],
        }
        totals = {"low": 5, "mid": 0, "high": 0}
        backend, _ = self._make_backend(leads_by_tier, total_by_tier=totals)
        camp_ids = {"low": _uuid(40), "mid": _uuid(41), "high": _uuid(42)}

        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(True, None)):
            result = campaign_push.recategorize_all_by_tier(
                backend, api_key="k",
                resolve_campaign_id=lambda t: camp_ids[t],
                max_workers=1,
            )

        self.assertEqual(result["moved"], 2)
        self.assertEqual(result["already_in_place"], 3)
        self.assertEqual(result["by_tier"]["low"]["already_in_place"], 3)
        self.assertEqual(result["by_tier"]["low"]["moved"], 2)

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

        with patch.object(campaign_push, "bulk_move_leads_to_campaign", return_value=(True, None)):
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
