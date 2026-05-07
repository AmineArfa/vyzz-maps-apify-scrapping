"""Prune flow: contacted-but-never-replied → delete from Instantly + soft-delete raw."""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch


if "streamlit" not in sys.modules:
    stub = types.ModuleType("streamlit")
    stub.write = lambda *a, **k: None
    stub.error = lambda *a, **k: None
    stub.json = lambda *a, **k: None
    sys.modules["streamlit"] = stub


from leadgen import prune  # noqa: E402


def _uuid(n: int) -> str:
    h = f"{n:032x}"
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


class _SoftDeleteBackend:
    """Captures soft-delete calls; pretends both id and email match exist."""

    def __init__(self, miss_ids: set | None = None):
        self.calls: list[dict] = []
        self.miss_ids = miss_ids or set()

    def soft_delete_by_instantly_id_or_email(self, *, instantly_lead_id, email, fields):
        self.calls.append({
            "instantly_lead_id": instantly_lead_id,
            "email": email,
            "fields": dict(fields),
        })
        if instantly_lead_id in self.miss_ids:
            return False
        return True


class IndustryExtractionTests(unittest.TestCase):
    def test_payload_industry_takes_precedence(self):
        lead = {"payload": {"industry": "Med Spa"}, "industry": "Other"}
        self.assertEqual(prune._industry_of(lead), "Med Spa")

    def test_falls_back_to_top_level(self):
        lead = {"industry": "Dentist"}
        self.assertEqual(prune._industry_of(lead), "Dentist")

    def test_unknown_when_missing(self):
        self.assertEqual(prune._industry_of({}), "(unknown)")
        self.assertEqual(prune._industry_of({"payload": {}}), "(unknown)")


class PreviewTests(unittest.TestCase):
    def test_groups_candidates_by_industry(self):
        fake_candidates = [
            {"id": _uuid(1), "email": "a@x.com", "payload": {"industry": "Med Spa"}},
            {"id": _uuid(2), "email": "b@x.com", "payload": {"industry": "Med Spa"}},
            {"id": _uuid(3), "email": "c@x.com", "payload": {"industry": "Dentist"}},
            {"id": _uuid(4), "email": "d@x.com", "payload": {}},  # unknown
        ]
        with patch.object(prune, "list_contacted_unreplied_leads", return_value=fake_candidates):
            preview = prune.preview_contacted_unreplied(api_key="k")
        self.assertEqual(preview["total"], 4)
        # by_industry is sorted desc by count
        counts = dict(preview["by_industry"])
        self.assertEqual(counts["Med Spa"], 2)
        self.assertEqual(counts["Dentist"], 1)
        self.assertEqual(counts["(unknown)"], 1)

    def test_empty_when_no_candidates(self):
        with patch.object(prune, "list_contacted_unreplied_leads", return_value=[]):
            preview = prune.preview_contacted_unreplied(api_key="k")
        self.assertEqual(preview["total"], 0)
        self.assertEqual(preview["by_industry"], [])


class ExecuteTests(unittest.TestCase):
    def test_deletes_and_soft_deletes_each_candidate(self):
        backend = _SoftDeleteBackend()
        candidates = [
            {"id": _uuid(10), "email": "x@y.com", "payload": {"industry": "Dentist"}},
            {"id": _uuid(11), "email": "z@y.com", "payload": {"industry": "Med Spa"}},
        ]
        with patch.object(prune, "delete_lead_from_instantly", return_value=(True, None)) as m_del:
            result = prune.execute_prune(
                backend, api_key="k", candidates=candidates, max_workers=1,
            )
        self.assertEqual(m_del.call_count, 2)
        self.assertEqual(result["deleted_instantly"], 2)
        self.assertEqual(result["soft_deleted_raw"], 2)
        self.assertEqual(result["failed"], 0)
        # Soft-delete carries the right reason + flag.
        self.assertEqual(len(backend.calls), 2)
        first = backend.calls[0]
        self.assertEqual(first["fields"]["excluded_reason"], "contacted_no_reply")
        self.assertIn("excluded_at", first["fields"])

    def test_404_on_instantly_treated_as_already_deleted(self):
        backend = _SoftDeleteBackend()
        candidates = [{"id": _uuid(20), "email": "a@b.com"}]
        with patch.object(prune, "delete_lead_from_instantly", return_value=(False, "not found")):
            result = prune.execute_prune(
                backend, api_key="k", candidates=candidates, max_workers=1,
            )
        # Treating "not found" as already-deleted means we still soft-delete raw
        # and don't count as failed.
        self.assertEqual(result["deleted_instantly"], 1)
        self.assertEqual(result["soft_deleted_raw"], 1)
        self.assertEqual(result["failed"], 0)

    def test_instantly_failure_aborts_soft_delete(self):
        # If Instantly returns a real error (not 404), don't soft-delete raw —
        # leaves the lead in a state where the operator can retry.
        backend = _SoftDeleteBackend()
        candidates = [{"id": _uuid(30), "email": "a@b.com"}]
        with patch.object(prune, "delete_lead_from_instantly", return_value=(False, "rate limited 429")):
            result = prune.execute_prune(
                backend, api_key="k", candidates=candidates, max_workers=1,
            )
        self.assertEqual(result["deleted_instantly"], 0)
        self.assertEqual(result["soft_deleted_raw"], 0)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(backend.calls, [])

    def test_raw_miss_does_not_block_instantly_delete(self):
        # Lead exists in Instantly but the matching raw row was already
        # excluded (or never existed). Still count Instantly delete success;
        # surface the raw miss in the failure detail.
        backend = _SoftDeleteBackend(miss_ids={_uuid(40)})
        candidates = [{"id": _uuid(40), "email": "a@b.com"}]
        with patch.object(prune, "delete_lead_from_instantly", return_value=(True, None)):
            result = prune.execute_prune(
                backend, api_key="k", candidates=candidates, max_workers=1,
            )
        self.assertEqual(result["deleted_instantly"], 1)
        self.assertEqual(result["soft_deleted_raw"], 0)


if __name__ == "__main__":
    unittest.main()
