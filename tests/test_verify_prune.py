"""Verify-prune flow: MillionVerifier → delete bad emails from Instantly + soft-delete raw."""
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


from leadgen import verify_prune  # noqa: E402


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


class PreviewTests(unittest.TestCase):
    def test_groups_all_leads_by_industry(self):
        fake = [
            {"id": _uuid(1), "email": "a@x.com", "payload": {"industry": "Med Spa"}},
            {"id": _uuid(2), "email": "b@x.com", "payload": {"industry": "Med Spa"}},
            {"id": _uuid(3), "email": "c@x.com", "payload": {"industry": "Dentist"}},
            {"id": _uuid(4), "email": "d@x.com", "payload": {}},
        ]
        with patch.object(verify_prune, "list_all_leads", return_value=fake):
            preview = verify_prune.preview_all_leads(api_key="k")
        self.assertEqual(preview["total"], 4)
        counts = dict(preview["by_industry"])
        self.assertEqual(counts["Med Spa"], 2)
        self.assertEqual(counts["Dentist"], 1)
        self.assertEqual(counts["(unknown)"], 1)

    def test_empty_account(self):
        with patch.object(verify_prune, "list_all_leads", return_value=[]):
            preview = verify_prune.preview_all_leads(api_key="k")
        self.assertEqual(preview["total"], 0)
        self.assertEqual(preview["by_industry"], [])


class ExecuteTests(unittest.TestCase):
    def _candidates(self):
        return [
            {"id": _uuid(10), "email": "ok@x.com",       "payload": {"industry": "Dentist"}},
            {"id": _uuid(11), "email": "invalid@x.com",  "payload": {"industry": "Med Spa"}},
            {"id": _uuid(12), "email": "disposable@x.com","payload": {"industry": "Med Spa"}},
            {"id": _uuid(13), "email": "unknown@x.com",  "payload": {"industry": "Dentist"}},
            {"id": _uuid(14), "email": "catch_all@x.com","payload": {"industry": "Dentist"}},
            {"id": _uuid(15), "email": "",               "payload": {"industry": "Other"}},
        ]

    def _fake_verify(self, _key, email, **kwargs):
        return {
            "ok@x.com": "ok",
            "invalid@x.com": "invalid",
            "disposable@x.com": "disposable",
            "unknown@x.com": "unknown",
            "catch_all@x.com": "catch_all",
        }.get(email, "unknown")

    def test_only_bad_statuses_deleted_by_default(self):
        backend = _SoftDeleteBackend()
        candidates = self._candidates()
        with patch.object(verify_prune, "verify_single_email", side_effect=self._fake_verify), \
             patch.object(verify_prune, "delete_lead_from_instantly", return_value=(True, None)) as m_del:
            result = verify_prune.execute_verify_prune(
                backend, api_key="k", mv_api_key="mv",
                candidates=candidates, max_workers=1,
            )

        # invalid + disposable → 2 deletes. unknown / ok / catch_all / no_email left alone.
        self.assertEqual(m_del.call_count, 2)
        self.assertEqual(result["deleted_instantly"], 2)
        self.assertEqual(result["soft_deleted_raw"], 2)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["good"], 2)        # ok, catch_all
        self.assertEqual(result["skipped"], 1)     # unknown
        self.assertEqual(result["bad"], 2)         # invalid, disposable
        self.assertEqual(result["no_email"], 1)
        self.assertEqual(result["verified"], 5)    # 6 candidates − 1 no_email

        # Soft-delete carries the right reason + flag.
        self.assertEqual(len(backend.calls), 2)
        reasons = sorted(c["fields"]["excluded_reason"] for c in backend.calls)
        self.assertEqual(reasons, ["bad_email:disposable", "bad_email:invalid"])
        for call in backend.calls:
            self.assertIn("excluded_at", call["fields"])

    def test_include_unknown_as_bad_opt_in(self):
        backend = _SoftDeleteBackend()
        candidates = self._candidates()
        with patch.object(verify_prune, "verify_single_email", side_effect=self._fake_verify), \
             patch.object(verify_prune, "delete_lead_from_instantly", return_value=(True, None)) as m_del:
            result = verify_prune.execute_verify_prune(
                backend, api_key="k", mv_api_key="mv",
                candidates=candidates, max_workers=1,
                include_unknown_as_bad=True,
            )

        # invalid + disposable + unknown → 3 deletes.
        self.assertEqual(m_del.call_count, 3)
        self.assertEqual(result["deleted_instantly"], 3)
        self.assertEqual(result["bad"], 3)
        self.assertEqual(result["skipped"], 0)

    def test_404_treated_as_already_deleted(self):
        backend = _SoftDeleteBackend()
        candidates = [{"id": _uuid(20), "email": "invalid@x.com"}]
        with patch.object(verify_prune, "verify_single_email", return_value="invalid"), \
             patch.object(verify_prune, "delete_lead_from_instantly", return_value=(False, "not found")):
            result = verify_prune.execute_verify_prune(
                backend, api_key="k", mv_api_key="mv",
                candidates=candidates, max_workers=1,
            )
        # "not found" → still soft-delete raw, not counted as failed.
        self.assertEqual(result["deleted_instantly"], 1)
        self.assertEqual(result["soft_deleted_raw"], 1)
        self.assertEqual(result["failed"], 0)

    def test_instantly_failure_blocks_soft_delete(self):
        backend = _SoftDeleteBackend()
        candidates = [{"id": _uuid(30), "email": "invalid@x.com"}]
        with patch.object(verify_prune, "verify_single_email", return_value="invalid"), \
             patch.object(verify_prune, "delete_lead_from_instantly", return_value=(False, "rate limited 429")):
            result = verify_prune.execute_verify_prune(
                backend, api_key="k", mv_api_key="mv",
                candidates=candidates, max_workers=1,
            )
        self.assertEqual(result["deleted_instantly"], 0)
        self.assertEqual(result["soft_deleted_raw"], 0)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(backend.calls, [])

    def test_raw_miss_does_not_block_instantly_delete(self):
        backend = _SoftDeleteBackend(miss_ids={_uuid(40)})
        candidates = [{"id": _uuid(40), "email": "invalid@x.com"}]
        with patch.object(verify_prune, "verify_single_email", return_value="invalid"), \
             patch.object(verify_prune, "delete_lead_from_instantly", return_value=(True, None)):
            result = verify_prune.execute_verify_prune(
                backend, api_key="k", mv_api_key="mv",
                candidates=candidates, max_workers=1,
            )
        self.assertEqual(result["deleted_instantly"], 1)
        self.assertEqual(result["soft_deleted_raw"], 0)
        # raw miss does not count as a hard failure since Instantly delete succeeded.
        self.assertEqual(result["failed"], 0)

    def test_missing_mv_key_raises(self):
        backend = _SoftDeleteBackend()
        with self.assertRaises(ValueError):
            verify_prune.execute_verify_prune(
                backend, api_key="k", mv_api_key="",
                candidates=[{"id": _uuid(50), "email": "a@b.com"}],
                max_workers=1,
            )

    def test_empty_candidates_short_circuits(self):
        backend = _SoftDeleteBackend()
        # Even without an mv_api_key, an empty list should never call MV.
        result = verify_prune.execute_verify_prune(
            backend, api_key="k", mv_api_key="",
            candidates=[], max_workers=1,
        )
        self.assertEqual(result["verified"], 0)
        self.assertEqual(result["bad"], 0)
        self.assertEqual(result["deleted_instantly"], 0)
        self.assertEqual(result["soft_deleted_raw"], 0)
        self.assertEqual(backend.calls, [])

    def test_mv_exception_falls_back_to_unknown(self):
        # Network blip → fail-safe: treated as unknown, NOT deleted by default.
        backend = _SoftDeleteBackend()
        candidates = [{"id": _uuid(60), "email": "a@b.com"}]

        def _boom(*a, **k):
            raise RuntimeError("network down")

        with patch.object(verify_prune, "verify_single_email", side_effect=_boom), \
             patch.object(verify_prune, "delete_lead_from_instantly", return_value=(True, None)) as m_del:
            result = verify_prune.execute_verify_prune(
                backend, api_key="k", mv_api_key="mv",
                candidates=candidates, max_workers=1,
            )
        self.assertEqual(m_del.call_count, 0)
        self.assertEqual(result["deleted_instantly"], 0)
        self.assertEqual(result["skipped"], 1)
        # The MV exception is captured in details for the operator log.
        self.assertTrue(any("MillionVerifier exception" in (d.get("error") or "")
                            for d in result["details"]))


if __name__ == "__main__":
    unittest.main()
