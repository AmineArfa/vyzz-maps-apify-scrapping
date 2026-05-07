"""ticket_tier is computed on INSERT only — UPDATE writes never re-derive it.

We exercise the pure helpers directly rather than spinning up Postgres:

- `_row_to_insert_tuple` is the canonical INSERT row builder.
- `batch_update_leads_sb` is the canonical UPDATE driver. We pass a fake
  cursor and assert what SQL it would have run.
"""
from __future__ import annotations

import sys
import types
import unittest


# Stub streamlit (st.error is the only call site exercised here).
if "streamlit" not in sys.modules:
    stub = types.ModuleType("streamlit")
    stub.error = lambda *a, **k: None
    stub.write = lambda *a, **k: None
    sys.modules["streamlit"] = stub


# Stub psycopg2 just enough so the import succeeds. We never open a real
# connection in these tests.
if "psycopg2" not in sys.modules:
    psy = types.ModuleType("psycopg2")
    extras = types.ModuleType("psycopg2.extras")
    extensions = types.ModuleType("psycopg2.extensions")

    class _Json:
        def __init__(self, obj):
            self.obj = obj

    extras.Json = _Json
    extras.RealDictCursor = object
    extras.execute_values = lambda *a, **k: None
    extensions.connection = object
    psy.extras = extras
    psy.extensions = extensions
    psy.connect = lambda *a, **k: None
    sys.modules["psycopg2"] = psy
    sys.modules["psycopg2.extras"] = extras
    sys.modules["psycopg2.extensions"] = extensions


from leadgen.supabase_utils import (  # noqa: E402
    COMPUTED_ON_INSERT_ONLY,
    INSERT_COLUMNS,
    _row_to_insert_tuple,
    batch_update_leads_sb,
)


class _FakeCursor:
    def __init__(self):
        self.executed: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, list(params) if params else []))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self):
        self.cursor_obj = _FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _idx(col: str) -> int:
    return INSERT_COLUMNS.index(col)


class InsertTierTests(unittest.TestCase):
    def test_tier_computed_from_industry_on_insert(self):
        row = _row_to_insert_tuple(
            {"company_name": "Acme", "industry": "Med Spa"},
            source_tool="csv_import",
            batch_id="b-1",
        )
        self.assertEqual(row[_idx("industry")], "Med Spa")
        self.assertEqual(row[_idx("ticket_tier")], "low")

    def test_tier_defaults_high_for_unmapped_industry(self):
        row = _row_to_insert_tuple(
            {"company_name": "Acme", "industry": "Business Consulting"},
            source_tool="csv_import",
            batch_id="b-1",
        )
        self.assertEqual(row[_idx("ticket_tier")], "high")

    def test_explicit_tier_wins_over_default(self):
        # Operator-supplied tier (e.g. luxury restaurant tagged 'high')
        # must not be overwritten by the industry default.
        row = _row_to_insert_tuple(
            {
                "company_name": "Le Bernardin",
                "industry": "Restaurants and Bars",  # default would be 'low'
                "ticket_tier": "high",
            },
            source_tool="csv_import",
            batch_id="b-1",
        )
        self.assertEqual(row[_idx("ticket_tier")], "high")

    def test_null_industry_yields_null_tier(self):
        row = _row_to_insert_tuple(
            {"company_name": "Acme"},
            source_tool="csv_import",
            batch_id="b-1",
        )
        self.assertIsNone(row[_idx("ticket_tier")])


class UpdateTierGuardTests(unittest.TestCase):
    def test_ticket_tier_is_in_computed_on_insert_only(self):
        self.assertIn("ticket_tier", COMPUTED_ON_INSERT_ONLY)

    def test_batch_update_drops_ticket_tier(self):
        # ticket_tier in the update fields must NOT make it into SET clause.
        # The sync flow occasionally pushes the entire lead dict back; we
        # don't want it to silently re-derive a tier the operator set.
        conn = _FakeConn()
        ok = batch_update_leads_sb(conn, [{
            "id": "00000000-0000-0000-0000-000000000001",
            "fields": {
                "industry": "Restaurants and Bars",
                "ticket_tier": "low",
                "instantly_status": "Success",
            },
        }])
        self.assertTrue(ok)
        self.assertEqual(len(conn.cursor_obj.executed), 1)
        sql, params = conn.cursor_obj.executed[0]
        self.assertIn("UPDATE raw.scraped_leads SET", sql)
        self.assertNotIn("ticket_tier", sql)
        self.assertNotIn("low", params)
        # Industry and status do go through.
        self.assertIn("industry = %s", sql)
        self.assertIn("instantly_status = %s", sql)

    def test_batch_update_writes_industry_alone(self):
        # Updating industry alone must not back-fill a new tier.
        conn = _FakeConn()
        batch_update_leads_sb(conn, [{
            "id": "00000000-0000-0000-0000-000000000001",
            "fields": {"industry": "Med Spa"},
        }])
        sql, _ = conn.cursor_obj.executed[0]
        self.assertNotIn("ticket_tier", sql)


if __name__ == "__main__":
    unittest.main()
