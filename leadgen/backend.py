"""
Backend adapter for the Lead Generation Engine (Step 3.3).

Defines the DataBackend protocol and the AirtableBackend implementation.
SupabaseBackend is in supabase_utils.py.

The rest of the app (runner.py, sync_manager.py) calls backend.xxx()
with no if/else branching — they don't know which backend is active.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd
import streamlit as st

from .airtable_utils import (
    batch_update_leads as _at_batch_update,
    fetch_all_leads as _at_fetch_all,
    fetch_existing_leads as _at_fetch_existing,
    filter_airtable_fields,
    get_airtable_writable_field_names,
    get_industry_options as _at_get_industries,
    init_airtable,
    log_transaction as _at_log_transaction,
)
from .json_sanitize import sanitize_for_json


@runtime_checkable
class DataBackend(Protocol):
    """Interface that both AirtableBackend and SupabaseBackend implement."""

    def fetch_existing_leads(self) -> tuple[set, set]:
        """Return (set of websites, set of phones) for dedup."""
        ...

    def fetch_all_leads(self) -> list[dict]:
        """Return all leads as a list of dicts (with 'id' key)."""
        ...

    def batch_create(
        self,
        records: list[dict],
        source_tool: str,
        industry: str,
        city: str,
    ) -> str | None:
        """
        Create leads. For Supabase, also creates import_batches row.
        Returns batch_id (Supabase) or None (Airtable).
        """
        ...

    def batch_update(self, updates: list[dict]) -> bool:
        """
        Update leads. updates: list of {'id': '...', 'fields': {...}}.
        Returns True on success.
        """
        ...

    def log_transaction(self, **kwargs) -> None:
        """Log a scrape session. No-op for Supabase (import_batches handles it)."""
        ...

    def get_industry_options(self) -> list[str]:
        """Return available industry choices."""
        ...

    def get_writable_field_names(self, table_id: str) -> set[str]:
        """Return set of writable field names (Airtable) or all fields (Supabase)."""
        ...

    def filter_fields(self, record: dict) -> dict:
        """Filter a record to only include writable fields."""
        ...


class AirtableBackend:
    """Wraps existing airtable_utils functions behind the DataBackend interface."""

    def __init__(self, secrets: dict, leads_table: str, log_table: str):
        self.secrets = secrets
        self.leads_table_id = leads_table
        self.table_leads, self.table_log = init_airtable(
            secrets["airtable_key"],
            secrets["airtable_base"],
            leads_table=leads_table,
            log_table=log_table,
        )
        self._writable_fields: set[str] | None = None

    def fetch_existing_leads(self) -> tuple[set, set]:
        return _at_fetch_existing(self.table_leads)

    def fetch_all_leads(self) -> list[dict]:
        return _at_fetch_all(self.table_leads)

    def batch_create(
        self,
        records: list[dict],
        source_tool: str,
        industry: str,
        city: str,
    ) -> str | None:
        if not records:
            return None
        try:
            self.table_leads.batch_create(records, typecast=True)
        except Exception as e:
            # Retry logic for computed fields (ported from runner.py)
            err_str = str(e)
            dropped_field = None
            if "INVALID_VALUE_FOR_COLUMN" in err_str and 'Field "' in err_str:
                try:
                    dropped_field = err_str.split('Field "', 1)[1].split('"', 1)[0]
                except Exception:
                    dropped_field = None

            if dropped_field:
                cleaned = [
                    {k: v for k, v in r.items() if k != dropped_field}
                    for r in records
                ]
                self.table_leads.batch_create(cleaned, typecast=True)
            else:
                raise
        return None

    def batch_update(self, updates: list[dict]) -> bool:
        return _at_batch_update(self.table_leads, updates)

    def log_transaction(self, **kwargs) -> None:
        _at_log_transaction(
            self.table_log,
            kwargs.get("industry", ""),
            kwargs.get("city_input", ""),
            kwargs.get("total_scraped", 0),
            kwargs.get("new_added", 0),
            kwargs.get("enrich_used", False),
            kwargs.get("status", "Unknown"),
            error_msg=kwargs.get("error_msg", ""),
            credit_used_apify=kwargs.get("credit_used_apify"),
            credit_used_apollo=kwargs.get("credit_used_apollo"),
            credit_used_instantly=kwargs.get("credit_used_instantly"),
            instantly_added=kwargs.get("instantly_added"),
            search_query=kwargs.get("search_query"),
        )

    def get_industry_options(self) -> list[str]:
        return _at_get_industries(
            self.secrets["airtable_key"],
            self.secrets["airtable_base"],
            self.leads_table_id,
        )

    def get_writable_field_names(self, table_id: str) -> set[str]:
        if self._writable_fields is None:
            self._writable_fields = get_airtable_writable_field_names(
                self.secrets["airtable_key"],
                self.secrets["airtable_base"],
                table_id,
            )
        return self._writable_fields

    def filter_fields(self, record: dict) -> dict:
        allowed = self.get_writable_field_names(self.leads_table_id)
        return filter_airtable_fields(record, allowed)
