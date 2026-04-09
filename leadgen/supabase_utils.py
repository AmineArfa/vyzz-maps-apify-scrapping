"""
Supabase backend for the Lead Generation Engine (Step 3.3).

Direct connection to raw.scraped_leads + raw.import_batches via supabase-py.
Implements the DataBackend protocol from backend.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st
from supabase import create_client, Client

from .json_sanitize import sanitize_for_json

# ── Field name mapping (app-internal → Supabase column) ──────────────────

APP_TO_SB: dict[str, str] = {
    "generic_phone": "phone",
    "scrapping_tool": "source_tool",
    "key_contact_name": "contact_name",
    "key_contact_email": "contact_email",
    "key_contact_position": "contact_position",
    "instantly_statuts": "instantly_status",
    "last_synced_at": "instantly_synced_at",
}

SB_TO_APP: dict[str, str] = {v: k for k, v in APP_TO_SB.items()}

# Fields that exist in the app but not in raw.scraped_leads — skip on insert
SKIP_ON_INSERT = {"id", "createdTime", "last_modified_at"}

# All valid columns in raw.scraped_leads (for filtering)
VALID_SB_COLUMNS = {
    "source_tool", "import_batch_id", "company_name", "industry", "website",
    "city", "state", "postal_code", "postal_address", "phone", "rating",
    "contact_name", "contact_email", "contact_position",
    "email_verified", "verification_status", "verified_at",
    "competitor1", "competitor2", "competitor3",
    "instantly_lead_id", "instantly_campaign_id", "instantly_status",
    "instantly_synced_at",
}

# Explicit column lists for reads
DEDUP_COLUMNS = "website, phone"
SYNC_COLUMNS = (
    "id, company_name, industry, website, city, state, postal_code, "
    "postal_address, phone, contact_name, contact_email, contact_position, "
    "instantly_lead_id, instantly_campaign_id, instantly_status, "
    "instantly_synced_at, updated_at, verification_status, "
    "competitor1, competitor2, competitor3"
)

# Hardcoded industry list (from current Airtable dropdown — no metadata API in Supabase)
INDUSTRY_OPTIONS = [
    "Accounting", "Architecture", "Auto Repair", "Bakery", "Beauty Salon",
    "Brewery", "Car Dealership", "Catering", "Chiropractic", "Cleaning",
    "Construction", "Consulting", "Dental", "Education", "Electrical",
    "Engineering", "Financial Planning", "Fitness", "Florist", "HVAC",
    "Healthcare", "Home Inspection", "Insurance", "Interior Design",
    "Landscaping", "Law", "Locksmith", "Marketing", "Massage",
    "Medical Spa", "Moving", "Optometry", "Orthodontics", "Painting",
    "Pest Control", "Pet Care", "Photography", "Plumbing", "Real Estate",
    "Restaurant", "Roofing", "Software", "Solar", "Spa", "Staffing",
    "Tattoo", "Therapy", "Towing", "Veterinary", "Wedding Planning",
    "Other",
]


# ── Init ──────────────────────────────────────────────────────────────────

def init_supabase(secrets: dict) -> Client:
    """Initialize Supabase client with service key."""
    url = secrets.get("supabase_url", "")
    key = secrets.get("supabase_service_key", "")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY are required for Supabase mode")
    return create_client(url, key)


# ── Helpers ───────────────────────────────────────────────────────────────

def _map_record_to_sb(record: dict) -> dict:
    """Map app-internal field names to Supabase column names. Skip unknown fields."""
    mapped = {}
    for key, value in record.items():
        if key in SKIP_ON_INSERT:
            continue
        sb_key = APP_TO_SB.get(key, key)
        if sb_key in VALID_SB_COLUMNS:
            if value is not None:
                mapped[sb_key] = value
    return mapped


def _map_record_to_app(record: dict) -> dict:
    """Map Supabase column names back to app-internal field names."""
    mapped = {}
    for key, value in record.items():
        app_key = SB_TO_APP.get(key, key)
        mapped[app_key] = value
    return mapped


# ── Read: dedup (2 columns only) ─────────────────────────────────────────

def fetch_existing_leads_sb(client: Client) -> tuple[set, set]:
    """Fetch website + phone for dedup. Returns (set of websites, set of phones)."""
    try:
        # Paginate through all rows (Supabase default limit is 1000)
        websites: set[str] = set()
        phones: set[str] = set()
        offset = 0
        page_size = 1000

        while True:
            resp = (
                client.schema("raw").table("scraped_leads")
                .select(DEDUP_COLUMNS)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = resp.data or []
            for row in rows:
                web = row.get("website")
                phone = row.get("phone")
                if web:
                    websites.add(str(web).strip().lower())
                if phone:
                    p = "".join(filter(str.isdigit, str(phone)))
                    if p:
                        phones.add(p)
            if len(rows) < page_size:
                break
            offset += page_size

        return websites, phones
    except Exception as e:
        st.error(f"Error fetching existing leads from Supabase: {e}")
        return set(), set()


# ── Read: all leads for sync manager (21 columns) ────────────────────────

def fetch_all_leads_sb(client: Client) -> list[dict]:
    """Fetch leads for the sync manager. Returns list of dicts with app-internal field names."""
    try:
        all_rows: list[dict] = []
        offset = 0
        page_size = 1000

        while True:
            resp = (
                client.schema("raw").table("scraped_leads")
                .select(SYNC_COLUMNS)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = resp.data or []
            for row in rows:
                mapped = _map_record_to_app(row)
                # Rename 'updated_at' → 'last_modified_at' for sync manager compatibility
                if "updated_at" in mapped:
                    mapped["last_modified_at"] = mapped.pop("updated_at")
                # Keep 'id' as-is (UUID string, not Airtable recXXX)
                all_rows.append(mapped)
            if len(rows) < page_size:
                break
            offset += page_size

        return all_rows
    except Exception as e:
        st.error(f"Error fetching leads from Supabase: {e}")
        return []


# ── Write: batch create with import_batches tracking ─────────────────────

def batch_create_leads_sb(
    client: Client,
    records: list[dict],
    source_tool: str,
    industry: str,
    city: str,
) -> str | None:
    """
    Create leads in raw.scraped_leads with import_batches tracking.
    Owns the full lifecycle of one import_batches row.
    Returns the batch_id on success, None on failure.
    """
    if not records:
        return None

    batch_id = None
    try:
        # 1. Create import_batches row
        batch_resp = (
            client.schema("raw").table("import_batches")
            .insert({
                "source": source_tool,
                "industry": industry or None,
                "city": city or None,
                "total_scraped": len(records),
                "status": "running",
            })
            .execute()
        )
        batch_id = batch_resp.data[0]["id"] if batch_resp.data else None
        if not batch_id:
            raise ValueError("Failed to create import batch")

        # 2. Map and insert leads in chunks of 100
        chunk_size = 100
        total_inserted = 0

        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            sb_rows = []
            for record in chunk:
                mapped = _map_record_to_sb(record)
                mapped["source_tool"] = source_tool
                mapped["import_batch_id"] = batch_id
                sb_rows.append(sanitize_for_json(mapped))

            client.schema("raw").table("scraped_leads").insert(sb_rows).execute()
            total_inserted += len(sb_rows)

        # 3. Mark batch completed
        now_iso = datetime.now(timezone.utc).isoformat()
        (
            client.schema("raw").table("import_batches")
            .update({
                "status": "completed",
                "new_added": total_inserted,
                "completed_at": now_iso,
            })
            .eq("id", batch_id)
            .execute()
        )

        return batch_id

    except Exception as e:
        # Mark batch failed (batch row persists, leads may be partial)
        if batch_id:
            try:
                now_iso = datetime.now(timezone.utc).isoformat()
                (
                    client.schema("raw").table("import_batches")
                    .update({
                        "status": "failed",
                        "error_message": str(e)[:500],
                        "completed_at": now_iso,
                    })
                    .eq("id", batch_id)
                    .execute()
                )
            except Exception:
                pass
        st.error(f"Supabase batch create failed: {e}")
        return None


# ── Update: after Instantly sync ──────────────────────────────────────────

def batch_update_leads_sb(client: Client, updates: list[dict]) -> bool:
    """
    Update leads in raw.scraped_leads.
    updates: list of {'id': uuid_str, 'fields': {field: value}}.
    Maps field names and explicitly sets updated_at=NOW().
    """
    if not updates:
        return True

    try:
        for update in updates:
            row_id = update.get("id")
            fields = update.get("fields", {})
            if not row_id or not fields:
                continue

            # Map field names to Supabase columns
            sb_fields: dict[str, Any] = {}
            for key, value in fields.items():
                sb_key = APP_TO_SB.get(key, key)
                if sb_key in VALID_SB_COLUMNS:
                    sb_fields[sb_key] = value

            # Always set updated_at to track modification time
            sb_fields["updated_at"] = datetime.now(timezone.utc).isoformat()

            (
                client.schema("raw").table("scraped_leads")
                .update(sanitize_for_json(sb_fields))
                .eq("id", row_id)
                .execute()
            )

        return True
    except Exception as e:
        st.error(f"Supabase batch update failed: {e}")
        return False


# ── SupabaseBackend class (implements DataBackend protocol) ───────────────

class SupabaseBackend:
    """Direct Supabase backend. Wraps supabase_utils functions."""

    def __init__(self, secrets: dict):
        self.client = init_supabase(secrets)
        self.secrets = secrets

    def fetch_existing_leads(self) -> tuple[set, set]:
        return fetch_existing_leads_sb(self.client)

    def fetch_all_leads(self) -> list[dict]:
        return fetch_all_leads_sb(self.client)

    def batch_create(
        self,
        records: list[dict],
        source_tool: str,
        industry: str,
        city: str,
    ) -> str | None:
        return batch_create_leads_sb(self.client, records, source_tool, industry, city)

    def batch_update(self, updates: list[dict]) -> bool:
        return batch_update_leads_sb(self.client, updates)

    def log_transaction(self, **kwargs) -> None:
        # No-op: import_batches row (created by batch_create) IS the log
        pass

    def get_industry_options(self) -> list[str]:
        return INDUSTRY_OPTIONS

    def get_writable_field_names(self, table_id: str) -> set[str]:
        # All mapped fields are writable in Supabase (no computed fields)
        return VALID_SB_COLUMNS

    def filter_fields(self, record: dict) -> dict:
        """Filter to valid Supabase fields, mapping names."""
        return _map_record_to_sb(record)
