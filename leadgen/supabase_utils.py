"""
Supabase backend for the Lead Generation Engine (Step 3.3).

Direct Postgres connection to raw.scraped_leads + raw.import_batches
via a restricted `scraper_app` role (raw.* only — no access to
public/crm/events schemas).

Uses psycopg2 via Supavisor session-mode pooler.
Implements the DataBackend protocol from backend.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import streamlit as st

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
SKIP_ON_INSERT = {"id", "createdTime", "last_modified_at", "created_at", "updated_at"}

# All valid columns in raw.scraped_leads (for filtering writes)
VALID_SB_COLUMNS = {
    "source_tool", "import_batch_id", "company_name", "industry", "website",
    "city", "state", "postal_code", "postal_address", "phone", "rating",
    "contact_name", "contact_email", "contact_position",
    "email_verified", "verification_status", "verified_at",
    "competitor1", "competitor2", "competitor3",
    "instantly_lead_id", "instantly_campaign_id", "instantly_status",
    "instantly_synced_at",
}

# Columns for INSERT (subset of VALID_SB_COLUMNS, fixed order for execute_values)
INSERT_COLUMNS = [
    "source_tool", "import_batch_id", "company_name", "industry", "website",
    "city", "state", "postal_code", "postal_address", "phone", "rating",
    "contact_name", "contact_email", "contact_position",
    "competitor1", "competitor2", "competitor3",
]

# Columns for dedup reads (minimal)
DEDUP_QUERY = "SELECT website, phone FROM raw.scraped_leads WHERE website IS NOT NULL OR phone IS NOT NULL"

# Columns for sync manager reads (21 of 26 — excludes source_tool, import_batch_id,
# email_verified, verified_at, rating, created_at)
SYNC_QUERY = """
SELECT id, company_name, industry, website, city, state, postal_code,
       postal_address, phone, contact_name, contact_email, contact_position,
       instantly_lead_id, instantly_campaign_id, instantly_status,
       instantly_synced_at, updated_at, verification_status,
       competitor1, competitor2, competitor3
FROM raw.scraped_leads
"""

# Hardcoded industry list (from current Airtable dropdown — no metadata API in Postgres)
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


# ── Connection management ─────────────────────────────────────────────────

def connect_db(db_url: str) -> psycopg2.extensions.connection:
    """Open a Postgres connection with statement timeout."""
    conn = psycopg2.connect(
        db_url,
        options="-c statement_timeout=30000",  # 30s per query
    )
    conn.autocommit = False
    return conn


# ── Helpers ───────────────────────────────────────────────────────────────

def _map_record_to_sb(record: dict) -> dict:
    """Map app-internal field names to Supabase column names. Skip unknown fields."""
    mapped = {}
    for key, value in record.items():
        if key in SKIP_ON_INSERT:
            continue
        sb_key = APP_TO_SB.get(key, key)
        if sb_key in VALID_SB_COLUMNS and value is not None:
            mapped[sb_key] = value
    return mapped


def _map_record_to_app(record: dict) -> dict:
    """Map Supabase column names back to app-internal field names."""
    mapped = {}
    for key, value in record.items():
        app_key = SB_TO_APP.get(key, key)
        mapped[app_key] = value
    return mapped


def _row_to_insert_tuple(record: dict, source_tool: str, batch_id: str) -> tuple:
    """Convert a mapped record to a tuple matching INSERT_COLUMNS order."""
    mapped = _map_record_to_sb(record)
    mapped["source_tool"] = source_tool
    mapped["import_batch_id"] = batch_id
    return tuple(mapped.get(col) for col in INSERT_COLUMNS)


# ── Read: dedup (2 columns only) ─────────────────────────────────────────

def fetch_existing_leads_sb(conn: psycopg2.extensions.connection) -> tuple[set, set]:
    """Fetch website + phone for dedup."""
    try:
        with conn.cursor() as cur:
            cur.execute(DEDUP_QUERY)
            websites: set[str] = set()
            phones: set[str] = set()
            for row in cur:
                web, phone = row
                if web:
                    websites.add(str(web).strip().lower())
                if phone:
                    p = "".join(filter(str.isdigit, str(phone)))
                    if p:
                        phones.add(p)
            return websites, phones
    except Exception as e:
        st.error(f"Error fetching existing leads from Supabase: {e}")
        conn.rollback()
        return set(), set()


# ── Read: all leads for sync manager (21 columns) ────────────────────────

def fetch_all_leads_sb(conn: psycopg2.extensions.connection) -> list[dict]:
    """Fetch leads for the sync manager. Returns list of dicts with app-internal field names."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SYNC_QUERY)
            rows = cur.fetchall()
            result = []
            for row in rows:
                mapped = _map_record_to_app(dict(row))
                # Rename updated_at → last_modified_at for sync manager compatibility
                if "updated_at" in mapped:
                    mapped["last_modified_at"] = mapped.pop("updated_at")
                # Convert datetime objects to ISO strings for pandas compatibility
                for key in ("last_modified_at", "last_synced_at"):
                    val = mapped.get(key)
                    if val and hasattr(val, "isoformat"):
                        mapped[key] = val.isoformat()
                result.append(mapped)
            return result
    except Exception as e:
        st.error(f"Error fetching leads from Supabase: {e}")
        conn.rollback()
        return []


# ── Write: batch create with import_batches tracking ─────────────────────

def batch_create_leads_sb(
    conn: psycopg2.extensions.connection,
    records: list[dict],
    source_tool: str,
    industry: str,
    city: str,
) -> str | None:
    """
    Create leads in raw.scraped_leads with import_batches tracking.
    Commits per chunk (500 rows). Not one giant transaction.
    Returns batch_id on success.
    """
    if not records:
        return None

    batch_id = None
    chunk_size = 500

    try:
        # 1. Create import_batches row (committed immediately)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO raw.import_batches (source, industry, city, total_scraped, status)
                   VALUES (%s, %s, %s, %s, 'running') RETURNING id""",
                (source_tool, industry or None, city or None, len(records)),
            )
            batch_id = str(cur.fetchone()[0])
        conn.commit()

        # 2. Insert leads in chunks via execute_values
        total_inserted = 0
        insert_sql = f"""
            INSERT INTO raw.scraped_leads ({', '.join(INSERT_COLUMNS)})
            VALUES %s
        """

        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            values = [_row_to_insert_tuple(r, source_tool, batch_id) for r in chunk]
            values = sanitize_for_json(values)  # clean NaN/Infinity

            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, insert_sql, values,
                    template=None,
                    page_size=len(values),
                )
            conn.commit()
            total_inserted += len(chunk)

        # 3. Mark batch completed
        now_iso = datetime.now(timezone.utc).isoformat()
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE raw.import_batches
                   SET status = 'completed', new_added = %s, completed_at = %s
                   WHERE id = %s::uuid""",
                (total_inserted, now_iso, batch_id),
            )
        conn.commit()

        return batch_id

    except Exception as e:
        conn.rollback()
        # Mark batch failed
        if batch_id:
            try:
                now_iso = datetime.now(timezone.utc).isoformat()
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE raw.import_batches
                           SET status = 'failed', error_message = %s, completed_at = %s
                           WHERE id = %s::uuid""",
                        (str(e)[:500], now_iso, batch_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
        st.error(f"Supabase batch create failed: {e}")
        return None


# ── Update: after Instantly sync ──────────────────────────────────────────

def batch_update_leads_sb(conn: psycopg2.extensions.connection, updates: list[dict]) -> bool:
    """
    Update leads in raw.scraped_leads.
    updates: list of {'id': uuid_str, 'fields': {field: value}}.
    Maps field names.

    updated_at handling:
      - If the update includes instantly_synced_at (last_synced_at), we set
        updated_at := instantly_synced_at so the "pending" filter
        (updated_at > instantly_synced_at) does not immediately re-trigger.
      - Otherwise, updated_at := NOW().

    Previously this always did updated_at=NOW(), which caused sync writes to
    leave updated_at strictly after the captured instantly_synced_at value
    (captured once at the start of sync_pending_leads). Result: every synced
    row immediately reappeared in the pending list → infinite re-push loop.
    See: 2026-04-11 incident, 4,228-row accidental re-push.
    """
    if not updates:
        return True

    try:
        with conn.cursor() as cur:
            for update in updates:
                row_id = update.get("id")
                fields = update.get("fields", {})
                if not row_id or not fields:
                    continue

                # Map field names + build SET clause
                set_parts = []
                params = []
                sync_ts_value = None
                for key, value in fields.items():
                    sb_key = APP_TO_SB.get(key, key)
                    if sb_key in VALID_SB_COLUMNS:
                        set_parts.append(f"{sb_key} = %s")
                        params.append(value)
                        if sb_key == "instantly_synced_at":
                            sync_ts_value = value

                if not set_parts:
                    continue

                # Pin updated_at to instantly_synced_at when we're writing one,
                # so the "pending" filter does not immediately re-trigger.
                if sync_ts_value is not None:
                    set_parts.append("updated_at = %s")
                    params.append(sync_ts_value)
                else:
                    set_parts.append("updated_at = NOW()")
                params.append(row_id)

                cur.execute(
                    f"UPDATE raw.scraped_leads SET {', '.join(set_parts)} WHERE id = %s::uuid",
                    params,
                )

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Supabase batch update failed: {e}")
        return False


# ── SupabaseBackend class (implements DataBackend protocol) ───────────────

class SupabaseBackend:
    """Direct Postgres backend using restricted scraper_app role."""

    def __init__(self, secrets: dict):
        db_url = secrets.get("supabase_db_url", "")
        if not db_url:
            raise ValueError("SUPABASE_DB_URL is required for Supabase mode")
        self.conn = connect_db(db_url)

    def fetch_existing_leads(self) -> tuple[set, set]:
        return fetch_existing_leads_sb(self.conn)

    def fetch_all_leads(self) -> list[dict]:
        return fetch_all_leads_sb(self.conn)

    def batch_create(
        self,
        records: list[dict],
        source_tool: str,
        industry: str,
        city: str,
    ) -> str | None:
        return batch_create_leads_sb(self.conn, records, source_tool, industry, city)

    def batch_update(self, updates: list[dict]) -> bool:
        return batch_update_leads_sb(self.conn, updates)

    def log_transaction(self, **kwargs) -> None:
        # No-op: import_batches row (created by batch_create) IS the log
        pass

    def get_industry_options(self) -> list[str]:
        return INDUSTRY_OPTIONS

    def get_writable_field_names(self, table_id: str) -> set[str]:
        # All mapped fields are writable (no computed fields in Postgres)
        return VALID_SB_COLUMNS

    def filter_fields(self, record: dict) -> dict:
        return _map_record_to_sb(record)

    def __del__(self):
        try:
            if hasattr(self, "conn") and self.conn and not self.conn.closed:
                self.conn.close()
        except Exception:
            pass
