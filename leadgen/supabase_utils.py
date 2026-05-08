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

from .campaign_filter import build_where as build_filter_where
from .json_sanitize import sanitize_for_json
from .ticket_tier import compute_ticket_tier

# ── Field name mapping (app-internal → Supabase column) ──────────────────

APP_TO_SB: dict[str, str] = {
    "generic_phone": "phone",
    "scrapping_tool": "source_tool",
    "key_contact_name": "contact_name",
    "key_contact_email": "contact_email",
    "key_contact_position": "contact_position",
}

SB_TO_APP: dict[str, str] = {v: k for k, v in APP_TO_SB.items()}

# Fields that exist in the app but not in raw.scraped_leads — skip on insert
SKIP_ON_INSERT = {"id", "createdTime", "last_modified_at", "created_at", "updated_at"}

# All valid columns in raw.scraped_leads (for filtering writes)
VALID_SB_COLUMNS = {
    "source_tool", "import_batch_id", "company_name", "industry", "ticket_tier",
    "website",
    "city", "state", "postal_code", "postal_address", "phone", "rating",
    "contact_name", "contact_email", "contact_position",
    "email_verified", "verification_status", "verified_at",
    "competitor1", "competitor2", "competitor3",
    "instantly_lead_id", "instantly_campaign_id", "instantly_status",
    "instantly_synced_at",
    "excluded_at", "excluded_reason",
}

# Columns for INSERT (subset of VALID_SB_COLUMNS, fixed order for execute_values)
INSERT_COLUMNS = [
    "source_tool", "import_batch_id", "company_name", "industry", "ticket_tier",
    "website",
    "city", "state", "postal_code", "postal_address", "phone", "rating",
    "contact_name", "contact_email", "contact_position",
    "competitor1", "competitor2", "competitor3",
]

# ticket_tier is computed from industry on INSERT only. We never auto-recompute
# it on UPDATE — once a row has a tier, it is treated as operator-set (a
# luxury restaurant could be "high" even though Restaurants and Bars defaults
# to "low"). Update paths must NOT silently re-derive tier from industry.
COMPUTED_ON_INSERT_ONLY = {"ticket_tier"}

# Columns for dedup reads (minimal). Excluded (soft-deleted) rows still
# count for dedup so we don't re-import a lead the operator has already
# pruned — but every other read path filters them out.
DEDUP_QUERY = "SELECT website, phone FROM raw.scraped_leads WHERE website IS NOT NULL OR phone IS NOT NULL"

# Columns for sync manager reads (21 of 26 — excludes source_tool, import_batch_id,
# email_verified, verified_at, rating, created_at)
SYNC_QUERY = """
SELECT id, company_name, industry, ticket_tier, website, city, state,
       postal_code, postal_address, phone,
       contact_name, contact_email, contact_position,
       instantly_lead_id, instantly_campaign_id, instantly_status,
       instantly_synced_at, updated_at, verification_status,
       competitor1, competitor2, competitor3
FROM raw.scraped_leads
WHERE excluded_at IS NULL
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
    """Convert a mapped record to a tuple matching INSERT_COLUMNS order.

    Computes `ticket_tier` from `industry` on INSERT when the caller did not
    supply one. Treat any tier the caller passed in as authoritative — that
    way a future bulk-import flow that already classifies leads can override
    the default mapping.
    """
    mapped = _map_record_to_sb(record)
    mapped["source_tool"] = source_tool
    mapped["import_batch_id"] = batch_id
    if not mapped.get("ticket_tier"):
        mapped["ticket_tier"] = compute_ticket_tier(mapped.get("industry"))
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
                for key in ("last_modified_at", "instantly_synced_at"):
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
      - If the update includes instantly_synced_at, we set updated_at to
        the same value so the "pending" filter (updated_at > instantly_synced_at)
        does not immediately re-trigger.
      - Otherwise, updated_at := NOW().

    Previously this always did updated_at=NOW(), which caused sync writes to
    leave updated_at strictly after the captured instantly_synced_at value
    (captured once at the start of sync_pending_leads). Result: every synced
    row immediately reappeared in the pending list → infinite re-push loop.
    See: 2026-04-11 incident, 4,228-row accidental re-push.
    """
    if not updates:
        return True

    # Per-row savepoints so a single failure (e.g. unique-violation from
    # uniq_scraped_leads_email_when_pushed) doesn't roll back the whole
    # batch. Without this, one bad row cancelled all writebacks and we'd
    # lose dozens of just-pushed Instantly leads' state, recreating them
    # on the next sync run. See P2 in 2026-05-08 Instantly sync fixes.
    skipped_unique = 0
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
                    if sb_key in COMPUTED_ON_INSERT_ONLY:
                        # ticket_tier is operator-overridable. The sync /
                        # post-import write paths must not auto-recompute it
                        # from industry. Operators set it explicitly through
                        # a dedicated admin action (not through this generic
                        # batch_update).
                        continue
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

                cur.execute("SAVEPOINT row_update")
                try:
                    cur.execute(
                        f"UPDATE raw.scraped_leads SET {', '.join(set_parts)} WHERE id = %s::uuid",
                        params,
                    )
                    cur.execute("RELEASE SAVEPOINT row_update")
                except psycopg2.errors.UniqueViolation:
                    # The duplicate-email partial index fired. Means another
                    # row in raw is already linked to this email; the push
                    # path's pre-flight dedupe should have prevented this,
                    # but the index is the belt-and-braces. Skip this row
                    # and keep going so the rest of the batch persists.
                    cur.execute("ROLLBACK TO SAVEPOINT row_update")
                    skipped_unique += 1

        conn.commit()
        if skipped_unique:
            st.warning(
                f"batch_update: {skipped_unique} row(s) skipped due to "
                f"duplicate-email-db-constraint (uniq_scraped_leads_email_when_pushed)."
            )
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Supabase batch update failed: {e}")
        return False


# ── Campaign composer: filter, count, persist ────────────────────────────

# Columns returned for the sample preview / push-eligibility list.
_SAMPLE_COLUMNS = (
    "id", "company_name", "industry", "ticket_tier", "city", "state",
    "contact_email", "contact_name", "website", "phone",
    "instantly_lead_id", "instantly_campaign_id", "verification_status",
)


def fetch_unlinked_leads_with_email_sb(
    conn: psycopg2.extensions.connection,
    *,
    limit: int | None = None,
) -> list[dict]:
    """Rows with NULL `instantly_lead_id` but a real email — candidates for
    the reconciliation sweep. Some of these may already exist in Instantly
    from earlier runs whose writeback was lost.
    """
    sql = """
        SELECT id, contact_email, company_name, ticket_tier, industry
          FROM raw.scraped_leads
         WHERE instantly_lead_id IS NULL
           AND contact_email IS NOT NULL
           AND contact_email <> ''
           AND excluded_at IS NULL
         ORDER BY created_at DESC NULLS LAST
    """
    params: list = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(int(limit))
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [_map_record_to_app(dict(r)) for r in cur.fetchall()]
    except Exception as e:
        st.error(f"Error fetching unlinked leads: {e}")
        conn.rollback()
        return []


def find_pushed_siblings_by_email_sb(
    conn: psycopg2.extensions.connection,
    *,
    emails: list[str],
    exclude_campaign_id: str | None,
) -> dict:
    """For each email, find a raw row that's already pushed to Instantly in
    a *different* campaign than `exclude_campaign_id`. Used by the push
    pre-flight to reroute would-be CREATEs into MOVEs, preventing the
    duplicate-lead_id drift seen in the 2026-05-08 IT review.

    Returns: {lower(email) -> {"instantly_lead_id": str,
                                "instantly_campaign_id": str}}.
    """
    if not emails:
        return {}
    lowered = sorted({e.strip().lower() for e in emails if e})
    if not lowered:
        return {}
    sql = """
        SELECT LOWER(contact_email) AS email,
               instantly_lead_id,
               instantly_campaign_id
          FROM raw.scraped_leads
         WHERE LOWER(contact_email) = ANY(%s)
           AND instantly_lead_id IS NOT NULL
           AND instantly_status = 'Success'
           AND excluded_at IS NULL
    """
    params: list = [lowered]
    if exclude_campaign_id:
        sql += " AND (instantly_campaign_id IS DISTINCT FROM %s)"
        params.append(exclude_campaign_id)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            out: dict = {}
            for r in cur.fetchall():
                email = r["email"]
                if email and email not in out:
                    out[email] = {
                        "instantly_lead_id": r["instantly_lead_id"],
                        "instantly_campaign_id": r["instantly_campaign_id"],
                    }
            return out
    except Exception as e:
        st.error(f"find_pushed_siblings_by_email failed: {e}")
        conn.rollback()
        return {}


def clear_link_on_sibling_rows_sb(
    conn: psycopg2.extensions.connection,
    *,
    instantly_lead_id: str,
    keep_row_id: str,
) -> int:
    """NULL out instantly_* fields on every row sharing `instantly_lead_id`
    except `keep_row_id`. Called from `_writeback_one` so a successful push
    leaves no stale siblings claiming the same Instantly lead.

    The same Instantly lead can only live in one campaign at a time, so any
    other raw row holding that id is now wrong (Instantly moved the lead,
    our DB still pointed at the old campaign). NULLing the satellite
    columns lets the next sync run re-evaluate them cleanly.

    Returns the number of sibling rows cleared.
    """
    if not instantly_lead_id or not keep_row_id:
        return 0
    sql = """
        UPDATE raw.scraped_leads
           SET instantly_lead_id      = NULL,
               instantly_campaign_id  = NULL,
               instantly_status       = NULL,
               instantly_synced_at    = NULL,
               updated_at             = NOW()
         WHERE instantly_lead_id = %s
           AND id <> %s::uuid
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (instantly_lead_id, keep_row_id))
            cleared = cur.rowcount
        conn.commit()
        return int(cleared or 0)
    except Exception as e:
        conn.rollback()
        st.error(f"clear_link_on_sibling_rows failed: {e}")
        return 0


def soft_delete_by_instantly_id_or_email_sb(
    conn: psycopg2.extensions.connection,
    *,
    instantly_lead_id: str | None,
    email: str | None,
    fields: dict,
) -> bool:
    """Match a single raw row by instantly_lead_id (preferred) or email,
    apply `fields`. Returns True iff at least one row was updated.

    The prune flow gets Instantly's lead representation, not the raw uuid,
    so we match on the foreign keys we do have. If neither key is set we
    can't match — return False.
    """
    if not instantly_lead_id and not email:
        return False
    set_parts = []
    params: list = []
    for k, v in fields.items():
        sb_key = APP_TO_SB.get(k, k)
        if sb_key in VALID_SB_COLUMNS:
            set_parts.append(f"{sb_key} = %s")
            params.append(v)
    if not set_parts:
        return False
    set_parts.append("updated_at = NOW()")

    where = []
    if instantly_lead_id:
        where.append("instantly_lead_id = %s")
        params.append(instantly_lead_id)
    elif email:
        where.append("LOWER(contact_email) = LOWER(%s)")
        params.append(email)

    sql = f"UPDATE raw.scraped_leads SET {', '.join(set_parts)} WHERE {' AND '.join(where)}"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            updated = cur.rowcount
        conn.commit()
        return updated > 0
    except Exception as e:
        conn.rollback()
        st.error(f"soft_delete_by_instantly_id_or_email failed: {e}")
        return False


def count_unlinked_leads_with_email_sb(conn: psycopg2.extensions.connection) -> int:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw.scraped_leads "
                "WHERE instantly_lead_id IS NULL "
                "AND contact_email IS NOT NULL AND contact_email <> '' "
                "AND excluded_at IS NULL"
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception as e:
        st.error(f"Error counting unlinked leads: {e}")
        conn.rollback()
        return 0


def count_leads_without_tier_sb(conn: psycopg2.extensions.connection) -> int:
    """Count leads with `ticket_tier IS NULL` — they won't appear in any
    tier-segmented campaign filter, so the operator should know how many
    rows are sitting outside the recategorization."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw.scraped_leads "
                "WHERE (ticket_tier IS NULL OR ticket_tier = '') "
                "AND excluded_at IS NULL"
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception as e:
        st.error(f"Error counting tier-less leads: {e}")
        conn.rollback()
        return 0


def fetch_distinct_industries_sb(conn: psycopg2.extensions.connection) -> list[str]:
    """Return the distinct, non-null industries currently in raw.scraped_leads."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT industry FROM raw.scraped_leads "
                "WHERE industry IS NOT NULL AND industry <> '' "
                "AND excluded_at IS NULL "
                "ORDER BY industry"
            )
            return [r[0] for r in cur.fetchall()]
    except Exception as e:
        st.error(f"Error loading industries: {e}")
        conn.rollback()
        return []


def count_leads_by_filter_sb(
    conn: psycopg2.extensions.connection,
    filter_spec: dict,
    *,
    exclude_in_active_campaign: bool = True,
    exclude_already_in_campaign_id: str | None = None,
) -> int:
    """Count `raw.scraped_leads` matching filter_spec."""
    where, params = build_filter_where(
        filter_spec,
        exclude_in_active_campaign=exclude_in_active_campaign,
        exclude_already_in_campaign_id=exclude_already_in_campaign_id,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM raw.scraped_leads WHERE {where}", params)
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception as e:
        st.error(f"Error counting filtered leads: {e}")
        conn.rollback()
        return 0


def fetch_leads_by_filter_sb(
    conn: psycopg2.extensions.connection,
    filter_spec: dict,
    *,
    limit: int | None = None,
    exclude_in_active_campaign: bool = True,
    exclude_already_in_campaign_id: str | None = None,
) -> list[dict]:
    """Return leads matching filter_spec. Caller controls `limit` (None = all)."""
    where, params = build_filter_where(
        filter_spec,
        exclude_in_active_campaign=exclude_in_active_campaign,
        exclude_already_in_campaign_id=exclude_already_in_campaign_id,
    )
    cols = ", ".join(_SAMPLE_COLUMNS)
    sql = f"SELECT {cols} FROM raw.scraped_leads WHERE {where} ORDER BY company_name"
    bound: list[Any] = list(params)
    if limit is not None:
        sql += " LIMIT %s"
        bound.append(int(limit))
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, bound)
            rows = cur.fetchall()
            return [_map_record_to_app(dict(r)) for r in rows]
    except Exception as e:
        st.error(f"Error fetching filtered leads: {e}")
        conn.rollback()
        return []


def create_campaign_record_sb(
    conn: psycopg2.extensions.connection,
    *,
    name: str,
    filter_spec: dict,
    instantly_campaign_id: str | None = None,
    status: str = "draft",
    created_by: str | None = None,
) -> str | None:
    """Insert (or refresh) a row in raw.campaigns. Returns the row id.

    Idempotent on `instantly_campaign_id`: the recategorize flow now persists
    the row right after resolving the campaign, before processing any leads,
    so a partially-failed or operator-cancelled run still leaves an audit
    record. Re-running the flow updates filter_spec / status / name in place
    rather than failing on the UNIQUE constraint.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO raw.campaigns
                       (name, filter_spec, instantly_campaign_id, status, created_by)
                   VALUES (%s, %s::jsonb, %s, %s, %s)
                   ON CONFLICT (instantly_campaign_id) DO UPDATE SET
                       name        = EXCLUDED.name,
                       filter_spec = EXCLUDED.filter_spec,
                       status      = EXCLUDED.status,
                       updated_at  = now()
                   RETURNING id""",
                (name, psycopg2.extras.Json(filter_spec), instantly_campaign_id,
                 status, created_by),
            )
            new_id = str(cur.fetchone()[0])
        conn.commit()
        return new_id
    except Exception as e:
        conn.rollback()
        st.error(f"Failed to record campaign: {e}")
        return None


def list_campaign_records_sb(conn: psycopg2.extensions.connection) -> list[dict]:
    """List recorded campaigns with their filter_spec for the audit view."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT id, name, filter_spec, instantly_campaign_id,
                          status, created_by, created_at
                     FROM raw.campaigns
                    ORDER BY created_at DESC
                    LIMIT 200"""
            )
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        st.error(f"Error loading campaign list: {e}")
        conn.rollback()
        return []


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

    # ── Campaign composer (filter by industry / ticket_tier) ──
    def fetch_distinct_industries(self) -> list[str]:
        return fetch_distinct_industries_sb(self.conn)

    def count_leads_without_tier(self) -> int:
        return count_leads_without_tier_sb(self.conn)

    # ── Reconciliation: relink orphans whose writeback was lost ──
    def fetch_unlinked_leads_with_email(self, *, limit: int | None = None) -> list[dict]:
        return fetch_unlinked_leads_with_email_sb(self.conn, limit=limit)

    def count_unlinked_leads_with_email(self) -> int:
        return count_unlinked_leads_with_email_sb(self.conn)

    def find_pushed_siblings_by_email(
        self,
        *,
        emails: list[str],
        exclude_campaign_id: str | None,
    ) -> dict:
        return find_pushed_siblings_by_email_sb(
            self.conn,
            emails=emails,
            exclude_campaign_id=exclude_campaign_id,
        )

    def clear_link_on_sibling_rows(
        self, *, instantly_lead_id: str, keep_row_id: str,
    ) -> int:
        return clear_link_on_sibling_rows_sb(
            self.conn,
            instantly_lead_id=instantly_lead_id,
            keep_row_id=keep_row_id,
        )

    def soft_delete_by_instantly_id_or_email(
        self, *, instantly_lead_id: str | None, email: str | None, fields: dict,
    ) -> bool:
        return soft_delete_by_instantly_id_or_email_sb(
            self.conn,
            instantly_lead_id=instantly_lead_id, email=email, fields=fields,
        )

    def count_leads_by_filter(
        self,
        filter_spec: dict,
        *,
        exclude_in_active_campaign: bool = True,
        exclude_already_in_campaign_id: str | None = None,
    ) -> int:
        return count_leads_by_filter_sb(
            self.conn, filter_spec,
            exclude_in_active_campaign=exclude_in_active_campaign,
            exclude_already_in_campaign_id=exclude_already_in_campaign_id,
        )

    def fetch_leads_by_filter(
        self,
        filter_spec: dict,
        *,
        limit: int | None = None,
        exclude_in_active_campaign: bool = True,
        exclude_already_in_campaign_id: str | None = None,
    ) -> list[dict]:
        return fetch_leads_by_filter_sb(
            self.conn, filter_spec, limit=limit,
            exclude_in_active_campaign=exclude_in_active_campaign,
            exclude_already_in_campaign_id=exclude_already_in_campaign_id,
        )

    def create_campaign_record(
        self,
        *,
        name: str,
        filter_spec: dict,
        instantly_campaign_id: str | None = None,
        status: str = "draft",
        created_by: str | None = None,
    ) -> str | None:
        return create_campaign_record_sb(
            self.conn,
            name=name,
            filter_spec=filter_spec,
            instantly_campaign_id=instantly_campaign_id,
            status=status,
            created_by=created_by,
        )

    def list_campaign_records(self) -> list[dict]:
        return list_campaign_records_sb(self.conn)

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
