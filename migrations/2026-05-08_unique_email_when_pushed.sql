-- 2026-05-08 — partial unique index on (lower(contact_email)) when pushed
--
-- Belt-and-braces against the duplicate-lead_id pattern. P0a (in-batch
-- dedupe) and P0b (sibling cleanup) prevent net-new duplicates in the
-- code path. This index makes them impossible: any future code path
-- that tries to push the same email twice while the first push is still
-- linked (instantly_lead_id NOT NULL, not soft-deleted) hits a clean
-- IntegrityError that the push code catches and routes to
-- `op="skipped"` with reason `duplicate-email-db-constraint`.
--
-- Scoped via partial WHERE to avoid blocking the import path: rows
-- without an instantly_lead_id (un-pushed leads) and excluded rows
-- (soft-deleted) can still share emails freely. Only "currently linked
-- to Instantly" rows must be unique by email.
--
-- Pre-condition: 2026-05-08_clear_orphaned_satellite_fields.sql has
-- been applied AND the IT-review one-shot UPDATE that NULLed duplicate
-- lead_ids has run. If neither has happened, this index creation will
-- fail because of leftover dups — investigate before forcing.
--
-- Applied 2026-05-08 to project ptkgkjzwtlyxdmwypomm via Supabase MCP.

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uniq_scraped_leads_email_when_pushed
    ON raw.scraped_leads (LOWER(contact_email))
 WHERE instantly_lead_id IS NOT NULL
   AND excluded_at IS NULL;
