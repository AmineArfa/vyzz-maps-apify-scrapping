-- 2026-05-08 — clear orphaned satellite fields on raw.scraped_leads
--
-- Context: a one-shot UPDATE earlier today NULLed `instantly_lead_id` on
-- 6,026 raw rows that shared a duplicate lead_id with another row. That
-- patch did NOT touch the surrounding satellite columns
-- (`instantly_status`, `instantly_campaign_id`, `instantly_synced_at`),
-- which left ~5,542 rows in the contradictory state of "Status=Success
-- but no lead_id". This migration cleans those satellite fields so the
-- partial unique index in 2026-05-08_unique_email_when_pushed.sql can be
-- created without leftover dups, and so the next sync run sees a
-- consistent view.
--
-- Filter rationale:
--   * `instantly_lead_id IS NULL`     → row no longer linked to Instantly
--   * `instantly_status IS NOT NULL`  → there's still stale satellite state
--   * `excluded_at IS NULL`           → preserve the 1,638 intentionally
--                                       pruned rows whose `Pruned` status
--                                       is meaningful (excluded_at set)
--
-- Applied 2026-05-08 to project ptkgkjzwtlyxdmwypomm via Supabase MCP.
-- Idempotent: re-running matches zero rows once the cleanup has landed.

UPDATE raw.scraped_leads
   SET instantly_campaign_id = NULL,
       instantly_status      = NULL,
       instantly_synced_at   = NULL,
       updated_at            = NOW()
 WHERE instantly_lead_id IS NULL
   AND instantly_status IS NOT NULL
   AND excluded_at IS NULL;
