-- 2026-05-10 — clear orphan-satellite rows where instantly_status IS NULL
--
-- Sister migration to 2026-05-08_clear_orphaned_satellite_fields.sql.
-- That one handled rows where `instantly_status IS NOT NULL` (the
-- "Status=Success but lead_id=NULL" inconsistency). It left untouched
-- 76 rows where the same inconsistency exists but `instantly_status IS
-- NULL` — these have `instantly_campaign_id` and
-- `instantly_synced_at='2026-05-07 21:11:18 UTC'` set but no lead_id and
-- no status. Pre-date the P0c cleanup.
--
-- Effect: `_dedupe_in_batch` (leadgen/campaign_push.py) sees
-- `instantly_campaign_id IS NOT NULL` on these rows and skips them as
-- "already pushed", so they never re-enter the push queue. NULLing the
-- satellite columns lets them flow through normally on the next cron
-- run.
--
-- Distribution at write time (2026-05-10 10:19 UTC):
--   * 12 rows in High Tier (826fd7fb-1d56-4ff9-8b42-d75513a99b79)
--   * 64 rows in Mid Tier  (a9c92976-fe82-48cc-b300-5f1ea1b670be)
--   *  0 rows in any other campaign
-- All 76 share the same instantly_synced_at='2026-05-07 21:11:18 UTC'.
--
-- Filter rationale:
--   * `instantly_campaign_id IS NOT NULL` → satellite state present
--   * `instantly_lead_id IS NULL`         → no real Instantly link
--   * `instantly_status IS NULL`          → distinguishes from the
--                                            P0c migration's target set
--   * `excluded_at IS NULL`               → preserve any future Pruned
--                                            semantics (defensive)
--
-- Applied 2026-05-10 to project ptkgkjzwtlyxdmwypomm via Supabase MCP.
-- Idempotent: re-running matches zero rows once the cleanup has landed.

UPDATE raw.scraped_leads
   SET instantly_campaign_id = NULL,
       instantly_synced_at   = NULL,
       updated_at            = NOW()
 WHERE instantly_campaign_id IS NOT NULL
   AND instantly_lead_id     IS NULL
   AND instantly_status      IS NULL
   AND excluded_at           IS NULL;
