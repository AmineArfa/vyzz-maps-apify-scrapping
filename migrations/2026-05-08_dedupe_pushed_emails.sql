-- 2026-05-08 — collapse remaining duplicate-email pairs in linked rows
--
-- Pre-condition for the partial unique index in
-- 2026-05-08_unique_email_when_pushed.sql. The 2026-05-08 IT review
-- patched duplicate-lead_id rows (same lead_id on multiple raws) but
-- did not touch the inverse pattern: same email on multiple raws with
-- *different* lead_ids. Three such pairs remained
--   * adrianan@thehotelumd.com   (2 rows, 2 distinct lead_ids)
--   * chanson@omegasrliving.com  (2 rows, 2 distinct lead_ids)
--   * ksprtel@congregationalhome.org (2 rows, 2 distinct lead_ids)
--
-- Tie-break: keep the row with verification_status = 'ok' (deterministic
-- on the observed data; both sets had exactly one verified row per
-- email). NULL the link fields on the other row so the index can land.
--
-- IMPORTANT: this leaves the loser row's Instantly lead_id orphaned
-- inside Instantly itself (same email exists as two separate Instantly
-- leads). That orphan must be cleaned up via the Instantly UI / API as
-- a follow-up — the DB cleanup here is necessary but not sufficient.
-- See clients/_internal/2026-05-08_instantly_sync_health/it_review.md.
--
-- Applied 2026-05-08 to project ptkgkjzwtlyxdmwypomm via Supabase MCP.

WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY LOWER(contact_email)
               ORDER BY (CASE WHEN verification_status = 'ok' THEN 0 ELSE 1 END),
                        updated_at DESC,
                        id
           ) AS rn
      FROM raw.scraped_leads
     WHERE instantly_lead_id IS NOT NULL
       AND excluded_at IS NULL
       AND LOWER(contact_email) IN (
              SELECT LOWER(contact_email)
                FROM raw.scraped_leads
               WHERE instantly_lead_id IS NOT NULL
                 AND excluded_at IS NULL
               GROUP BY LOWER(contact_email)
              HAVING count(*) > 1
       )
)
UPDATE raw.scraped_leads l
   SET instantly_lead_id      = NULL,
       instantly_campaign_id  = NULL,
       instantly_status       = NULL,
       instantly_synced_at    = NULL,
       updated_at             = NOW()
  FROM ranked r
 WHERE l.id = r.id
   AND r.rn > 1;
