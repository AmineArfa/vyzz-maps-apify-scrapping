-- 2026-05-10 — repoint stale lead_id references after Instantly auto-dedupe
--
-- Discovered while executing yesterday's IT review remediation. The
-- 2026-05-08_dedupe_pushed_emails.sql migration kept the row with
-- `verification_status='ok'` and NULLed the loser. But Instantly itself
-- silently auto-deduplicated on its side BETWEEN our two pushes (using
-- email as the canonical key) and kept the OPPOSITE row of what our
-- tie-break picked. Net effect: for two of the three deduplicated
-- emails, the lead_id we held onto in raw.scraped_leads now points at
-- a non-existent (404) Instantly lead, while the lead_id we NULLed is
-- the one actually living in Instantly and being sent to.
--
-- Verified 2026-05-10 via Instantly MCP get_lead:
--   * adrianan@thehotelumd.com
--       DB-kept    019be925-c2a3-7878-9526-24291518cbe4 → 404
--       Instantly  019be927-7f47-7d51-99c5-ddb65766ed18 → exists, on
--                  Low Tier campaign 282213cc-…, payload.lid matches id
--   * ksprtel@congregationalhome.org
--       DB-kept    019b14e6-530a-7ae0-ad9a-77ba093c81de → 404
--       Instantly  019be94b-35a1-72f0-ac85-4c6bd4b21230 → exists, on
--                  High Tier campaign 826fd7fb-…, payload.lid matches id
--   * chanson@omegasrliving.com
--       DB-kept    019be94c-1570-7291-915f-53149a5e8c8e → exists ✓ — no
--       repoint required.
--
-- Effect: this UPDATE rewrites the two stale lead_ids on the kept rows
-- so the DB matches Instantly's reality. The DB row identity (the
-- internal id, contact_email, ticket_tier, verification_status,
-- instantly_campaign_id, instantly_status, instantly_synced_at) all
-- stay unchanged — only `instantly_lead_id` moves.
--
-- This unblocks any future writeback / per-lead lookup that joined on
-- `instantly_lead_id`, and lets the partial unique index continue to
-- guarantee one Success row per email.
--
-- Applied 2026-05-10 to project ptkgkjzwtlyxdmwypomm via Supabase MCP.
-- Idempotent: re-running is a no-op once the new ids are in place.

-- adrianan@thehotelumd.com
UPDATE raw.scraped_leads
   SET instantly_lead_id = '019be927-7f47-7d51-99c5-ddb65766ed18',
       updated_at        = NOW()
 WHERE id                = '936eadea-a93e-4edd-93e8-cd107a6a8703'
   AND instantly_lead_id = '019be925-c2a3-7878-9526-24291518cbe4';

-- ksprtel@congregationalhome.org
UPDATE raw.scraped_leads
   SET instantly_lead_id = '019be94b-35a1-72f0-ac85-4c6bd4b21230',
       updated_at        = NOW()
 WHERE id                = 'c123bfc3-b1f5-46d4-b818-3dd948efd526'
   AND instantly_lead_id = '019b14e6-530a-7ae0-ad9a-77ba093c81de';
