-- 2026-05-07 — raw.campaigns
--
-- Local registry of Instantly campaigns the scraping app has created or
-- assigned leads to. Holds the filter that segmented the leads (industry /
-- ticket_tier / both), so operators can audit "who got into what campaign
-- and why" without re-deriving from Instantly itself.
--
-- Lives in the `raw` schema because the scraping app is restricted to
-- raw.* (per dev/CLAUDE.md). Apply this migration as a privileged role,
-- then GRANT to scraper_app at the bottom.
--
-- Applied 2026-05-07 to project ptkgkjzwtlyxdmwypomm via Supabase MCP.

CREATE TABLE IF NOT EXISTS raw.campaigns (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    instantly_campaign_id uuid UNIQUE,
    name text NOT NULL,
    filter_spec jsonb,
    status text NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'active', 'archived')),
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS campaigns_filter_spec_gin
    ON raw.campaigns USING gin (filter_spec);

CREATE INDEX IF NOT EXISTS campaigns_instantly_campaign_id_idx
    ON raw.campaigns (instantly_campaign_id);

ALTER TABLE raw.campaigns ENABLE ROW LEVEL SECURITY;

-- Permissions for scraper_app (raw.* only). Mirrors the existing
-- raw.scraped_leads policy pattern: permissive USING/WITH CHECK = true,
-- scoped to the role. RLS without these would block the app entirely.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'scraper_app') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON raw.campaigns TO scraper_app';

        IF NOT EXISTS (
            SELECT 1 FROM pg_policies
            WHERE schemaname = 'raw' AND tablename = 'campaigns'
              AND policyname = 'scraper_app_select_all'
        ) THEN
            EXECUTE 'CREATE POLICY scraper_app_select_all ON raw.campaigns FOR SELECT TO scraper_app USING (true)';
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_policies
            WHERE schemaname = 'raw' AND tablename = 'campaigns'
              AND policyname = 'scraper_app_insert_all'
        ) THEN
            EXECUTE 'CREATE POLICY scraper_app_insert_all ON raw.campaigns FOR INSERT TO scraper_app WITH CHECK (true)';
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_policies
            WHERE schemaname = 'raw' AND tablename = 'campaigns'
              AND policyname = 'scraper_app_update_all'
        ) THEN
            EXECUTE 'CREATE POLICY scraper_app_update_all ON raw.campaigns FOR UPDATE TO scraper_app USING (true) WITH CHECK (true)';
        END IF;
    END IF;
END $$;
