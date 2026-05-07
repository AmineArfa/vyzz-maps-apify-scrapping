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

-- Permissions for scraper_app (raw.* only). Adjust role name if different.
GRANT SELECT, INSERT, UPDATE ON raw.campaigns TO scraper_app;
