-- ─────────────────────────────────────────────
--  Delllo RAIN3.0 — Migration: Full iKG node type support
--  Run after init_db.sql if you already have a running instance.
--  Safe to re-run (all statements are idempotent or guarded).
-- ─────────────────────────────────────────────

-- 1. Extend extracted_facts.fact_type to cover all iKG node types
ALTER TABLE extracted_facts
    DROP CONSTRAINT IF EXISTS extracted_facts_fact_type_check;

ALTER TABLE extracted_facts
    ADD CONSTRAINT extracted_facts_fact_type_check
    CHECK (fact_type IN (
        'skill',
        'domain',
        'topic',
        'need',
        'objective',
        'offer',
        'achievement',
        'asset',
        'project',
        'location',
        'constraint'
    ));

-- 2. Index for new fact types used in matching
CREATE INDEX IF NOT EXISTS idx_facts_tenant_user_topic
    ON extracted_facts (tenant_id, user_id, fact_type)
    WHERE fact_type = 'topic';

CREATE INDEX IF NOT EXISTS idx_facts_tenant_user_need
    ON extracted_facts (tenant_id, user_id, fact_type)
    WHERE fact_type = 'need';

CREATE INDEX IF NOT EXISTS idx_facts_tenant_user_asset
    ON extracted_facts (tenant_id, user_id, fact_type)
    WHERE fact_type = 'asset';

CREATE INDEX IF NOT EXISTS idx_facts_tenant_user_project
    ON extracted_facts (tenant_id, user_id, fact_type)
    WHERE fact_type = 'project';