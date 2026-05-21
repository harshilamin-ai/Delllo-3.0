-- ─────────────────────────────────────────────────────────────────
--  Delllo RAIN3.0 — Migration: Node Integration Support
--  Run this ONCE against your existing database.
--  Safe to run on a live DB — uses IF NOT EXISTS / ALTER constraints.
-- ─────────────────────────────────────────────────────────────────


-- ── 1. Expand fact_type CHECK constraint ─────────────────────────
--  The original constraint only allowed:
--    skill | domain | objective | offer | achievement | constraint
--  The new profile update endpoint also writes:
--    need | topic | location
--  Postgres requires DROP + ADD to change a CHECK constraint.

ALTER TABLE extracted_facts
    DROP CONSTRAINT IF EXISTS extracted_facts_fact_type_check;

ALTER TABLE extracted_facts
    ADD CONSTRAINT extracted_facts_fact_type_check
    CHECK (fact_type IN (
        'skill',
        'domain',
        'objective',
        'offer',
        'achievement',
        'constraint',
        'need',
        'topic',
        'location'
    ));


-- ── 2. Add updated_at to documents table ─────────────────────────
--  ingestion.py does:  SET status = 'ingested', updated_at = NOW()
--  but the original schema has no updated_at on documents.

ALTER TABLE documents
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();


-- ── 3. Add unique constraint on extracted_facts ───────────────────
--  profiles.py uses ON CONFLICT (tenant_id, user_id, fact_type, canonical_value)
--  This requires a unique index/constraint on those four columns.

ALTER TABLE extracted_facts
    DROP CONSTRAINT IF EXISTS extracted_facts_unique_fact;

ALTER TABLE extracted_facts
    ADD CONSTRAINT extracted_facts_unique_fact
    UNIQUE (tenant_id, user_id, fact_type, canonical_value);


-- ── 4. Add index on users.status for active-user queries ─────────
CREATE INDEX IF NOT EXISTS idx_users_status
    ON users (tenant_id, status);


-- ── Verify ────────────────────────────────────────────────────────
SELECT
    'fact_type constraint updated'   AS check_1,
    'documents.updated_at added'     AS check_2,
    'extracted_facts unique added'   AS check_3;