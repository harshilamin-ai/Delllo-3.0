-- Delllo RAIN3.0 — Phase 2 DB Migration
-- Run once against your Postgres instance:
--   docker compose exec postgres psql -U delllo -d delllo -f /migrations/phase2.sql

-- ── pgvector extension ──────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;

-- Add embedding column to document_chunks if not already present
ALTER TABLE document_chunks
    ADD COLUMN IF NOT EXISTS embedding vector(768);

-- Index for fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_chunks_embedding
    ON document_chunks
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- ── Feature snapshots (feedback learning) ──────────────────
CREATE TABLE IF NOT EXISTS feature_snapshots (
    snapshot_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID NOT NULL REFERENCES users(user_id),
    tenant_id     UUID NOT NULL REFERENCES tenants(tenant_id),
    feature_name  TEXT NOT NULL,          -- e.g. 'outcome_likelihood'
    feature_value NUMERIC(6,4) NOT NULL,
    computed_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, tenant_id, feature_name)
);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_user
    ON feature_snapshots(user_id, tenant_id);

-- ── Tenant ontology overrides ───────────────────────────────
CREATE TABLE IF NOT EXISTS tenant_ontology_overrides (
    override_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID NOT NULL REFERENCES tenants(tenant_id),
    override_type        TEXT NOT NULL,
    transaction_type_id  TEXT NOT NULL,
    target_capability    TEXT,
    weight_delta         NUMERIC(4,2),
    reason               TEXT,
    created_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_overrides_tenant
    ON tenant_ontology_overrides(tenant_id, transaction_type_id);

-- ── Notifications table ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS notifications (
    notification_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID NOT NULL REFERENCES tenants(tenant_id),
    recipient_user_id UUID NOT NULL REFERENCES users(user_id),
    notification_type TEXT NOT NULL,
    title             TEXT NOT NULL,
    body              TEXT NOT NULL,
    payload_json      JSONB DEFAULT '{}',
    channel           TEXT DEFAULT 'webhook',
    status            TEXT DEFAULT 'pending',
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    sent_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notifications_pending
    ON notifications(status, created_at)
    WHERE status = 'pending';

-- ── explanations: add model_used if missing ─────────────────
ALTER TABLE explanations
    ADD COLUMN IF NOT EXISTS model_used TEXT;

-- Add unique constraint on match_id so ON CONFLICT works
ALTER TABLE explanations
    DROP CONSTRAINT IF EXISTS explanations_match_id_key;
ALTER TABLE explanations
    ADD CONSTRAINT explanations_match_id_key UNIQUE (match_id);

-- ── match_scores: add missing columns if upgrading from Phase 1 ──
ALTER TABLE match_scores
    ADD COLUMN IF NOT EXISTS complementarity     NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS timing              NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS proximity           NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS outcome_likelihood  NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS novelty             NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS privacy_risk        NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS interaction_friction NUMERIC(6,4);