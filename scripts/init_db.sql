-- ─────────────────────────────────────────────
--  Delllo RAIN3.0 — PostgreSQL Schema
--  Run order matters: tenants → users → profiles
--  → documents → facts → signals → matches
-- ─────────────────────────────────────────────

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- for text search

-- ── Tenants ───────────────────────────────────────────────────────
CREATE TABLE tenants (
    tenant_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
    config_json JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Users ─────────────────────────────────────────────────────────
CREATE TABLE users (
    user_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id        UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    external_subject TEXT,                                    -- SSO subject
    email            TEXT NOT NULL,
    display_name     TEXT NOT NULL,
    role             TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin', 'member', 'viewer')),
    status           TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, email)
);

-- ── User Profiles ─────────────────────────────────────────────────
CREATE TABLE user_profiles (
    profile_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id             UUID NOT NULL UNIQUE REFERENCES users(user_id) ON DELETE CASCADE,
    headline            TEXT,
    summary             TEXT,                                 -- latest LLM-generated summary
    default_visibility  TEXT NOT NULL DEFAULT 'match_engine_only'
                        CHECK (default_visibility IN ('private', 'match_engine_only', 'tenant_discoverable')),
    home_location       TEXT,                                 -- coarse location e.g. "Amsterdam HQ"
    embedding           vector(1536),                        -- profile embedding for similarity
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Documents ─────────────────────────────────────────────────────
CREATE TABLE documents (
    document_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id    UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    user_id      UUID REFERENCES users(user_id) ON DELETE SET NULL,  -- nullable = shared doc
    source_type  TEXT NOT NULL CHECK (source_type IN ('cv', 'paper', 'note', 'bio', 'upload', 'meeting_note', 'chat')),
    filename     TEXT,
    mime_type    TEXT,
    storage_uri  TEXT,                                        -- MinIO object path
    checksum     TEXT,                                        -- sha256 for dedup
    status       TEXT NOT NULL DEFAULT 'uploaded' CHECK (status IN ('uploaded', 'ingested', 'parsed', 'extracted', 'failed')),
    meta_json    JSONB DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Document Chunks ───────────────────────────────────────────────
CREATE TABLE document_chunks (
    chunk_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id    UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
    chunk_index    INT  NOT NULL,
    text           TEXT NOT NULL,
    token_count    INT,
    embedding      vector(1536),                             -- pgvector semantic embedding
    metadata_json  JSONB DEFAULT '{}',                       -- page number, headings, etc.
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Extracted Facts ───────────────────────────────────────────────
CREATE TABLE extracted_facts (
    fact_id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id             UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    user_id               UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    fact_type             TEXT NOT NULL CHECK (fact_type IN ('skill', 'domain', 'objective', 'offer', 'achievement', 'constraint')),
    canonical_value       TEXT NOT NULL,                     -- normalized e.g. "ml_credit_pricing"
    raw_value             TEXT NOT NULL,                     -- extracted phrase verbatim
    confidence            NUMERIC(4,3) CHECK (confidence BETWEEN 0 AND 1),
    freshness_date        DATE,
    source_document_id    UUID REFERENCES documents(document_id) ON DELETE SET NULL,
    source_chunk_id       UUID REFERENCES document_chunks(chunk_id) ON DELETE SET NULL,
    visibility            TEXT NOT NULL DEFAULT 'match_engine_only'
                          CHECK (visibility IN ('private', 'match_engine_only', 'tenant_discoverable', 'mutual_match_only', 'public_event_only')),
    validated_by_user     BOOLEAN NOT NULL DEFAULT FALSE,
    validated_by_outcome  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Live Signals ──────────────────────────────────────────────────
CREATE TABLE live_signals (
    signal_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id    UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    user_id      UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    signal_type  TEXT NOT NULL CHECK (signal_type IN ('intent', 'presence', 'urgency', 'availability', 'meeting_outcome')),
    payload_json JSONB NOT NULL DEFAULT '{}',
    valid_from   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to     TIMESTAMPTZ,                                -- NULL = still active
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Matches ───────────────────────────────────────────────────────
CREATE TABLE matches (
    match_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id        UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    person_a         UUID NOT NULL REFERENCES users(user_id),   -- initiator/requester
    person_b         UUID NOT NULL REFERENCES users(user_id),   -- candidate
    transaction_type TEXT NOT NULL,
    score            NUMERIC(5,4),
    status           TEXT NOT NULL DEFAULT 'recommended'
                     CHECK (status IN ('recommended', 'accepted', 'dismissed', 'expired')),
    explanation_id   UUID,                                      -- FK set after explanation created
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT no_self_match CHECK (person_a != person_b)
);

-- ── Match Score Breakdown ─────────────────────────────────────────
CREATE TABLE match_scores (
    match_score_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    match_id             UUID NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    relevance            NUMERIC(4,3),
    complementarity      NUMERIC(4,3),
    timing               NUMERIC(4,3),
    proximity            NUMERIC(4,3),
    evidence_strength    NUMERIC(4,3),
    outcome_likelihood   NUMERIC(4,3),
    novelty              NUMERIC(4,3),
    privacy_risk         NUMERIC(4,3),
    interaction_friction NUMERIC(4,3),
    score_version        TEXT NOT NULL DEFAULT 'v1.0',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Explanations ──────────────────────────────────────────────────
CREATE TABLE explanations (
    explanation_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    match_id         UUID NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    explanation_text TEXT,                                    -- policy-safe narrative
    agenda_text      TEXT,                                    -- suggested agenda
    opening_question TEXT,
    model_used       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Feedback Events ───────────────────────────────────────────────
CREATE TABLE feedback_events (
    feedback_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id      UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    match_id       UUID NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    actor_user_id  UUID NOT NULL REFERENCES users(user_id),
    feedback_type  TEXT NOT NULL CHECK (feedback_type IN ('accepted', 'dismissed', 'useful', 'not_useful', 'met', 'no_show')),
    payload_json   JSONB DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Audit Log ─────────────────────────────────────────────────────
CREATE TABLE audit_log (
    audit_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id     UUID REFERENCES tenants(tenant_id) ON DELETE SET NULL,
    actor_user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    action        TEXT NOT NULL,
    object_type   TEXT NOT NULL,
    object_id     TEXT,
    decision_json JSONB DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────
--  INDEXES
-- ─────────────────────────────────────────────

-- Vector similarity search on chunks
CREATE INDEX idx_chunks_embedding       ON document_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX idx_profiles_embedding     ON user_profiles   USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Tenant-scoped lookups (everything goes through tenant_id)
CREATE INDEX idx_users_tenant           ON users           (tenant_id);
CREATE INDEX idx_documents_tenant_user  ON documents       (tenant_id, user_id);
CREATE INDEX idx_facts_tenant_user_type ON extracted_facts (tenant_id, user_id, fact_type);
CREATE INDEX idx_signals_tenant_user    ON live_signals    (tenant_id, user_id);
CREATE INDEX idx_matches_tenant         ON matches         (tenant_id);
CREATE INDEX idx_matches_person_a       ON matches         (person_a, status);

-- Active signals lookup
CREATE INDEX idx_signals_active         ON live_signals    (tenant_id, user_id, signal_type) WHERE valid_to IS NULL;

-- Text search on facts
CREATE INDEX idx_facts_canonical_trgm   ON extracted_facts USING gin (canonical_value gin_trgm_ops);

-- Audit log time-series
CREATE INDEX idx_audit_tenant_time      ON audit_log       (tenant_id, created_at DESC);

-- ─────────────────────────────────────────────
--  SEED DATA — Dev tenant + admin user
-- ─────────────────────────────────────────────

INSERT INTO tenants (tenant_id, name, slug, status) VALUES
    ('00000000-0000-0000-0000-000000000001', 'Delllo Dev', 'dev', 'active'),
    ('00000000-0000-0000-0000-000000000002', 'ING Amsterdam', 'ing-amsterdam', 'active');

INSERT INTO users (user_id, tenant_id, email, display_name, role) VALUES
    ('00000000-0000-0000-0001-000000000001', '00000000-0000-0000-0000-000000000001', 'admin@delllo.dev', 'Dev Admin', 'admin'),
    ('00000000-0000-0000-0001-000000000002', '00000000-0000-0000-0000-000000000002', 'trader@ing.nl', 'ING Trader', 'member'),
    ('00000000-0000-0000-0001-000000000003', '00000000-0000-0000-0000-000000000002', 'quant@ing.nl', 'ING Quant', 'member');

INSERT INTO user_profiles (user_id, headline, home_location, default_visibility) VALUES
    ('00000000-0000-0000-0001-000000000001', 'Platform Administrator', 'London', 'match_engine_only'),
    ('00000000-0000-0000-0001-000000000002', 'Credit Trader — Illiquid Bonds', 'Amsterdam HQ Floor 3', 'match_engine_only'),
    ('00000000-0000-0000-0001-000000000003', 'Quantitative Analyst — ML Pricing', 'Amsterdam HQ Floor 7', 'match_engine_only');

-- Seed an extracted fact to verify the stack end-to-end
INSERT INTO extracted_facts
    (tenant_id, user_id, fact_type, canonical_value, raw_value, confidence, visibility, validated_by_user)
VALUES
    ('00000000-0000-0000-0000-000000000002',
     '00000000-0000-0000-0001-000000000003',
     'skill',
     'ml_credit_pricing',
     'ML-based credit pricing for illiquid corporate bonds',
     0.91,
     'match_engine_only',
     true);
