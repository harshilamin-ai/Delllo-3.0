CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- ORGANISATIONS
-- =====================================================

CREATE TABLE IF NOT EXISTS organisations (
    org_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    domain TEXT,
    status TEXT DEFAULT 'active'
        CHECK (status IN ('active','suspended')),
    config_json JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- NETWORK JOIN RULES
-- =====================================================

CREATE TABLE IF NOT EXISTS network_join_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    rule_type TEXT NOT NULL
        CHECK (rule_type IN ('email_domain','open','explicit')),
    rule_value TEXT,
    created_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- USER TENANTS
-- =====================================================

CREATE TABLE IF NOT EXISTS user_tenants (
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','active','rejected','removed')),
    role TEXT NOT NULL DEFAULT 'member'
        CHECK (role IN ('admin','member','viewer')),
    joined_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, tenant_id)
);

-- =====================================================
-- TENANTS TABLE MODIFICATION
-- =====================================================

ALTER TABLE tenants
ADD COLUMN IF NOT EXISTS org_id UUID REFERENCES organisations(org_id);

-- =====================================================
-- BACKFILL USER MEMBERSHIPS
-- =====================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name='users'
        AND column_name='tenant_id'
    ) THEN

        INSERT INTO user_tenants (
            user_id,
            tenant_id,
            status,
            role,
            joined_at
        )
        SELECT
            user_id,
            tenant_id,
            'active',
            COALESCE(role, 'member'),
            NOW()
        FROM users
        ON CONFLICT DO NOTHING;

    END IF;
END $$;

-- =====================================================
-- REMOVE OLD CONSTRAINTS
-- =====================================================

ALTER TABLE users
DROP CONSTRAINT IF EXISTS users_tenant_id_email_key;

-- =====================================================
-- REMOVE OLD TENANT COLUMN
-- =====================================================

ALTER TABLE users
DROP COLUMN IF EXISTS tenant_id;

-- =====================================================
-- GLOBAL EMAIL UNIQUENESS
-- =====================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'users_email_unique'
    ) THEN

        ALTER TABLE users
        ADD CONSTRAINT users_email_unique UNIQUE (email);

    END IF;
END $$;

-- =====================================================
-- INDEXES
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_user_tenants_user
ON user_tenants (user_id);

CREATE INDEX IF NOT EXISTS idx_user_tenants_tenant
ON user_tenants (tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_tenants_org
ON tenants (org_id);

CREATE INDEX IF NOT EXISTS idx_join_rules_tenant
ON network_join_rules (tenant_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_open_rule
ON network_join_rules (tenant_id)
WHERE rule_type = 'open';