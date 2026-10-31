-- 0001_analytics_schema.sql
-- Analytics Database Schema, Security Roles, and Row-Level Security (RLS)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Roles setup (ensure roles exist)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'api_ro') THEN
        CREATE ROLE api_ro WITH LOGIN PASSWORD 'api_ro_secret' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'worker_rw') THEN
        CREATE ROLE worker_rw WITH LOGIN PASSWORD 'worker_rw_secret' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
    END IF;
END
$$;

-- 1. Metadata & Control Tables
CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    health TEXT NOT NULL DEFAULT 'healthy' CHECK (health IN ('healthy', 'degraded', 'quarantined')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_keys (
    key_hash TEXT PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id TEXT PRIMARY KEY,
    last_processed_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dead_letter (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    bucket_hour TIMESTAMPTZ NOT NULL,
    error_message TEXT NOT NULL,
    attempts INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NULL,
    action TEXT NOT NULL,
    details JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Aggregation Rollup Tables
CREATE TABLE IF NOT EXISTS agg_hourly (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    hour TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    order_count BIGINT NOT NULL DEFAULT 0,
    sales_paise BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, hour, status)
);

CREATE TABLE IF NOT EXISTS agg_hourly_product (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    hour TIMESTAMPTZ NOT NULL,
    product_id UUID NOT NULL,
    category TEXT NOT NULL,
    units BIGINT NOT NULL DEFAULT 0,
    sales_paise BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, hour, product_id)
);

CREATE TABLE IF NOT EXISTS bucket_state (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    hour TIMESTAMPTZ NOT NULL,
    published_version BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, hour)
);

-- Indexes on Rollups
CREATE INDEX IF NOT EXISTS idx_agg_hourly_tenant_hour ON agg_hourly (tenant_id, hour);
CREATE INDEX IF NOT EXISTS idx_agg_hourly_prod_tenant_hour ON agg_hourly_product (tenant_id, hour);
CREATE INDEX IF NOT EXISTS idx_agg_hourly_prod_tenant_sales ON agg_hourly_product (tenant_id, sales_paise DESC);

-- 3. Row-Level Security (RLS) Configuration
-- Enable and FORCE Row-Level Security so table owners / restricted roles cannot bypass it
ALTER TABLE agg_hourly ENABLE ROW LEVEL SECURITY;
ALTER TABLE agg_hourly FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agg_hourly_tenant_policy ON agg_hourly;
CREATE POLICY agg_hourly_tenant_policy ON agg_hourly
    FOR ALL
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

ALTER TABLE agg_hourly_product ENABLE ROW LEVEL SECURITY;
ALTER TABLE agg_hourly_product FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agg_hourly_product_tenant_policy ON agg_hourly_product;
CREATE POLICY agg_hourly_product_tenant_policy ON agg_hourly_product
    FOR ALL
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

ALTER TABLE bucket_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE bucket_state FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS bucket_state_tenant_policy ON bucket_state;
CREATE POLICY bucket_state_tenant_policy ON bucket_state
    FOR ALL
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

-- 4. Role Grants & Permissions
-- Worker Permissions (Read/Write)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO worker_rw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO worker_rw;

-- API Role Permissions (SELECT ONLY, NO DDL, NO BYPASSRLS)
GRANT SELECT ON agg_hourly TO api_ro;
GRANT SELECT ON agg_hourly_product TO api_ro;
GRANT SELECT ON bucket_state TO api_ro;
GRANT SELECT ON tenants TO api_ro;
GRANT SELECT ON api_keys TO api_ro;
GRANT INSERT ON audit_log TO api_ro;
GRANT USAGE, SELECT ON SEQUENCE audit_log_id_seq TO api_ro;
