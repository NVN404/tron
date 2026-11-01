-- 0001_analytics_schema.sql
-- Multi-Source Analytics Database Schema, Security Roles, and Row-Level Security (RLS)

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
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    capture_mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    is_degraded BOOLEAN NOT NULL DEFAULT FALSE,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenants (
    tenant_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    name TEXT NULL,
    health TEXT NOT NULL DEFAULT 'healthy' CHECK (health IN ('healthy', 'degraded', 'quarantined')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_source_external_tenant UNIQUE (source_id, external_id)
);

CREATE TABLE IF NOT EXISTS api_keys (
    key_hash TEXT PRIMARY KEY,
    tenant_uuid UUID NOT NULL REFERENCES tenants(tenant_uuid) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT 'default',
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_cursors (
    source_id TEXT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    cursor_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE SEQUENCE IF NOT EXISTS dirty_seq_seq START 1;

CREATE TABLE IF NOT EXISTS dirty_buckets (
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    dirty_seq BIGINT NOT NULL DEFAULT nextval('dirty_seq_seq'),
    status TEXT NOT NULL DEFAULT 'pending',
    claimed_by TEXT,
    claim_token TEXT,
    locked_until TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    PRIMARY KEY (source_id, tenant_id, hour)
);
CREATE INDEX IF NOT EXISTS idx_dirty_claim ON dirty_buckets (status, locked_until, dirty_seq);

CREATE TABLE IF NOT EXISTS bucket_state (
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    published_seq BIGINT NOT NULL DEFAULT 0,
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    row_count BIGINT NOT NULL DEFAULT 0,
    total_amount_minor BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (source_id, tenant_id, hour)
);

CREATE TABLE IF NOT EXISTS dead_letter (
    id BIGSERIAL PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INT NOT NULL,
    error_message TEXT NOT NULL,
    payload JSONB
);

CREATE TABLE IF NOT EXISTS source_health (
    source_id TEXT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    is_healthy BOOLEAN NOT NULL DEFAULT TRUE,
    degraded_reason TEXT,
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    tenant_uuid UUID NULL REFERENCES tenants(tenant_uuid) ON DELETE CASCADE,
    details JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- 2. Aggregation Rollup Tables
CREATE TABLE IF NOT EXISTS agg_hourly_status (
    tenant_uuid UUID NOT NULL REFERENCES tenants(tenant_uuid) ON DELETE CASCADE,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    hour TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    order_count BIGINT NOT NULL DEFAULT 0,
    total_amount_minor BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_uuid, hour, status)
);

CREATE TABLE IF NOT EXISTS agg_hourly_product (
    tenant_uuid UUID NOT NULL REFERENCES tenants(tenant_uuid) ON DELETE CASCADE,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    hour TIMESTAMPTZ NOT NULL,
    product_id TEXT NOT NULL,
    units_sold BIGINT NOT NULL DEFAULT 0,
    gross_revenue_minor BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_uuid, hour, product_id)
);

-- Backward compatibility view / table for legacy queries
CREATE OR REPLACE VIEW agg_hourly AS
SELECT
    tenant_uuid AS tenant_id,
    hour,
    status,
    order_count,
    total_amount_minor AS sales_paise
FROM agg_hourly_status;

-- Indexes on Rollups
CREATE INDEX IF NOT EXISTS idx_agg_hourly_status_tenant_hour ON agg_hourly_status (tenant_uuid, hour);
CREATE INDEX IF NOT EXISTS idx_agg_hourly_prod_tenant_hour ON agg_hourly_product (tenant_uuid, hour);
CREATE INDEX IF NOT EXISTS idx_agg_hourly_prod_tenant_sales ON agg_hourly_product (tenant_uuid, gross_revenue_minor DESC);

-- 3. Row-Level Security (RLS) Configuration
-- Enable and FORCE Row-Level Security so table owners / restricted roles cannot bypass it
ALTER TABLE agg_hourly_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE agg_hourly_status FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agg_hourly_status_tenant_policy ON agg_hourly_status;
CREATE POLICY agg_hourly_status_tenant_policy ON agg_hourly_status
    FOR ALL
    USING (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

ALTER TABLE agg_hourly_product ENABLE ROW LEVEL SECURITY;
ALTER TABLE agg_hourly_product FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agg_hourly_product_tenant_policy ON agg_hourly_product;
CREATE POLICY agg_hourly_product_tenant_policy ON agg_hourly_product
    FOR ALL
    USING (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenants_tenant_policy ON tenants;
CREATE POLICY tenants_tenant_policy ON tenants
    FOR ALL
    USING (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS audit_log_tenant_policy ON audit_log;
CREATE POLICY audit_log_tenant_policy ON audit_log
    FOR ALL
    USING (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_uuid = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

-- 4. Role Grants & Permissions
GRANT USAGE ON SCHEMA public TO worker_rw, api_ro;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO worker_rw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO worker_rw;

-- API Role Permissions (SELECT ONLY on specific tables, NO DDL, NO BYPASSRLS)
GRANT SELECT ON agg_hourly_status TO api_ro;
GRANT SELECT ON agg_hourly_product TO api_ro;
GRANT SELECT ON agg_hourly TO api_ro;
GRANT SELECT ON tenants TO api_ro;
GRANT SELECT ON api_keys TO api_ro;
GRANT INSERT ON audit_log TO api_ro;
GRANT USAGE, SELECT ON SEQUENCE audit_log_id_seq TO api_ro;
