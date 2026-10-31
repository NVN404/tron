-- 0001_source_schema.sql
-- Source Database Schema (OLTP Engine)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Tenants
CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Products per Tenant
CREATE TABLE IF NOT EXISTS products (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    product_id UUID NOT NULL,
    category TEXT NOT NULL,
    PRIMARY KEY (tenant_id, product_id)
);

-- Customers per Tenant
CREATE TABLE IF NOT EXISTS customers (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    customer_id UUID NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (tenant_id, customer_id)
);

-- Orders Table (Primary 10M OLTP table)
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    product_id UUID NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL, -- 'completed', 'pending', 'cancelled', 'refunded'
    amount_paise BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ NULL
);

-- Tenant-Leading Composite Indexes for Fast Tenant-Scoped Slices
CREATE INDEX IF NOT EXISTS idx_orders_tenant_created 
    ON orders (tenant_id, created_at);

CREATE INDEX IF NOT EXISTS idx_orders_tenant_product_created 
    ON orders (tenant_id, product_id, created_at);

CREATE INDEX IF NOT EXISTS idx_orders_tenant_status_created 
    ON orders (tenant_id, status, created_at);
