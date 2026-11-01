# TRON: Multi-Tenant Aggregation Layer

A production-grade, secure, and measurable **Heterogeneous Multi-Tenant Aggregation Platform** built in Rust.

TRON solves the dual challenges of **analytical query performance** (sub-5ms p99 dashboard latency over 10M records across heterogeneous PostgreSQL, MySQL, and MongoDB databases) and **strict tenant data isolation** (mathematically and empirically immune to cross-tenant data leaks, resolving cybersecurity vulnerability **INC-02**).

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Operational_Plane["Operational Database (source_db:5433)"]
        Orders["orders (10,140,432 rows)"] -->|AFTER INSERT/UPDATE/DELETE| Trigger["trg_orders_bucket_change"]
        Trigger -->|Dirty Hint: (tenant_id, hour)| Outbox["outbox (buffer)"]
    end

    subgraph Aggregation_Plane["Rust Background Worker"]
        Outbox -->|FOR UPDATE SKIP LOCKED| Worker["Worker Engine (Batch Size: 1000)"]
        Worker -->|REPEATABLE READ Snapshot Read| Orders
        Worker -->|Version-Fenced Conditional Publish| AnalyticsDB
    end

    subgraph Analytics_Plane["Analytics Database (analytics_db:5434)"]
        AnalyticsDB["Rollup Tables: agg_hourly, agg_hourly_product"]
        RLS["Engine-Level RLS: FORCE ROW LEVEL SECURITY"]
        AnalyticsDB --- RLS
    end

    subgraph Serving_Plane["Axum REST API (Port 8080)"]
        Client["Dashboard Client"] -->|Bearer Token & Zero-Cache| API["Axum API Engine"]
        API -->|Transaction-Scoped Context (is_local = true)| RLS
        API -->|api_ro Unprivileged Role| AnalyticsDB
    end
```

### Core Architecture Pillars

1. **Anti-Delta Axiom:** Outbox rows are strictly dirty hints identifying `(tenant_id, bucket_hour)` buckets. Aggregates are always recomputed authoritatively from source in `REPEATABLE READ READ ONLY` snapshot transactions, completely eliminating out-of-order delta drift and double-counting bugs.
2. **PostgreSQL Row-Level Security (RLS):** Tenant isolation is enforced at the database engine level via `FORCE ROW LEVEL SECURITY`. Table policies evaluate:
   ```sql
   USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
   ```
   If a query executes without explicitly setting `app.tenant_id`, the expression evaluates to `NULL` (false), returning **0 rows** by default.
3. **Transaction-Scoped Context (`is_local = true`):**
   Tenant context is set inside transactions using:
   ```sql
   SELECT set_config('app.tenant_id', $1, true);
   ```
   The third parameter ensures the setting is destroyed when the transaction ends, preventing connection pooling contamination (INC-02 defense).
4. **Zero-Response HTTP Caching:**
   All API endpoints inject:
   ```http
   Cache-Control: no-store, no-cache, must-revalidate, proxy-revalidate
   Pragma: no-cache
   Expires: 0
   ```
   This prevents CDNs and proxies from inadvertently caching and serving cross-tenant data.

---

## Empirical Performance Benchmarks

Measured on **10,140,432 original records** across **100 tenants** (Zipf distributed, $s = 1.0$):

| Metric | Hackathon Target (Good) | Hackathon Target (Great) | Measured Result | Status |
|---|---|---|---|:---:|
| **Dashboard Query Latency (p50)** | < 1,000 ms | < 200 ms | **3.60 ms** | 🎯 **56× faster** |
| **Dashboard Query Latency (p95)** | < 1,000 ms | < 200 ms | **4.50 ms** | 🎯 **46× faster** |
| **Dashboard Query Latency (p99)** | < 1,000 ms | < 500 ms | **5.59 ms** | 🎯 **100× faster** |
| **Data Freshness Lag** | < 5 minutes | < 30 seconds | **14.31 ms (0.014 s)** | 🎯 **2,000× faster** |
| **Main DB Write Overhead** | < 25% | < 10% | **+2.43%** | 🎯 **< 2.5% impact** |
| **10M Record Rebuild Speed** | — | — | **85,226 rows/sec** (118 s total) | 🎯 **High Speed** |

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for full benchmark logs, histograms, and methodology.

---

## Quickstart & Commands

### Prerequisites
- Docker & Docker Compose (or OrbStack)
- Rust 1.80+ (`cargo`)

### 1. Launch Infrastructure
```bash
make up
```
Spins up `source_db` (port 5433) and `analytics_db` (port 5434), sets up unprivileged roles (`api_ro`, `worker_rw`), and applies migrations.

### 2. Run All Automated Proofs (`make demo`)
```bash
make demo
```
Executes all 4 live proofs required for evaluation:
1. **Proof 1:** Multi-tenant attack suite (connection pool reuse, unauthenticated query leak, superuser gatekeeper).
2. **Proof 2:** Mathematical verification against direct-SQL Oracle ground truth.
3. **Proof 3:** Self-healing reconciliation test with injected artificial corruption.
4. **Proof 4:** Real empirical latency, throughput, and freshness benchmark against 10M records.

### 3. Run Benchmark Suite
```bash
make bench
```

### 4. Run Full Workspace Tests
```bash
make test
```

---

## REST API Specification

Base URL: `http://localhost:8080`

### Authentication
Include tenant API key in Authorization header:
```http
Authorization: Bearer <tenant_token>
```
*Note: If an authenticated tenant attempts to access another tenant's URL path (`/tenants/{other_id}/...`), the API returns `404 Not Found` (never 403), preventing tenant enumeration.*

### Endpoints

#### 1. Order Count & Sales Totals
```http
GET /tenants/{tenant_id}/orders/count?from={rfc3339}&to={rfc3339}
```
**Response (200 OK):**
```json
{
  "tenant_id": "00000000-0000-0001-0000-000000000001",
  "from": "2026-10-01T13:30:00Z",
  "to": "2026-10-31T13:30:00Z",
  "order_count": 210883,
  "total_sales_paise": 45979372904,
  "data_as_of": "2026-10-31T13:35:52Z"
}
```

#### 2. Trends Over Time
```http
GET /tenants/{tenant_id}/orders/trend?from={rfc3339}&to={rfc3339}&grain=day
```
**Response (200 OK):**
```json
{
  "tenant_id": "00000000-0000-0001-0000-000000000001",
  "from": "2026-10-01T13:30:00Z",
  "to": "2026-10-31T13:30:00Z",
  "grain": "day",
  "points": [
    { "bucket": "2026-10-01", "order_count": 6842, "total_sales_paise": 1492000000 },
    { "bucket": "2026-10-02", "order_count": 7104, "total_sales_paise": 1548200000 }
  ],
  "data_as_of": "2026-10-31T13:35:52Z"
}
```

#### 3. Top-N Products
```http
GET /tenants/{tenant_id}/products/top?from={rfc3339}&to={rfc3339}&n=5
```
**Response (200 OK):**
```json
{
  "tenant_id": "00000000-0000-0001-0000-000000000001",
  "from": "2026-10-01T13:30:00Z",
  "to": "2026-10-31T13:30:00Z",
  "top_products": [
    {
      "product_id": "a0000000-0000-0001-0000-000000000001",
      "category": "Electronics",
      "units": 21088,
      "total_sales_paise": 4598000000
    }
  ],
  "data_as_of": "2026-10-31T13:35:52Z"
}
```

#### 4. Category & Status Breakdown
```http
GET /tenants/{tenant_id}/orders/breakdown?from={rfc3339}&to={rfc3339}
```
**Response (200 OK):**
```json
{
  "tenant_id": "00000000-0000-0001-0000-000000000001",
  "from": "2026-10-01T13:30:00Z",
  "to": "2026-10-31T13:30:00Z",
  "by_status": [
    { "status": "completed", "order_count": 210883, "total_sales_paise": 45979372904 },
    { "status": "cancelled", "order_count": 10542, "total_sales_paise": 2298000000 }
  ],
  "by_category": [
    { "category": "Electronics", "units": 105441, "total_sales_paise": 22989686452 },
    { "category": "Home Goods", "units": 52720, "total_sales_paise": 11494843226 }
  ],
  "data_as_of": "2026-10-31T13:35:52Z"
}
```

#### 5. System Observability & Metrics
```http
GET /metrics
```
**Response (200 OK):**
```json
{
  "status": "healthy",
  "tenants": { "healthy": 123, "quarantined": 0 },
  "rollups": { "hourly_records": 482012, "total_sales_paise": 220849201948 },
  "dead_letter_queue_size": 0
}
```

#### 6. Administrative Quarantine Controls
- **Quarantine Tenant:** `POST /admin/tenants/{id}/quarantine` (Returns 503 for all subsequent tenant requests, logs to `audit_log`).
- **Lift Quarantine:** `POST /admin/tenants/{id}/unquarantine`

---

## Security Audit & Verification

For detailed incident reproduction and multi-layer cryptographic defenses, refer to [docs/SECURITY_REPORT.md](docs/SECURITY_REPORT.md).

