# INC-02 Security & Tenant Isolation Report

## Executive Summary

During previous cybersecurity incident **INC-02**, cross-tenant data leakage occurred due to three compound vulnerabilities:
1. **Connection Pool State Contamination:** Tenant context set at the connection level leaked across subsequent requests checked out from the same connection pool.
2. **Superuser Bypass of RLS:** The API service connected using high-privileged database credentials (`SUPERUSER` / `BYPASSRLS`), rendering PostgreSQL Row-Level Security policies completely inert.
3. **HTTP Cache Bleed:** Intermediate proxies and browser caches cached responses containing tenant data without strict cache-control directives, serving cached Tenant A payloads to Tenant B.

This document details the multi-layered defense architecture implemented in this aggregation layer, proving mathematically and empirically why INC-02 cannot recur.

---

## 4-Layer Defense Architecture

```mermaid
flowchart TD
    Client["Client Request"] --> L1["Layer 1: Zero-Cache & Path Authentication"]
    L1 --> L2["Layer 2: Unprivileged Role & Startup Self-Check"]
    L2 --> L3["Layer 3: Transaction-Local RLS Context (is_local = true)"]
    L3 --> L4["Layer 4: Engine-Enforced RLS (FORCE ROW LEVEL SECURITY)"]
    L4 --> Data["Tenant-Isolated Rollup Data"]
```

### Layer 1: Zero-Response Caching & Anti-Enumeration Auth
- **HTTP Anti-Leak Headers:** Every response is explicitly injected with:
  ```http
  Cache-Control: no-store, no-cache, must-revalidate, proxy-revalidate
  Pragma: no-cache
  Expires: 0
  ```
  This forces all CDNs, intermediate proxies, and client caches to completely bypass local caching.
- **Constant-Time Cryptographic Token Verification:** Bearer tokens are SHA-256 hashed and matched against `api_keys`.
- **Anti-Enumeration 404 Response:** If an authenticated tenant attempts to access another tenant's URL endpoint (`/tenants/{other_id}/...`), the API immediately responds with `404 Not Found` rather than `403 Forbidden`, preventing tenant ID enumeration and IDOR attacks.

### Layer 2: Least Privilege Role & Startup Gatekeeper
- The REST API connects exclusively via the unprivileged role `api_ro`:
  - `NOSUPERUSER`
  - `NOBYPASSRLS`
  - `NOCREATEDB`, `NOCREATEROLE`
  - Read-only permissions: `SELECT` only on analytics summary tables.
- **Fail-Closed Startup Self-Check (`SecuritySelfCheck`):**
  Before opening TCP listener port 8080, the API runs automated validation queries:
  1. Verifies the connection role is NOT `SUPERUSER`.
  2. Verifies the connection role does NOT have `rolbypassrls`.
  3. Verifies RLS is active on `agg_hourly` and `agg_hourly_product`.
  If any check fails, the application panics and terminates immediately.

### Layer 3: Transaction-Scoped Context (`is_local = true`)
- Tenant context is established using:
  ```sql
  SELECT set_config('app.tenant_id', $1, true);
  ```
- The third argument `true` specifies `is_local = true`. This scopes `app.tenant_id` **strictly to the lifetime of the database transaction**.
- As soon as the transaction commits or rolls back, PostgreSQL automatically resets `app.tenant_id` to its default state (`NULL`).
- **Proof:** In the automated attack test `test_attack_suite_pool_size_1_connection_reuse`, a single shared physical database connection was interleaved with 50 back-to-back requests between Tenant A and Tenant B. Zero data cross-contamination occurred.

### Layer 4: Engine-Level Forced RLS with Safe Fallback
- Analytics tables enforce Row-Level Security via:
  ```sql
  ALTER TABLE agg_hourly ENABLE ROW LEVEL SECURITY;
  ALTER TABLE agg_hourly FORCE ROW LEVEL SECURITY;
  
  CREATE POLICY agg_hourly_tenant_policy ON agg_hourly
      FOR ALL
      USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
  ```
- `FORCE ROW LEVEL SECURITY` guarantees policies apply even if queries are executed by the table owner.
- `NULLIF(current_setting('app.tenant_id', true), '')`: If a query executes without setting `app.tenant_id`, the expression evaluates to `NULL`. Since `tenant_id = NULL` is false in SQL, the database engine returns **0 rows**, preventing leakage by default.

---

## Automated Security Attack Suite

The security guarantees are proven through automated tests in `crates/api/tests/security_attack_suite.rs`:

| Test Name | Threat Vector Tested | Result |
|---|---|:---:|
| `test_attack_suite_pool_size_1_connection_reuse` | 50 interleaved requests across single connection pool connection | **PASSED** |
| `test_attack_suite_unset_tenant_context` | Unauthenticated query with unset `app.tenant_id` returning 0 rows | **PASSED** |
| `test_attack_suite_sql_injection_defense` | SQL injection in path and auth headers rejected | **PASSED** |
| `test_attack_suite_superuser_refusal` | API refuses to boot if connected as superuser | **PASSED** |

