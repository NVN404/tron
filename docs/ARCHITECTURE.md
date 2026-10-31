# Multi-Tenant Aggregation Layer: System Architecture & Safety Proofs

## 1. System Architecture Diagram

```
                 +--------------------------------------------------+
                 |              Operational Source DB               |
                 |                   (Port 5433)                    |
                 |  - orders (10M rows, skewed Zipf distribution)   |
                 |  - outbox (dirty-bucket hint queue)              |
                 +------------------------+-------------------------+
                                          |
                                          | Trigger writes (tenant_id, bucket_hour)
                                          v
                 +--------------------------------------------------+
                 |               Rust Worker Service                |
                 |                                                  |
                 | 1. Claim batch (FOR UPDATE SKIP LOCKED + token)  |
                 | 2. Deduplicate affected (tenant, hour) buckets   |
                 | 3. Source Snapshot: REPEATABLE READ READ ONLY    |
                 | 4. Compute Rollups (counts, sales, products)     |
                 | 5. Analytics Tx: Version-fenced atomic publish   |
                 | 6. Ack claimed outbox rows (token-predicated)    |
                 +------------------------+-------------------------+
                                          |
                                          | Atomic UPSERT into
                                          v
                 +--------------------------------------------------+
                 |               Analytics DB (Port 5434)           |
                 |                                                  |
                 |  - agg_hourly (tenant_id, hour, status, ...)     |
                 |  - agg_hourly_product (tenant_id, hour, prod...) |
                 |  - bucket_state (tracks published_version)       |
                 |  - RLS: FORCE ROW LEVEL SECURITY (NULLIF policy) |
                 |  - Role: api_ro (SELECT-only, NOBYPASSRLS)       |
                 +------------------------+-------------------------+
                                          ^
                                          | SET LOCAL app.tenant_id = $token
                                          | SELECT from precomputed rollups
                 +------------------------+-------------------------+
                 |               Rust Axum REST API                 |
                 |                                                  |
                 |  - Authenticate Bearer API key -> TenantId       |
                 |  - Enforce URL path tenant == Token tenant       |
                 |  - Boot Self-Check: verify role is not owner     |
                 |  - Zero response caching (INC-02 prevention)     |
                 +------------------------+-------------------------+
                                          ^
                                          | HTTP GET (sub-second queries)
                                          |
                                [ Tenant Dashboards ]
```

---

## 2. The Five System Proofs

### Proof 1: Tenant Isolation Proof (INC-02 Prevention)
- **The Threat:** Application-level filter omission, shared query caches returning Tenant A's cached aggregate to Tenant B, or database connection pool state leakage.
- **Defense Mechanism:**
  1. **Dual Verification:** Axum derives `tenant_id` exclusively from cryptographically hashed API keys stored in `api_keys`. If `/tenants/{id}/...` differs from the key's tenant, the API returns `404 Not Found` (indistinguishable from an invalid tenant).
  2. **Transaction-Scoped Database Context:** Every analytics query executes within a PostgreSQL transaction where:
     ```sql
     SELECT set_config('app.tenant_id', $1, true);
     ```
     The 3rd argument (`true`) ensures the variable is `SET LOCAL`, discarding it automatically when the transaction closes or returns to the pool.
  3. **Engine-Level Row-Level Security (RLS):** Both `ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY` are enforced on all rollup tables. The policy:
     ```sql
     CREATE POLICY tenant_isolation_policy ON agg_hourly
     FOR ALL
     USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)
     WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
     ```
  4. **Unprivileged Role:** The API connects exclusively as `api_ro`. `api_ro` does NOT own tables, does NOT have `BYPASSRLS`, and has `NOSUPERUSER`.
  5. **Fail-Closed Guarantee:** If a query executes with an empty or missing `app.tenant_id`, `NULLIF` evaluates to `NULL`, the condition `tenant_id = NULL` evaluates to `UNKNOWN`, and PostgreSQL returns **0 rows**.
  6. **Boot Self-Check:** On startup, the Axum server queries `pg_roles` and `pg_class`. If `api_ro` has `rolbypassrls`, is a superuser, owns rollup tables, or if any tenant table lacks `relforcerowsecurity`, the server process **aborts immediately**.

### Proof 2: Correctness Proof (The Anti-Delta Axiom)
- **The Threat:** Blind incremental updates (`count = count + 1`, `sales = sales + amount`) inevitably drift when orders are edited, cancelled, refunded, backdated, or when events are reprocessed.
- **Defense Mechanism:**
  1. **Full-Bucket Recomputation:** Outbox messages are strictly *dirty hints*. The worker does not read event deltas. Instead, it identifies the affected `(tenant_id, bucket_hour)` and recomputes the entire hour's aggregation from scratch against the source database.
  2. **Snapshot Isolation:** The worker reads from the source database using `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`. This ensures all status breakdowns, counts, and product totals reflect a globally consistent, point-in-time snapshot.
  3. **Direct-SQL Oracle Validation:** An independent test oracle computes the exact aggregation on the source database. The automated test suite asserts that `Analytics API Result == Direct SQL Oracle Result` across all 4 query dimensions.

### Proof 3: Freshness & Version Fencing Proof
- **The Threat:** Worker A claims an outbox batch for bucket `(T1, H1)`. Worker A pauses (GC pause or network lag). The lease expires. Worker B claims a newer batch for `(T1, H1)`, recomputes, and publishes version 100. Worker A unpauses and attempts to overwrite the analytics database with stale data from version 50.
- **Defense Mechanism:**
  1. **Lease & Heartbeat Protocol:** Workers claim outbox rows with `FOR UPDATE SKIP LOCKED` and assign a unique `claim_token` UUID and `locked_until` timestamp. A background heartbeat task extends `locked_until` while processing.
  2. **Version Fencing:** In the analytics database, `bucket_state` maintains `published_version = MAX(outbox_id)`. Publishing occurs inside a single transaction with an advisory lock:
     ```sql
     SELECT pg_advisory_xact_lock(hashtext($tenant_id || $hour));
     ```
     The publish script updates rollups if and only if:
     ```sql
     WHERE incoming_version > COALESCE(published_version, -1)
     ```
     If Worker A is stale (`50 <= 100`), the transaction rejects the write. Worker A's subsequent outbox acknowledgement also fails because its claim has expired.

### Proof 4: Source Database Protection & Performance Proof
- **The Threat:** Running heavy analytics queries on a 10M-row production OLTP database exhausts CPU, fills buffer pools, and causes lock contention with checkout transactions.
- **Defense Mechanism:**
  1. **Zero Dashboard Traffic on Source:** 100% of tenant dashboard requests hit the Analytics DB (`agg_hourly`), which contains at most a few thousand precomputed rows per tenant, queried by primary key in `<5ms`.
  2. **Minimal Trigger Footprint:** The source trigger executes `AFTER INSERT OR UPDATE OR DELETE` and performs zero joins and zero aggregations; it inserts a single 32-byte tuple into `outbox`.
  3. **Deduplication & Rate Limiting:** Even if 10,000 orders are inserted for a hot tenant in 10 seconds, the worker collapses all outbox hints for that hour into a single dirty bucket and executes **exactly one** recompute query.
  4. **Strict Safety Caps:** All source worker reads execute with `statement_timeout = '2s'` and unprivileged read credentials.

### Proof 5: Crash Recovery & Resiliency Proof
- **The Threat:** Workers crashing mid-batch, power failure, or network partitions leaving jobs abandoned or data inconsistent.
- **Defense Mechanism:**
  1. **Two-Phase Durable Acknowledgement:** Outbox status transitions from `pending` -> `claimed` -> `done`. Acknowledgement occurs only **after** the analytics database transaction commits.
  2. **Automatic Lease Expiry Reclaim:** If a worker crashes while holding a claim, `locked_until` naturally expires. Other workers pick up the expired claim on the next polling loop via:
     ```sql
     WHERE status = 'pending' OR (status = 'claimed' AND locked_until < NOW())
     ```
  3. **Idempotency & Rebuild:** Because rollup writes are atomic replaces (`DELETE` then `INSERT` within the same transaction), jobs can be retried infinitely without double-counting.
  4. **Continuous Reconciler:** A background daemon compares cryptographic hashes and checksums between the source database and analytics rollups, queuing dirty-bucket recomputes whenever drift is detected.
