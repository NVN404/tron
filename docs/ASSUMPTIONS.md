# Business Rules & System Assumptions

This document tracks all business rules, edge cases, and environmental constraints.
Every rule is marked as either **LOCKED** (prescribed by the architecture/PRD) or **OPEN** (pending explicit stakeholder/mentor decision).

| Rule | My Assumption / Working Design | Source in PRD / Prompt | Status |
|---|---|---|---|
| **Timezone & Granularity** | Bucketing is anchored in **Indian Standard Time (IST, UTC+05:30)** (`Asia/Kolkata`). Date boundaries (e.g. daily rollups) truncate at 00:00:00 IST. | Prompt Rule 6: "All bucketing in IST (confirm vs PRD)" | **LOCKED** (Configurable via `TIMEZONE`) |
| **Monetary Representation** | All currency values are stored, calculated, and returned as 64-bit integer **paise** (`BIGINT`). No floating-point math. | Prompt Rule 6: "Money = integer paise" | **LOCKED** |
| **Cancelled Orders in Totals** | `status = 'cancelled'` orders are excluded from `sales_paise` totals in `/count` and `/trend`, but tracked in `/breakdown`. | Industry standard financial accounting | **OPEN** (Awaiting mentor sign-off if cancelled count should be in `/count`) |
| **Refunds** | Refunded orders have `status = 'refunded'`. Their amount is retained as gross sales or deducted based on accounting policy. Current baseline: gross sales tracked under status='refunded'. | PRD general ecommerce model | **OPEN** |
| **Date Range Inclusivity** | Query ranges `from` and `to` follow half-open intervals: `[from, to)` (i.e. `created_at >= from AND created_at < to`). | ISO 8601 & SQL standard range conventions | **LOCKED** |
| **Empty Bucket Behavior** | For `/trend` queries (hourly/daily), all time buckets within the requested `[from, to)` range must be returned. If a tenant had 0 orders, return count = 0 and sales_paise = 0. | Prompt Phase 3 & 8 | **LOCKED** |
| **Top-N Metric ("Sales" vs "Units")** | `GET /tenants/{id}/products/top` ranks products by `total_sales_paise DESC, product_id ASC`. Units are returned in the payload but ranking is driven by revenue. | Prompt Rule 6: "Deterministic tie-breaks (ORDER BY sales DESC, product_id)" | **LOCKED** |
| **Top-N Deterministic Tie-Break** | If two products have identical sales, tie-break by `product_id ASC` (UUID comparison). | Prompt Rule 6 | **LOCKED** |
| **Rolling 7-Day Definition** | Exactly the previous 168 hours relative to the query watermark (`[NOW() - INTERVAL '7 days', NOW())`). | Standard telemetry window | **OPEN** |
| **Source Trigger Permission** | The source database allows creating an `outbox` table and an `AFTER INSERT OR UPDATE OR DELETE` row trigger. If prohibited, fallback to `updated_at` polling. | Prompt Phase 5 & README requirements | **LOCKED** (with polling fallback) |
| **Source Read Replica** | No read replica is assumed. Worker queries the primary source DB with low-priority, `REPEATABLE READ READ ONLY` snapshot queries with a strict 2s `statement_timeout`. | Prompt Section 3 & Phase 6 | **LOCKED** |
| **Tenant Context Enforcement** | `app.tenant_id` is set per transaction via `set_config('app.tenant_id', $1, true)`. Missing or mismatched tenant fails closed (0 rows). | Prompt Rule 7 & Phase 4/6/9 | **LOCKED** |
| **Response Caching** | No response caching (Redis or in-memory HTTP cache) in the API layer. Dashboards query pre-aggregated rollups in Analytics Postgres directly. Prevents INC-02 data leakage. | Prompt Section 2 & Rule 7 | **LOCKED** |
