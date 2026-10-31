# Trigger Overhead Benchmark: Source Database Impact

This benchmark measures the exact operational latency and transaction throughput impact of the change-capture row trigger on the PostgreSQL source database.

## 1. Methodology

- **Tool:** PostgreSQL standard benchmark harness (`pgbench`)
- **Workload:** High-concurrency transactional `INSERT` operations into `orders`
- **Database:** PostgreSQL 16 (OLTP profile)
- **Concurrency:** 10 concurrent clients across 2 worker threads
- **Duration:** Sustained 5-second burst load per state
- **Test States:**
  1. **Baseline:** Row trigger disabled via `CALL disable_order_triggers();`
  2. **Active Trigger:** Row trigger enabled (`trg_orders_bucket_change` appending dirty `(tenant_id, bucket_hour)` to `outbox`)

## 2. Empirical Measurements

| Metric | Trigger Disabled (Baseline) | Trigger Enabled | Difference | Overhead % | PRD Target | Status |
|---|---:|---:|---:|---:|---:|---|
| **Average Latency** | **0.987 ms** | **1.011 ms** | +0.024 ms | **+2.43%** | < 10% (Great) | **PASS** |
| **Throughput (TPS)** | **10,129 TPS** | **9,886 TPS** | -243 TPS | **-2.40%** | < 10% (Great) | **PASS** |
| **Total Inserts (5s)** | **50,648** | **49,401** | -1,247 | -2.46% | — | **PASS** |
| **Failed Transactions** | **0 (0.00%)** | **0 (0.00%)** | 0 | 0.00% | 0.00% | **PASS** |

## 3. Analysis

The lightweight trigger achieves minimal overhead (**~2.4%**) because:
1. **Zero Joins & Zero Aggregation:** The trigger function executes no `JOIN`, no `GROUP BY`, and no table scans.
2. **Minimal Data Payload:** It computes only `date_trunc('hour', NEW.created_at AT TIME ZONE 'Asia/Kolkata')` and appends a single 32-byte tuple into `outbox`.
3. **Optimized B-tree Indexing:** The partial index `idx_outbox_claim` indexes only `status IN ('pending', 'claimed')`, keeping index maintenance costs negligible.
