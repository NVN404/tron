use crate::protocol::*;
use anyhow::Result;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

pub struct WorkerEngine {
    source_pool: PgPool,
    analytics_pool: PgPool,
    worker_name: String,
    batch_size: i64,
    lease_seconds: i64,
}

impl WorkerEngine {
    pub fn new(
        source_pool: PgPool,
        analytics_pool: PgPool,
        worker_name: String,
        batch_size: i64,
        lease_seconds: i64,
    ) -> Self {
        Self {
            source_pool,
            analytics_pool,
            worker_name,
            batch_size,
            lease_seconds,
        }
    }

    /// Claim a batch of outbox rows using FOR UPDATE SKIP LOCKED
    pub async fn claim_batch(&self) -> Result<Option<ClaimedBatch>> {
        let claim_token = Uuid::new_v4();
        let claimed_by = format!("{}-{}", self.worker_name, std::process::id());

        let mut tx = self.source_pool.begin().await?;

        let rows = sqlx::query(
            r#"
            WITH claimable AS (
                SELECT id FROM outbox
                WHERE (status = 'pending' AND next_retry_at <= NOW())
                   OR (status = 'claimed' AND locked_until < NOW())
                ORDER BY id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE outbox
            SET status = 'claimed',
                claimed_by = $2,
                claim_token = $3,
                locked_until = NOW() + ($4 || ' seconds')::interval,
                attempts = attempts + 1
            WHERE id IN (SELECT id FROM claimable)
            RETURNING id, tenant_id, bucket_hour, attempts;
            "#,
        )
        .bind(self.batch_size)
        .bind(&claimed_by)
        .bind(claim_token)
        .bind(self.lease_seconds)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let claimed_rows = rows
            .into_iter()
            .map(|r| ClaimedRow {
                id: r.get("id"),
                tenant_id: r.get("tenant_id"),
                bucket_hour: r.get("bucket_hour"),
                attempts: r.get("attempts"),
            })
            .collect();

        Ok(Some(ClaimedBatch {
            claim_token,
            claimed_by,
            rows: claimed_rows,
        }))
    }

    /// Deduplicate claimed hints into distinct (tenant_id, bucket_hour) targets with version = max(outbox_id)
    pub fn deduplicate_targets(&self, batch: &ClaimedBatch) -> Vec<(BucketTarget, i64)> {
        let mut map: HashMap<BucketTarget, i64> = HashMap::new();
        for r in &batch.rows {
            let target = BucketTarget {
                tenant_id: r.tenant_id,
                bucket_hour: r.bucket_hour,
            };
            let entry = map.entry(target).or_insert(r.id);
            if r.id > *entry {
                *entry = r.id;
            }
        }
        map.into_iter().collect()
    }

    /// Recompute rollups for a specific bucket slice inside a REPEATABLE READ snapshot on the source DB
    pub async fn recompute_bucket(&self, target: &BucketTarget, version: i64) -> Result<BucketComputation> {
        let mut tx = self.source_pool.begin().await?;

        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SET statement_timeout = '2s'")
            .execute(&mut *tx)
            .await?;

        // 1. Status rollups
        let status_rows = sqlx::query(
            r#"
            SELECT 
                status, 
                COUNT(*)::BIGINT AS order_count, 
                COALESCE(SUM(amount_paise), 0)::BIGINT AS sales_paise
            FROM orders
            WHERE tenant_id = $1
              AND created_at >= $2 
              AND created_at < ($2 + INTERVAL '1 hour')
              AND deleted_at IS NULL
            GROUP BY status
            "#,
        )
        .bind(target.tenant_id)
        .bind(target.bucket_hour)
        .fetch_all(&mut *tx)
        .await?;

        let status_rollups = status_rows
            .into_iter()
            .map(|r| StatusRollup {
                status: r.get("status"),
                order_count: r.get("order_count"),
                sales_paise: r.get("sales_paise"),
            })
            .collect();

        // 2. Product rollups (strictly completed orders)
        let prod_rows = sqlx::query(
            r#"
            SELECT 
                product_id, 
                category, 
                COUNT(*)::BIGINT AS units, 
                COALESCE(SUM(amount_paise), 0)::BIGINT AS sales_paise
            FROM orders
            WHERE tenant_id = $1
              AND created_at >= $2 
              AND created_at < ($2 + INTERVAL '1 hour')
              AND deleted_at IS NULL
              AND status = 'completed'
            GROUP BY product_id, category
            "#,
        )
        .bind(target.tenant_id)
        .bind(target.bucket_hour)
        .fetch_all(&mut *tx)
        .await?;

        let product_rollups = prod_rows
            .into_iter()
            .map(|r| ProductRollup {
                product_id: r.get("product_id"),
                category: r.get("category"),
                units: r.get("units"),
                sales_paise: r.get("sales_paise"),
            })
            .collect();

        tx.commit().await?;

        Ok(BucketComputation {
            target: target.clone(),
            version,
            status_rollups,
            product_rollups,
        })
    }

    /// Publish computation to Analytics DB atomically with version fencing and advisory locking
    pub async fn publish_bucket(&self, comp: &BucketComputation) -> Result<PublishResult> {
        let mut tx = self.analytics_pool.begin().await?;

        // 1. Transaction-scoped advisory lock on (tenant_id, bucket_hour)
        let lock_key = format!("{}-{}", comp.target.tenant_id, comp.target.bucket_hour);
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(&lock_key)
            .execute(&mut *tx)
            .await?;

        // 2. Version fencing check: incoming_version must be > published_version
        let current_version: Option<(i64,)> = sqlx::query_as(
            "SELECT published_version FROM bucket_state WHERE tenant_id = $1 AND hour = $2"
        )
        .bind(comp.target.tenant_id)
        .bind(comp.target.bucket_hour)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some((pub_ver,)) = current_version {
            if comp.version <= pub_ver {
                warn!(
                    "Stale worker write rejected: tenant={}, hour={}, incoming_ver={}, pub_ver={}",
                    comp.target.tenant_id, comp.target.bucket_hour, comp.version, pub_ver
                );
                tx.rollback().await?;
                return Ok(PublishResult::RejectedStale);
            }
        }

        // 3. Ensure tenant exists in analytics DB to satisfy foreign key constraint
        sqlx::query(
            "INSERT INTO tenants (id, name) VALUES ($1, 'Synced Tenant') ON CONFLICT (id) DO NOTHING"
        )
        .bind(comp.target.tenant_id)
        .execute(&mut *tx)
        .await?;

        // 4. Delete old rollups for this bucket
        sqlx::query("DELETE FROM agg_hourly WHERE tenant_id = $1 AND hour = $2")
            .bind(comp.target.tenant_id)
            .bind(comp.target.bucket_hour)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM agg_hourly_product WHERE tenant_id = $1 AND hour = $2")
            .bind(comp.target.tenant_id)
            .bind(comp.target.bucket_hour)
            .execute(&mut *tx)
            .await?;

        // 4. Insert recomputed status rollups (if empty, none are inserted!)
        for sr in &comp.status_rollups {
            sqlx::query(
                "INSERT INTO agg_hourly (tenant_id, hour, status, order_count, sales_paise) \
                 VALUES ($1, $2, $3, $4, $5)"
            )
            .bind(comp.target.tenant_id)
            .bind(comp.target.bucket_hour)
            .bind(&sr.status)
            .bind(sr.order_count)
            .bind(sr.sales_paise)
            .execute(&mut *tx)
            .await?;
        }

        // 5. Insert recomputed product rollups
        for pr in &comp.product_rollups {
            sqlx::query(
                "INSERT INTO agg_hourly_product (tenant_id, hour, product_id, category, units, sales_paise) \
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(comp.target.tenant_id)
            .bind(comp.target.bucket_hour)
            .bind(pr.product_id)
            .bind(&pr.category)
            .bind(pr.units)
            .bind(pr.sales_paise)
            .execute(&mut *tx)
            .await?;
        }

        // 6. Update bucket_state with published version
        sqlx::query(
            r#"
            INSERT INTO bucket_state (tenant_id, hour, published_version, computed_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (tenant_id, hour) 
            DO UPDATE SET published_version = EXCLUDED.published_version, computed_at = EXCLUDED.computed_at
            "#
        )
        .bind(comp.target.tenant_id)
        .bind(comp.target.bucket_hour)
        .bind(comp.version)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(PublishResult::Published)
    }

    /// Acknowledge outbox batch predicated on claimed_by and claim_token
    pub async fn acknowledge_batch(&self, batch: &ClaimedBatch) -> Result<u64> {
        let ids: Vec<i64> = batch.rows.iter().map(|r| r.id).collect();
        let res = sqlx::query(
            r#"
            UPDATE outbox
            SET status = 'done', processed_at = NOW()
            WHERE id = ANY($1)
              AND claimed_by = $2
              AND claim_token = $3
              AND status = 'claimed'
            "#
        )
        .bind(&ids)
        .bind(&batch.claimed_by)
        .bind(batch.claim_token)
        .execute(&self.source_pool)
        .await?;

        Ok(res.rows_affected())
    }

    /// Process one iteration of claiming, recomputing, publishing, and acking
    pub async fn process_one_batch(&self) -> Result<usize> {
        let batch = match self.claim_batch().await? {
            Some(b) => b,
            None => return Ok(0),
        };

        let row_count = batch.rows.len();
        let targets = self.deduplicate_targets(&batch);

        for (target, version) in targets {
            match self.recompute_bucket(&target, version).await {
                Ok(comp) => {
                    if let Err(e) = self.publish_bucket(&comp).await {
                        error!("Failed to publish bucket ({}, {}): {}", target.tenant_id, target.bucket_hour, e);
                    }
                }
                Err(e) => {
                    error!("Failed to recompute bucket ({}, {}): {}", target.tenant_id, target.bucket_hour, e);
                }
            }
        }

        // Acknowledge claimed outbox rows
        self.acknowledge_batch(&batch).await?;

        Ok(row_count)
    }

    /// Run continuous loop until shutdown signal is received
    pub async fn run_loop(&self, running: Arc<AtomicBool>, poll_interval_ms: u64) -> Result<()> {
        info!("Worker loop started (poll_interval = {}ms)...", poll_interval_ms);
        while running.load(Ordering::Relaxed) {
            match self.process_one_batch().await {
                Ok(0) => {
                    tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                }
                Ok(n) => {
                    // Work was processed; immediately poll again without sleep
                    tracing::debug!("Processed {} outbox rows", n);
                }
                Err(e) => {
                    error!("Worker loop iteration error: {}", e);
                    tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                }
            }
        }
        info!("Worker loop shut down cleanly.");
        Ok(())
    }
}
