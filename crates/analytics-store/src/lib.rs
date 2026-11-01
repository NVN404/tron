//! Multi-Source Analytics Store implementation with strict RLS, version fencing, and lease claiming.

use anyhow::{Context, Result};
use platform_core::types::{BucketAggregate, BucketKey, Cursor};
use sqlx::postgres::PgPool;
use sqlx::{Row, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ClaimedBucket {
    pub key: BucketKey,
    pub dirty_seq: i64,
    pub claim_token: String,
    pub attempts: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishResult {
    Published { published_seq: i64 },
    Stale { published_seq: i64 },
}

#[derive(Clone)]
pub struct AnalyticsStore {
    pool: PgPool,
}

impl AnalyticsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Registers configured sources into the `sources` metadata table.
    pub async fn register_sources(&self, config: &platform_core::AppConfig) -> Result<()> {
        for src in &config.sources {
            let kind_str = format!("{:?}", src.kind).to_lowercase();
            let capture_str = format!("{:?}", src.capture).to_lowercase();
            sqlx::query(
                r#"
                INSERT INTO sources (id, kind, capture_mode, status, is_degraded, last_heartbeat)
                VALUES ($1, $2, $3, 'active', false, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    capture_mode = EXCLUDED.capture_mode,
                    last_heartbeat = NOW();
                "#,
            )
            .bind(&src.id)
            .bind(&kind_str)
            .bind(&capture_str)
            .execute(&self.pool)
            .await
            .with_context(|| format!("Failed to register source {}", src.id))?;
        }
        Ok(())
    }

    /// Inserts or updates dirty buckets and optionally updates the source cursor atomically.
    pub async fn upsert_dirty_and_cursor(
        &self,
        source_id: &str,
        buckets: &[BucketKey],
        cursor: Option<&Cursor>,
    ) -> Result<()> {
        let mut tx: Transaction<'_, sqlx::Postgres> = self.pool.begin().await?;

        for key in buckets {
            sqlx::query(
                r#"
                INSERT INTO dirty_buckets (
                    source_id, tenant_id, hour, dirty_seq, status, attempts, next_retry_at
                )
                VALUES ($1, $2, $3, nextval('dirty_seq_seq'), 'pending', 0, NULL)
                ON CONFLICT (source_id, tenant_id, hour) DO UPDATE SET
                    dirty_seq = nextval('dirty_seq_seq'),
                    status = 'pending',
                    attempts = 0,
                    next_retry_at = NULL;
                "#,
            )
            .bind(source_id)
            .bind(&key.tenant_id)
            .bind(key.hour_start_utc)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("Failed to upsert dirty bucket {:?}", key))?;
        }

        if let Some(c) = cursor {
            sqlx::query(
                r#"
                INSERT INTO source_cursors (source_id, cursor_value, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (source_id) DO UPDATE SET
                    cursor_value = EXCLUDED.cursor_value,
                    updated_at = NOW();
                "#,
            )
            .bind(source_id)
            .bind(c)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("Failed to update cursor for source {}", source_id))?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Reads current persisted cursor for a source.
    pub async fn get_source_cursor(&self, source_id: &str) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT cursor_value FROM source_cursors WHERE source_id = $1",
        )
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.get::<String, _>("cursor_value")))
    }

    /// Claims a batch of dirty buckets using `FOR UPDATE SKIP LOCKED`.
    /// Optionally filters by source_id for per-source concurrency management.
    pub async fn claim_buckets(
        &self,
        source_id: Option<&str>,
        worker_id: &str,
        limit: i64,
        lease_secs: i64,
    ) -> Result<Vec<ClaimedBucket>> {
        let claim_token = Uuid::new_v4().to_string();

        let rows = sqlx::query(
            r#"
            WITH to_claim AS (
                SELECT source_id, tenant_id, hour
                FROM dirty_buckets
                WHERE ($1::text IS NULL OR source_id = $1)
                  AND (status = 'pending'
                   OR (status = 'claimed' AND locked_until < NOW())
                   OR (status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= NOW()))
                ORDER BY dirty_seq ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE dirty_buckets d
            SET status = 'claimed',
                claimed_by = $3,
                claim_token = $4,
                locked_until = NOW() + make_interval(secs => $5::double precision)
            FROM to_claim
            WHERE d.source_id = to_claim.source_id
              AND d.tenant_id = to_claim.tenant_id
              AND d.hour = to_claim.hour
            RETURNING d.source_id, d.tenant_id, d.hour, d.dirty_seq, d.claim_token, d.attempts;
            "#,
        )
        .bind(source_id)
        .bind(limit)
        .bind(worker_id)
        .bind(&claim_token)
        .bind(lease_secs as f64)
        .fetch_all(&self.pool)
        .await?;

        let claimed = rows
            .into_iter()
            .map(|r| ClaimedBucket {
                key: BucketKey::new(
                    r.get::<String, _>("source_id"),
                    r.get::<String, _>("tenant_id"),
                    r.get("hour"),
                ),
                dirty_seq: r.get::<i64, _>("dirty_seq"),
                claim_token: r.get::<Option<String>, _>("claim_token").unwrap_or_default(),
                attempts: r.get::<i32, _>("attempts"),
            })
            .collect();

        Ok(claimed)
    }

    /// Heartbeat to extend the lease of a claimed bucket batch.
    pub async fn heartbeat(&self, claim_token: &str, lease_secs: i64) -> Result<u64> {
        let res = sqlx::query(
            r#"
            UPDATE dirty_buckets
            SET locked_until = NOW() + make_interval(secs => $1::double precision)
            WHERE claim_token = $2 AND status = 'claimed';
            "#,
        )
        .bind(lease_secs as f64)
        .bind(claim_token)
        .execute(&self.pool)
        .await?;

        Ok(res.rows_affected())
    }

    /// Resolves an internal `tenant_uuid` for `(source_id, external_id)`, creating if necessary.
    pub async fn get_or_create_tenant(
        &self,
        source_id: &str,
        external_id: &str,
    ) -> Result<Uuid> {
        let row = sqlx::query(
            r#"
            INSERT INTO tenants (source_id, external_id)
            VALUES ($1, $2)
            ON CONFLICT (source_id, external_id) DO UPDATE SET source_id = EXCLUDED.source_id
            RETURNING tenant_uuid;
            "#,
        )
        .bind(source_id)
        .bind(external_id)
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("Failed to get or create tenant {}/{}", source_id, external_id))?;

        Ok(row.get("tenant_uuid"))
    }

    /// Publishes a recomputed bucket aggregate with pg_advisory_xact_lock and version fencing.
    pub async fn publish_bucket(
        &self,
        claim: &ClaimedBucket,
        aggregate: &BucketAggregate,
    ) -> Result<PublishResult> {
        let mut tx = self.pool.begin().await?;

        // 1. Take advisory transaction lock on hash(source_id:tenant_id:hour)
        let lock_key = format!(
            "{}:{}:{}",
            claim.key.source_id,
            claim.key.tenant_id,
            claim.key.hour_start_utc.to_rfc3339()
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(&lock_key)
            .execute(&mut *tx)
            .await?;

        // 2. Version fence: check bucket_state.published_seq
        let existing = sqlx::query(
            r#"
            SELECT published_seq
            FROM bucket_state
            WHERE source_id = $1 AND tenant_id = $2 AND hour = $3
            FOR UPDATE;
            "#,
        )
        .bind(&claim.key.source_id)
        .bind(&claim.key.tenant_id)
        .bind(claim.key.hour_start_utc)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(r) = existing {
            let published_seq: i64 = r.get("published_seq");
            if published_seq >= claim.dirty_seq {
                // Stale recompute: reject as no-op
                // Still clear dirty bucket if dirty_seq matches claim
                sqlx::query(
                    r#"
                    DELETE FROM dirty_buckets
                    WHERE source_id = $1 AND tenant_id = $2 AND hour = $3
                      AND dirty_seq = $4 AND claim_token = $5;
                    "#,
                )
                .bind(&claim.key.source_id)
                .bind(&claim.key.tenant_id)
                .bind(claim.key.hour_start_utc)
                .bind(claim.dirty_seq)
                .bind(&claim.claim_token)
                .execute(&mut *tx)
                .await?;

                tx.commit().await?;
                return Ok(PublishResult::Stale { published_seq });
            }
        }

        // 3. Resolve tenant_uuid
        let tenant_row = sqlx::query(
            r#"
            INSERT INTO tenants (source_id, external_id)
            VALUES ($1, $2)
            ON CONFLICT (source_id, external_id) DO UPDATE SET source_id = EXCLUDED.source_id
            RETURNING tenant_uuid;
            "#,
        )
        .bind(&claim.key.source_id)
        .bind(&claim.key.tenant_id)
        .fetch_one(&mut *tx)
        .await?;
        let tenant_uuid: Uuid = tenant_row.get("tenant_uuid");

        // 4. Replace rollup rows in agg_hourly_status
        sqlx::query(
            r#"
            DELETE FROM agg_hourly_status
            WHERE tenant_uuid = $1 AND hour = $2;
            "#,
        )
        .bind(tenant_uuid)
        .bind(claim.key.hour_start_utc)
        .execute(&mut *tx)
        .await?;

        for s in &aggregate.status_aggregates {
            sqlx::query(
                r#"
                INSERT INTO agg_hourly_status (
                    tenant_uuid, source_id, hour, status, order_count, total_amount_minor
                )
                VALUES ($1, $2, $3, $4, $5, $6);
                "#,
            )
            .bind(tenant_uuid)
            .bind(&claim.key.source_id)
            .bind(claim.key.hour_start_utc)
            .bind(&s.status)
            .bind(s.order_count)
            .bind(s.sales_units)
            .execute(&mut *tx)
            .await?;
        }

        // 5. Replace rollup rows in agg_hourly_product
        sqlx::query(
            r#"
            DELETE FROM agg_hourly_product
            WHERE tenant_uuid = $1 AND hour = $2;
            "#,
        )
        .bind(tenant_uuid)
        .bind(claim.key.hour_start_utc)
        .execute(&mut *tx)
        .await?;

        for p in &aggregate.product_aggregates {
            sqlx::query(
                r#"
                INSERT INTO agg_hourly_product (
                    tenant_uuid, source_id, hour, product_id, units_sold, gross_revenue_minor
                )
                VALUES ($1, $2, $3, $4, $5, $6);
                "#,
            )
            .bind(tenant_uuid)
            .bind(&claim.key.source_id)
            .bind(claim.key.hour_start_utc)
            .bind(&p.product_id)
            .bind(p.units)
            .bind(p.sales_units)
            .execute(&mut *tx)
            .await?;
        }

        // Compute totals for bucket_state
        let total_count: i64 = aggregate.status_aggregates.iter().map(|s| s.order_count).sum();
        let total_minor: i64 = aggregate.status_aggregates.iter().map(|s| s.sales_units).sum();

        // 6. Update bucket_state
        sqlx::query(
            r#"
            INSERT INTO bucket_state (
                source_id, tenant_id, hour, published_seq, published_at, row_count, total_amount_minor
            )
            VALUES ($1, $2, $3, $4, NOW(), $5, $6)
            ON CONFLICT (source_id, tenant_id, hour) DO UPDATE SET
                published_seq = EXCLUDED.published_seq,
                published_at = NOW(),
                row_count = EXCLUDED.row_count,
                total_amount_minor = EXCLUDED.total_amount_minor;
            "#,
        )
        .bind(&claim.key.source_id)
        .bind(&claim.key.tenant_id)
        .bind(claim.key.hour_start_utc)
        .bind(claim.dirty_seq)
        .bind(total_count)
        .bind(total_minor)
        .execute(&mut *tx)
        .await?;

        // 7. Clear dirty_buckets ONLY where dirty_seq AND claim_token match!
        sqlx::query(
            r#"
            DELETE FROM dirty_buckets
            WHERE source_id = $1 AND tenant_id = $2 AND hour = $3
              AND dirty_seq = $4 AND claim_token = $5;
            "#,
        )
        .bind(&claim.key.source_id)
        .bind(&claim.key.tenant_id)
        .bind(claim.key.hour_start_utc)
        .bind(claim.dirty_seq)
        .bind(&claim.claim_token)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(PublishResult::Published {
            published_seq: claim.dirty_seq,
        })
    }

    /// Handles worker failure: retries with backoff or moves to dead_letter.
    pub async fn handle_failure(
        &self,
        claim: &ClaimedBucket,
        error_msg: &str,
        max_attempts: i32,
        backoff_base_ms: u64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let next_attempt = claim.attempts + 1;
        if next_attempt >= max_attempts {
            // Dead letter
            sqlx::query(
                r#"
                INSERT INTO dead_letter (source_id, tenant_id, hour, failed_at, attempts, error_message)
                VALUES ($1, $2, $3, NOW(), $4, $5);
                "#,
            )
            .bind(&claim.key.source_id)
            .bind(&claim.key.tenant_id)
            .bind(claim.key.hour_start_utc)
            .bind(next_attempt)
            .bind(error_msg)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                UPDATE dirty_buckets
                SET status = 'failed',
                    locked_until = NULL,
                    next_retry_at = NULL,
                    attempts = $4
                WHERE source_id = $1 AND tenant_id = $2 AND hour = $3;
                "#,
            )
            .bind(&claim.key.source_id)
            .bind(&claim.key.tenant_id)
            .bind(claim.key.hour_start_utc)
            .bind(next_attempt)
            .execute(&mut *tx)
            .await?;

            let degraded_reason = format!("Bucket {:?} reached max attempts: {}", claim.key, error_msg);
            sqlx::query(
                r#"
                INSERT INTO source_health (source_id, is_healthy, degraded_reason, last_checked_at)
                VALUES ($1, false, $2, NOW())
                ON CONFLICT (source_id) DO UPDATE SET
                    is_healthy = false,
                    degraded_reason = EXCLUDED.degraded_reason,
                    last_checked_at = NOW();
                "#,
            )
            .bind(&claim.key.source_id)
            .bind(&degraded_reason)
            .execute(&mut *tx)
            .await?;
        } else {
            // Exponential backoff in seconds
            let delay_secs = (backoff_base_ms as f64 / 1000.0) * (2.0f64.powi(claim.attempts));
            sqlx::query(
                r#"
                UPDATE dirty_buckets
                SET status = 'pending',
                    attempts = $4,
                    locked_until = NULL,
                    next_retry_at = NOW() + make_interval(secs => $5::double precision)
                WHERE source_id = $1 AND tenant_id = $2 AND hour = $3;
                "#,
            )
            .bind(&claim.key.source_id)
            .bind(&claim.key.tenant_id)
            .bind(claim.key.hour_start_utc)
            .bind(next_attempt)
            .bind(delay_secs)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Resolves an API key to its tenant_uuid.
    pub async fn resolve_api_key(&self, key_hash: &str) -> Result<Option<Uuid>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_uuid
            FROM api_keys
            WHERE key_hash = $1 AND revoked = false;
            "#,
        )
        .bind(key_hash)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.get("tenant_uuid")))
    }

    /// Creates an API key for a given tenant.
    pub async fn create_api_key(
        &self,
        key_hash: &str,
        tenant_uuid: Uuid,
        name: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO api_keys (key_hash, tenant_uuid, name, revoked, created_at)
            VALUES ($1, $2, $3, false, NOW())
            ON CONFLICT (key_hash) DO NOTHING;
            "#,
        )
        .bind(key_hash)
        .bind(tenant_uuid)
        .bind(name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
