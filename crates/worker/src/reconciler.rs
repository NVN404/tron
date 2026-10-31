use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct DriftReport {
    pub tenant_id: Uuid,
    pub bucket_hour: DateTime<Utc>,
    pub source_count: i64,
    pub source_sum: i64,
    pub rollup_count: i64,
    pub rollup_sum: i64,
    pub repaired: bool,
}

pub struct Reconciler {
    source_pool: PgPool,
    analytics_pool: PgPool,
}

impl Reconciler {
    pub fn new(source_pool: PgPool, analytics_pool: PgPool) -> Self {
        Self {
            source_pool,
            analytics_pool,
        }
    }

    /// Reconciles rollups against source data for a given tenant and time window.
    /// Detects silent corruption or drift, authoritatively repairs it, and writes an audit log.
    pub async fn reconcile_window(
        &self,
        tenant_id: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<DriftReport>> {
        // Align boundaries to IST hourly boundaries (+1800s offset from UTC hour)
        let from_aligned = Utc.timestamp_opt(((from.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();
        let to_aligned = Utc.timestamp_opt(((to.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();

        // 1. Fetch source aggregates
        let source_rows = sqlx::query(
            r#"
            SELECT 
                date_trunc('hour', created_at AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata' AS hour,
                COUNT(*)::BIGINT AS order_count,
                COALESCE(SUM(amount_paise), 0)::BIGINT AS sales_paise
            FROM orders
            WHERE tenant_id = $1
              AND created_at >= $2 AND created_at < $3
              AND deleted_at IS NULL
              AND status = 'completed'
            GROUP BY 1
            "#,
        )
        .bind(tenant_id)
        .bind(from_aligned)
        .bind(to_aligned)
        .fetch_all(&self.source_pool)
        .await?;

        let mut source_map: HashMap<DateTime<Utc>, (i64, i64)> = HashMap::new();
        for r in source_rows {
            let hour: DateTime<Utc> = r.get("hour");
            let count: i64 = r.get("order_count");
            let sum: i64 = r.get("sales_paise");
            source_map.insert(hour, (count, sum));
        }

        // 2. Fetch analytics rollups
        let rollup_rows = sqlx::query(
            r#"
            SELECT 
                hour,
                order_count,
                sales_paise
            FROM agg_hourly
            WHERE tenant_id = $1
              AND hour >= $2 AND hour < $3
              AND status = 'completed'
            "#,
        )
        .bind(tenant_id)
        .bind(from_aligned)
        .bind(to_aligned)
        .fetch_all(&self.analytics_pool)
        .await?;

        let mut rollup_map: HashMap<DateTime<Utc>, (i64, i64)> = HashMap::new();
        for r in rollup_rows {
            let hour: DateTime<Utc> = r.get("hour");
            let count: i64 = r.get("order_count");
            let sum: i64 = r.get("sales_paise");
            rollup_map.insert(hour, (count, sum));
        }

        let mut drift_reports = Vec::new();

        // 3. Compare source against rollup
        for (hour, (src_cnt, src_sum)) in &source_map {
            let (roll_cnt, roll_sum) = rollup_map.get(hour).copied().unwrap_or((0, 0));

            if *src_cnt != roll_cnt || *src_sum != roll_sum {
                warn!(
                    "🚨 Drift detected for tenant {} at {}: source=({}, {}), rollup=({}, {})",
                    tenant_id, hour, src_cnt, src_sum, roll_cnt, roll_sum
                );

                // Self-healing: Repair the bucket in analytics DB
                let mut ana_tx = self.analytics_pool.begin().await?;

                sqlx::query(
                    r#"
                    INSERT INTO agg_hourly (tenant_id, hour, status, order_count, sales_paise)
                    VALUES ($1, $2, 'completed', $3, $4)
                    ON CONFLICT (tenant_id, hour, status)
                    DO UPDATE SET order_count = EXCLUDED.order_count, sales_paise = EXCLUDED.sales_paise
                    "#,
                )
                .bind(tenant_id)
                .bind(*hour)
                .bind(*src_cnt)
                .bind(*src_sum)
                .execute(&mut *ana_tx)
                .await?;

                // Bump bucket state version
                sqlx::query(
                    r#"
                    INSERT INTO bucket_state (tenant_id, hour, published_version, computed_at)
                    VALUES ($1, $2, 1, NOW())
                    ON CONFLICT (tenant_id, hour)
                    DO UPDATE SET published_version = bucket_state.published_version + 1, computed_at = NOW()
                    "#,
                )
                .bind(tenant_id)
                .bind(*hour)
                .execute(&mut *ana_tx)
                .await?;

                // Log audit action
                sqlx::query(
                    "INSERT INTO audit_log (tenant_id, action, details) VALUES ($1, 'RECONCILIATION_AUTO_HEAL', $2)",
                )
                .bind(tenant_id)
                .bind(json!({
                    "bucket_hour": hour.to_rfc3339(),
                    "drift": {
                        "expected_count": src_cnt,
                        "actual_count": roll_cnt,
                        "expected_sum": src_sum,
                        "actual_sum": roll_sum,
                    }
                }))
                .execute(&mut *ana_tx)
                .await?;

                ana_tx.commit().await?;

                info!("✅ Auto-healed drift for tenant {} at {}", tenant_id, hour);

                drift_reports.push(DriftReport {
                    tenant_id,
                    bucket_hour: *hour,
                    source_count: *src_cnt,
                    source_sum: *src_sum,
                    rollup_count: roll_cnt,
                    rollup_sum: roll_sum,
                    repaired: true,
                });
            }
        }

        Ok(drift_reports)
    }
}
