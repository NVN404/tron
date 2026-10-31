use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, QueryBuilder, Row};
use std::collections::HashSet;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

pub struct BackfillPlan {
    pub tenant_id: Uuid,
    pub order_count: i64,
}

struct StatusRecord {
    hour: DateTime<Utc>,
    status: String,
    order_count: i64,
    sales_paise: i64,
}

struct ProductRecord {
    hour: DateTime<Utc>,
    product_id: Uuid,
    category: String,
    units: i64,
    sales_paise: i64,
}

pub struct BackfillRunner {
    source_pool: PgPool,
    analytics_pool: PgPool,
}

impl BackfillRunner {
    pub fn new(source_pool: PgPool, analytics_pool: PgPool) -> Self {
        Self {
            source_pool,
            analytics_pool,
        }
    }

    /// Discover tenants sorted by order count descending (largest tenants first)
    pub async fn plan_tenants(&self, target_tenant: Option<Uuid>) -> Result<Vec<BackfillPlan>> {
        let plans: Vec<BackfillPlan> = if let Some(tid) = target_tenant {
            let count: (i64,) = sqlx::query_as(
                "SELECT count(*) FROM orders WHERE tenant_id = $1"
            )
            .bind(tid)
            .fetch_one(&self.source_pool)
            .await?;
            vec![BackfillPlan { tenant_id: tid, order_count: count.0 }]
        } else {
            let rows = sqlx::query(
                "SELECT tenant_id, COUNT(*) as cnt FROM orders GROUP BY tenant_id ORDER BY cnt DESC"
            )
            .fetch_all(&self.source_pool)
            .await?;

            rows.into_iter()
                .map(|r| BackfillPlan {
                    tenant_id: r.get("tenant_id"),
                    order_count: r.get("cnt"),
                })
                .collect()
        };

        Ok(plans)
    }

    /// Run chunked backfill for a tenant across [from, to)
    pub async fn backfill_tenant(
        &self,
        tenant_id: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        chunk_days: i64,
    ) -> Result<(u64, i64)> {
        let mut total_processed_rows = 0u64;
        let mut max_bucket_orders = 0i64;

        // Ensure tenant exists in analytics
        sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Backfilled Tenant') ON CONFLICT (id) DO NOTHING")
            .bind(tenant_id)
            .execute(&self.analytics_pool)
            .await?;

        // Strictly align boundaries to IST hourly boundaries (+1800s offset from UTC hour)
        use chrono::TimeZone;
        let mut current_start = Utc.timestamp_opt(((from.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();
        let to_aligned = Utc.timestamp_opt(((to.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();

        while current_start < to_aligned {
            let next_chunk = current_start + Duration::days(chunk_days);
            let current_end = if next_chunk < to_aligned {
                next_chunk
            } else {
                to_aligned
            };

            // 1. Snapshot read on source: multi-hour GROUP BY in a single query
            let mut src_tx = self.source_pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").execute(&mut *src_tx).await?;

            let raw_status = sqlx::query(
                r#"
                SELECT 
                    date_trunc('hour', created_at AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata' AS hour,
                    status,
                    COUNT(*)::BIGINT AS order_count,
                    COALESCE(SUM(amount_paise), 0)::BIGINT AS sales_paise
                FROM orders
                WHERE tenant_id = $1
                  AND created_at >= $2 AND created_at < $3
                  AND deleted_at IS NULL
                GROUP BY 1, 2
                "#
            )
            .bind(tenant_id)
            .bind(current_start)
            .bind(current_end)
            .fetch_all(&mut *src_tx)
            .await?;

            let raw_prods = sqlx::query(
                r#"
                SELECT 
                    date_trunc('hour', created_at AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata' AS hour,
                    product_id,
                    category,
                    COUNT(*)::BIGINT AS units,
                    COALESCE(SUM(amount_paise), 0)::BIGINT AS sales_paise
                FROM orders
                WHERE tenant_id = $1
                  AND created_at >= $2 AND created_at < $3
                  AND deleted_at IS NULL
                  AND status = 'completed'
                GROUP BY 1, 2, 3
                "#
            )
            .bind(tenant_id)
            .bind(current_start)
            .bind(current_end)
            .fetch_all(&mut *src_tx)
            .await?;

            src_tx.commit().await?;

            if raw_status.is_empty() && raw_prods.is_empty() {
                current_start = current_end;
                continue;
            }

            // 2. Identify existing live versions (>0) in analytics so live writes always win!
            let mut ana_tx = self.analytics_pool.begin().await?;

            let live_hours: HashSet<DateTime<Utc>> = sqlx::query_scalar(
                "SELECT hour FROM bucket_state WHERE tenant_id = $1 AND hour >= $2 AND hour < $3 AND published_version > 0"
            )
            .bind(tenant_id)
            .bind(current_start)
            .bind(current_end)
            .fetch_all(&mut *ana_tx)
            .await?
            .into_iter()
            .collect();

            // Filter out hours that have active live versions
            let mut status_records = Vec::new();
            for r in raw_status {
                let hour: DateTime<Utc> = r.get("hour");
                let order_count: i64 = r.get("order_count");
                if order_count > max_bucket_orders {
                    max_bucket_orders = order_count;
                }
                total_processed_rows += order_count as u64;

                if !live_hours.contains(&hour) {
                    status_records.push(StatusRecord {
                        hour,
                        status: r.get("status"),
                        order_count,
                        sales_paise: r.get("sales_paise"),
                    });
                }
            }

            let mut prod_records = Vec::new();
            for r in raw_prods {
                let hour: DateTime<Utc> = r.get("hour");
                if !live_hours.contains(&hour) {
                    prod_records.push(ProductRecord {
                        hour,
                        product_id: r.get("product_id"),
                        category: r.get("category"),
                        units: r.get("units"),
                        sales_paise: r.get("sales_paise"),
                    });
                }
            }

            // 3. Batch insert agg_hourly in chunks of 500
            for chunk in status_records.chunks(500) {
                let mut qb = QueryBuilder::new("INSERT INTO agg_hourly (tenant_id, hour, status, order_count, sales_paise) ");
                qb.push_values(chunk, |mut b, rec| {
                    b.push_bind(tenant_id)
                        .push_bind(rec.hour)
                        .push_bind(&rec.status)
                        .push_bind(rec.order_count)
                        .push_bind(rec.sales_paise);
                });
                qb.push(" ON CONFLICT (tenant_id, hour, status) DO UPDATE SET order_count = EXCLUDED.order_count, sales_paise = EXCLUDED.sales_paise");
                qb.build().execute(&mut *ana_tx).await?;
            }

            // 4. Batch insert agg_hourly_product in chunks of 500
            for chunk in prod_records.chunks(500) {
                let mut qb = QueryBuilder::new("INSERT INTO agg_hourly_product (tenant_id, hour, product_id, category, units, sales_paise) ");
                qb.push_values(chunk, |mut b, rec| {
                    b.push_bind(tenant_id)
                        .push_bind(rec.hour)
                        .push_bind(rec.product_id)
                        .push_bind(&rec.category)
                        .push_bind(rec.units)
                        .push_bind(rec.sales_paise);
                });
                qb.push(" ON CONFLICT (tenant_id, hour, product_id) DO UPDATE SET units = EXCLUDED.units, sales_paise = EXCLUDED.sales_paise");
                qb.build().execute(&mut *ana_tx).await?;
            }

            // 5. Batch insert bucket_state (version = 0)
            let unique_hours: HashSet<DateTime<Utc>> = status_records.iter().map(|s| s.hour).collect();
            let hours_vec: Vec<DateTime<Utc>> = unique_hours.into_iter().collect();
            for chunk in hours_vec.chunks(500) {
                let mut qb = QueryBuilder::new("INSERT INTO bucket_state (tenant_id, hour, published_version, computed_at) ");
                qb.push_values(chunk, |mut b, hour| {
                    b.push_bind(tenant_id)
                        .push_bind(hour)
                        .push_bind(0i64)
                        .push_bind(Utc::now());
                });
                qb.push(" ON CONFLICT (tenant_id, hour) DO NOTHING");
                qb.build().execute(&mut *ana_tx).await?;
            }

            // 6. Record checkpoint
            sqlx::query(
                "INSERT INTO checkpoints (id, last_processed_id, updated_at) VALUES ($1, 0, NOW()) \
                 ON CONFLICT (id) DO UPDATE SET updated_at = NOW()"
            )
            .bind(format!("backfill-{}", tenant_id))
            .execute(&mut *ana_tx)
            .await?;

            ana_tx.commit().await?;
            current_start = current_end;
        }

        Ok((total_processed_rows, max_bucket_orders))
    }

    /// Rebuild rollups: truncates analytics tables and executes full backfill
    pub async fn rebuild(&self, target_tenant: Option<Uuid>, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<()> {
        info!("Initiating rebuild: truncating existing analytics rollups...");
        if let Some(tid) = target_tenant {
            sqlx::query("DELETE FROM agg_hourly WHERE tenant_id = $1").bind(tid).execute(&self.analytics_pool).await?;
            sqlx::query("DELETE FROM agg_hourly_product WHERE tenant_id = $1").bind(tid).execute(&self.analytics_pool).await?;
            sqlx::query("DELETE FROM bucket_state WHERE tenant_id = $1").bind(tid).execute(&self.analytics_pool).await?;
        } else {
            sqlx::query("TRUNCATE TABLE agg_hourly, agg_hourly_product, bucket_state CASCADE")
                .execute(&self.analytics_pool)
                .await?;
        }

        let plans = self.plan_tenants(target_tenant).await?;
        info!("Executing backfill across {} tenants (largest first)...", plans.len());

        let start = Instant::now();
        let mut total_orders = 0u64;
        let mut worst_case_bucket = 0i64;

        for (idx, p) in plans.iter().enumerate() {
            let t_start = Instant::now();
            let (orders, max_b) = self.backfill_tenant(p.tenant_id, from, to, 30).await?;
            total_orders += orders;
            if max_b > worst_case_bucket {
                worst_case_bucket = max_b;
            }

            let pct = ((idx + 1) as f64 / plans.len() as f64) * 100.0;
            let rps = total_orders as f64 / start.elapsed().as_secs_f64().max(0.001);
            info!(
                "Tenant #{}/{} ({:.1}%) {} => {} orders ({:.2}s) | Running speed: {:.0} rows/sec",
                idx + 1, plans.len(), pct, p.tenant_id, orders, t_start.elapsed().as_secs_f64(), rps
            );
        }

        info!(
            "🎉 Rebuild complete: {} total orders backfilled in {:.2}s ({:.0} rows/sec). Worst-case hourly bucket size: {} orders",
            total_orders, start.elapsed().as_secs_f64(), total_orders as f64 / start.elapsed().as_secs_f64().max(0.001), worst_case_bucket
        );

        Ok(())
    }
}
