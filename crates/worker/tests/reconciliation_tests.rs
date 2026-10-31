use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use common::AppConfig;
use sqlx::{PgPool, Row};
use uuid::Uuid;
use worker::Reconciler;

#[tokio::test]
async fn test_reconciliation_drift_detection_and_self_healing() -> Result<()> {
    let config = AppConfig::from_env()?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    let reconciler = Reconciler::new(source_pool.clone(), analytics_pool.clone());

    // Top tenant from 10M dataset
    let tenant_id = Uuid::parse_str("00000000-0000-0001-0000-000000000001")?;

    // 1. Fetch an existing legitimate bucket from agg_hourly
    let legit_row = sqlx::query(
        "SELECT hour, order_count, sales_paise FROM agg_hourly WHERE tenant_id = $1 AND status = 'completed' AND order_count > 10 LIMIT 1"
    )
    .bind(tenant_id)
    .fetch_one(&analytics_pool)
    .await?;

    let target_hour: DateTime<Utc> = legit_row.get("hour");
    let original_count: i64 = legit_row.get("order_count");
    let original_sales: i64 = legit_row.get("sales_paise");

    // 2. Inject artificial data corruption into agg_hourly
    let corrupted_count = original_count + 7777;
    let corrupted_sales = original_sales + 888888;

    sqlx::query(
        "UPDATE agg_hourly SET order_count = $1, sales_paise = $2 WHERE tenant_id = $3 AND hour = $4 AND status = 'completed'"
    )
    .bind(corrupted_count)
    .bind(corrupted_sales)
    .bind(tenant_id)
    .bind(target_hour)
    .execute(&analytics_pool)
    .await?;

    // Verify corruption was applied
    let corrupted_check: (i64,) = sqlx::query_as(
        "SELECT order_count FROM agg_hourly WHERE tenant_id = $1 AND hour = $2 AND status = 'completed'"
    )
    .bind(tenant_id)
    .bind(target_hour)
    .fetch_one(&analytics_pool)
    .await?;
    assert_eq!(corrupted_check.0, corrupted_count);

    // 3. Run Reconciler over the window containing target_hour
    let window_start = target_hour - Duration::hours(1);
    let window_end = target_hour + Duration::hours(2);

    let drifts = reconciler
        .reconcile_window(tenant_id, window_start, window_end)
        .await?;

    // 4. Verify drift was detected and repaired
    assert_eq!(drifts.len(), 1, "Expected exactly 1 corrupted bucket to be detected");
    let drift = &drifts[0];
    assert_eq!(drift.bucket_hour, target_hour);
    assert_eq!(drift.rollup_count, corrupted_count);
    assert_eq!(drift.source_count, original_count);
    assert!(drift.repaired);

    // 5. Verify database rollup was self-healed
    let healed_row = sqlx::query(
        "SELECT order_count, sales_paise FROM agg_hourly WHERE tenant_id = $1 AND hour = $2 AND status = 'completed'"
    )
    .bind(tenant_id)
    .bind(target_hour)
    .fetch_one(&analytics_pool)
    .await?;

    let healed_count: i64 = healed_row.get("order_count");
    let healed_sales: i64 = healed_row.get("sales_paise");

    assert_eq!(healed_count, original_count, "Rollup order count must be restored to source truth");
    assert_eq!(healed_sales, original_sales, "Rollup sales paise must be restored to source truth");

    // 6. Verify audit log entry
    let audit_count: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM audit_log WHERE tenant_id = $1 AND action = 'RECONCILIATION_AUTO_HEAL'"
    )
    .bind(tenant_id)
    .fetch_one(&analytics_pool)
    .await?;

    assert!(audit_count.0 >= 1, "Audit log must record self-healing action");

    Ok(())
}
