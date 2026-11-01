use chrono::{TimeZone, Utc};
use platform_core::AppConfig;
use sqlx::postgres::PgPoolOptions;
use std::path::Path;
use uuid::Uuid;

fn load_test_config() -> AppConfig {
    dotenvy::dotenv().ok();
    let dir = if Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    AppConfig::load_from_dir(dir, Some("test")).expect("Failed to load test config")
}

#[tokio::test]
async fn test_rls_isolation_and_security_guarantees() -> anyhow::Result<()> {
    let config = load_test_config();
    let admin_dsn = config.analytics.resolve_dsn()?;
    let ro_dsn = config.analytics.resolve_ro_dsn()?;

    // Admin pool (superuser) used only to set up test seed data
    let admin_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&admin_dsn)
        .await?;

    // Restricted api_ro pool (unprivileged role) used by the API
    let api_ro_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&ro_dsn)
        .await?;

    // Seed two independent tenants and test rollup rows
    let tenant_a = Uuid::new_v4();
    let tenant_b = Uuid::new_v4();
    let now = Utc.timestamp_opt((Utc::now().timestamp() / 3600) * 3600, 0).unwrap();

    sqlx::query("INSERT INTO sources (id, kind, capture_mode) VALUES ('pg_source_1', 'postgres', 'outbox') ON CONFLICT DO NOTHING;")
        .execute(&admin_pool)
        .await?;

    sqlx::query(
        "INSERT INTO tenants (tenant_uuid, source_id, external_id, name) VALUES ($1, 'pg_source_1', $3, 'Tenant-RLS-A'), ($2, 'pg_source_1', $4, 'Tenant-RLS-B')",
    )
    .bind(tenant_a)
    .bind(tenant_b)
    .bind(format!("ext_{}", tenant_a))
    .bind(format!("ext_{}", tenant_b))
    .execute(&admin_pool)
    .await?;

    // Insert rollups for Tenant A and Tenant B
    sqlx::query(
        "INSERT INTO agg_hourly_status (tenant_uuid, source_id, hour, status, order_count, total_amount_minor) VALUES \
         ($1, 'pg_source_1', $3, 'completed', 10, 100000), \
         ($1, 'pg_source_1', $3, 'pending', 2, 20000), \
         ($2, 'pg_source_1', $3, 'completed', 50, 500000) \
         ON CONFLICT DO NOTHING;"
    )
    .bind(tenant_a)
    .bind(tenant_b)
    .bind(now)
    .execute(&admin_pool)
    .await?;

    // --- PROOF 1: api_ro with NO tenant context returns ZERO rows (Fail-Closed) ---
    {
        let mut tx = api_ro_pool.begin().await?;
        let rows: (i64,) = sqlx::query_as("SELECT count(*) FROM agg_hourly_status")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(
            rows.0, 0,
            "CRITICAL SECURITY FAIL: api_ro read {} rows without app.tenant_id set!",
            rows.0
        );
        tx.rollback().await?;
    }

    // --- PROOF 2: api_ro with Tenant A context sees ONLY Tenant A rows ---
    {
        let mut tx = api_ro_pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", tenant_a))
            .execute(&mut *tx)
            .await?;

        let count: (i64,) = sqlx::query_as("SELECT count(*) FROM agg_hourly_status")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(count.0, 2, "Tenant A should see exactly 2 rollup rows");

        let sum: (i64,) = sqlx::query_as("SELECT COALESCE(SUM(total_amount_minor), 0)::BIGINT FROM agg_hourly_status")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(sum.0, 120000, "Tenant A should see 120,000 total");
        tx.rollback().await?;
    }

    // --- PROOF 3: api_ro with Tenant B context sees ONLY Tenant B rows ---
    {
        let mut tx = api_ro_pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", tenant_b))
            .execute(&mut *tx)
            .await?;

        let count: (i64,) = sqlx::query_as("SELECT count(*) FROM agg_hourly_status")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(count.0, 1, "Tenant B should see exactly 1 rollup row");

        let sum: (i64,) = sqlx::query_as("SELECT COALESCE(SUM(total_amount_minor), 0)::BIGINT FROM agg_hourly_status")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(sum.0, 500000, "Tenant B should see 500,000 total");
        tx.rollback().await?;
    }

    Ok(())
}
