use api::self_check::SecuritySelfCheck;
use chrono::Utc;
use common::AppConfig;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn test_rls_isolation_and_security_guarantees() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;

    // Admin pool (superuser) used only to set up test seed data
    let admin_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.analytics_database_url)
        .await?;

    // Restricted api_ro pool (unprivileged role) used by the API
    let api_ro_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.analytics_ro_database_url)
        .await?;

    // Seed two independent tenants and test rollup rows
    let tenant_a = Uuid::new_v4();
    let tenant_b = Uuid::new_v4();
    let now = Utc::now();

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Tenant-RLS-A'), ($2, 'Tenant-RLS-B')")
        .bind(tenant_a)
        .bind(tenant_b)
        .execute(&admin_pool)
        .await?;

    // Insert 3 rollups for Tenant A and 2 rollups for Tenant B
    sqlx::query(
        "INSERT INTO agg_hourly (tenant_id, hour, status, order_count, sales_paise) VALUES \
         ($1, $3, 'completed', 10, 100000), \
         ($1, $3, 'pending', 2, 20000), \
         ($1, $3, 'cancelled', 1, 10000), \
         ($2, $3, 'completed', 50, 500000), \
         ($2, $3, 'refunded', 5, 50000)"
    )
    .bind(tenant_a)
    .bind(tenant_b)
    .bind(now)
    .execute(&admin_pool)
    .await?;

    // --- PROOF 1: api_ro with NO tenant context returns ZERO rows (Fail-Closed) ---
    {
        let mut tx = api_ro_pool.begin().await?;
        let count: (i64,) = sqlx::query_as("SELECT count(*) FROM agg_hourly")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(count.0, 0, "api_ro without tenant context MUST return 0 rows, never all rows!");
        tx.rollback().await?;
    }

    // --- PROOF 2: api_ro with Tenant A context returns Tenant A's rows, ZERO of Tenant B ---
    {
        let mut tx = api_ro_pool.begin().await?;
        // Set transaction-scoped tenant context
        sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
            .bind(tenant_a.to_string())
            .execute(&mut *tx)
            .await?;

        // Query all rows from agg_hourly
        let rows: Vec<(Uuid, String, i64)> = sqlx::query_as(
            "SELECT tenant_id, status, order_count FROM agg_hourly"
        )
        .fetch_all(&mut *tx)
        .await?;

        assert_eq!(rows.len(), 3, "Tenant A should see exactly its 3 rollup rows");
        for row in rows {
            assert_eq!(row.0, tenant_a, "Every returned row MUST belong to Tenant A");
        }

        // Adversarial query: explicitly filter for Tenant B's data
        let b_rows: Vec<(Uuid,)> = sqlx::query_as(
            "SELECT tenant_id FROM agg_hourly WHERE tenant_id = $1"
        )
        .bind(tenant_b)
        .fetch_all(&mut *tx)
        .await?;

        assert!(b_rows.is_empty(), "Even explicit WHERE tenant_id = Tenant_B must return 0 rows under Tenant A's RLS policy");
        tx.rollback().await?;
    }

    // --- PROOF 3: api_ro with Tenant B context returns Tenant B's rows, ZERO of Tenant A ---
    {
        let mut tx = api_ro_pool.begin().await?;
        sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
            .bind(tenant_b.to_string())
            .execute(&mut *tx)
            .await?;

        let rows: Vec<(Uuid, String, i64)> = sqlx::query_as(
            "SELECT tenant_id, status, order_count FROM agg_hourly"
        )
        .fetch_all(&mut *tx)
        .await?;

        assert_eq!(rows.len(), 2, "Tenant B should see exactly its 2 rollup rows");
        for row in rows {
            assert_eq!(row.0, tenant_b, "Every returned row MUST belong to Tenant B");
        }
        tx.rollback().await?;
    }

    // --- PROOF 4: Security Startup Self-Check on api_ro PASSES ---
    let self_check_res = SecuritySelfCheck::run(&api_ro_pool).await;
    assert!(self_check_res.is_ok(), "Security self check must pass on properly configured api_ro role");

    // --- PROOF 5: Security Startup Self-Check on Superuser FAILS CLOSED ---
    let superuser_check = SecuritySelfCheck::run(&admin_pool).await;
    assert!(superuser_check.is_err(), "Security self check MUST FAIL when run as superuser!");
    let err_msg = superuser_check.unwrap_err().to_string();
    assert!(err_msg.contains("SUPERUSER"), "Error message must report superuser violation: {}", err_msg);

    println!("✅ All Phase 4 RLS isolation and security self-check tests passed!");
    Ok(())
}

