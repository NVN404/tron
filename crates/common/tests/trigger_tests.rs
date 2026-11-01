use chrono::{DateTime, Duration};
use common::AppConfig;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
async fn test_outbox_trigger_behavior() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let pool = PgPool::connect(&config.source_database_url).await?;

    let tenant_1 = format!("tenant_{}", Uuid::new_v4().simple());
    let tenant_2 = format!("tenant_{}", Uuid::new_v4().simple());
    let prod_id = format!("prod_{}", Uuid::new_v4().simple());

    // --- TEST 1: INSERT generates exactly 1 outbox row with new bucket ---
    let t_10_00 = DateTime::parse_from_rfc3339("2026-05-10T10:15:00Z")?.to_utc();
    let order_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO orders (id, tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, $3, 'Electronics', 'completed', 50000, $4)"
    )
    .bind(order_id)
    .bind(&tenant_1)
    .bind(&prod_id)
    .bind(t_10_00)
    .execute(&pool)
    .await?;

    let outbox_rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT id, tenant_id FROM _orders_outbox WHERE tenant_id = $1"
    )
    .bind(&tenant_1)
    .fetch_all(&pool)
    .await?;

    assert_eq!(outbox_rows.len(), 1, "INSERT must generate exactly 1 outbox row");

    // --- TEST 2: UPDATE amount generates an outbox row ---
    sqlx::query("UPDATE orders SET amount_paise = 60000 WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await?;

    let outbox_after_amt: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM _orders_outbox WHERE tenant_id = $1"
    )
    .bind(&tenant_1)
    .fetch_one(&pool)
    .await?;
    assert_eq!(outbox_after_amt.0, 2, "Updating amount must append a dirty hint for that hour");

    // --- TEST 3: UPDATE created_at across hour boundaries appends BOTH old and new buckets ---
    let t_14_00 = t_10_00 + Duration::hours(4);
    sqlx::query("UPDATE orders SET created_at = $1 WHERE id = $2")
        .bind(t_14_00)
        .bind(order_id)
        .execute(&pool)
        .await?;

    let outbox_after_cross_hour: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM _orders_outbox WHERE tenant_id = $1"
    )
    .bind(&tenant_1)
    .fetch_one(&pool)
    .await?;
    // Previous count was 2; shifting across hours appends 2 hints (NEW bucket and OLD bucket) -> 4 hints
    assert_eq!(outbox_after_cross_hour.0, 4, "Cross-hour update must append both OLD and NEW buckets");

    // --- TEST 4: UPDATE tenant appends hints for BOTH old and new tenants ---
    sqlx::query("UPDATE orders SET tenant_id = $1 WHERE id = $2")
        .bind(&tenant_2)
        .bind(order_id)
        .execute(&pool)
        .await?;

    let t2_outbox: (i64,) = sqlx::query_as("SELECT count(*) FROM _orders_outbox WHERE tenant_id = $1")
        .bind(&tenant_2)
        .fetch_one(&pool)
        .await?;
    assert!(t2_outbox.0 >= 1, "New tenant must receive dirty outbox hint");

    let t1_outbox: (i64,) = sqlx::query_as("SELECT count(*) FROM _orders_outbox WHERE tenant_id = $1")
        .bind(&tenant_1)
        .fetch_one(&pool)
        .await?;
    assert_eq!(t1_outbox.0, 5, "Old tenant must receive dirty outbox hint to recalculate and remove the order");

    // --- TEST 5: DELETE appends OLD bucket hint ---
    sqlx::query("DELETE FROM orders WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await?;

    let t2_after_delete: (i64,) = sqlx::query_as("SELECT count(*) FROM _orders_outbox WHERE tenant_id = $1")
        .bind(&tenant_2)
        .fetch_one(&pool)
        .await?;
    assert_eq!(t2_after_delete.0, 2, "DELETE must append old bucket hint to outbox");

    // --- TEST 6: Rolled-back transaction leaves NO outbox row ---
    let pre_rollback_count: (i64,) = sqlx::query_as("SELECT count(*) FROM _orders_outbox").fetch_one(&pool).await?;
    {
        let mut tx = pool.begin().await?;
        let temp_order_id = Uuid::new_v4();
        sqlx::query("INSERT INTO orders (id, tenant_id, product_id, category, status, amount_paise, created_at) VALUES ($1, $2, $3, 'Electronics', 'completed', 12345, NOW())")
            .bind(temp_order_id)
            .bind(&tenant_1)
            .bind(&prod_id)
            .execute(&mut *tx)
            .await?;
        tx.rollback().await?;
    }
    let post_rollback_count: (i64,) = sqlx::query_as("SELECT count(*) FROM _orders_outbox").fetch_one(&pool).await?;
    assert_eq!(pre_rollback_count.0, post_rollback_count.0, "Aborted transaction must roll back outbox rows!");

    println!("✅ All outbox trigger and change capture tests passed!");
    Ok(())
}
