use adapter_postgres::PostgresAdapter;
use chrono::{Duration, TimeZone, Utc};
use platform_core::config::{AppConfig, SourceConfig};
use platform_core::traits::{BucketReader, ChangeSource};
use platform_core::types::BucketKey;
use sqlx::Row;
use std::path::Path;
use uuid::Uuid;

fn get_postgres_config() -> SourceConfig {
    dotenvy::dotenv().ok();
    let dir = if Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    let app_config = AppConfig::load_from_dir(dir, Some("test")).expect("load config");
    app_config
        .sources
        .into_iter()
        .find(|s| s.id == "pg_source_1")
        .expect("pg_source_1 config")
}

#[tokio::test]
async fn test_postgres_outbox_capture_and_recompute() {
    let config = get_postgres_config();
    let adapter = PostgresAdapter::new(config.clone()).await.unwrap();

    // 1. Setup schema & triggers
    adapter.setup_schema().await.unwrap();

    let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
    let hour = Utc.with_ymd_and_hms(2026, 11, 1, 14, 0, 0).unwrap();
    let created_at = hour + Duration::minutes(15);
    let order_id = Uuid::new_v4();

    // 2. Insert order directly into Postgres source table
    let entity = config.mapping.entity.as_str();
    let sql = format!(
        "INSERT INTO {entity} (id, tenant_id, product_id, category, amount_paise, status, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7);"
    );
    sqlx::query(&sql)
        .bind(order_id)
        .bind(&tenant_id)
        .bind("prod_pg_1")
        .bind("books")
        .bind(1500i64)
        .bind("completed")
        .bind(created_at)
        .execute(adapter.pool())
        .await
        .unwrap();

    // 3. Poll outbox
    let batch = adapter.poll(None).await.unwrap();
    assert!(!batch.bucket_keys.is_empty());
    let expected_key = BucketKey::new(&config.id, &tenant_id, hour);
    assert!(
        batch.bucket_keys.contains(&expected_key),
        "Outbox must capture inserted bucket key: {:?}",
        expected_key
    );

    // 4. Compute bucket aggregate
    let agg = adapter.compute(&expected_key).await.unwrap();
    assert_eq!(agg.key, expected_key);
    assert_eq!(agg.status_aggregates.len(), 1);
    assert_eq!(agg.status_aggregates[0].status, "completed");
    assert_eq!(agg.status_aggregates[0].order_count, 1);
    assert_eq!(agg.status_aggregates[0].sales_units, 1500);

    assert_eq!(agg.product_aggregates.len(), 1);
    assert_eq!(agg.product_aggregates[0].product_id, "prod_pg_1");
    assert_eq!(agg.product_aggregates[0].units, 1);
    assert_eq!(agg.product_aggregates[0].sales_units, 1500);

    // 5. Ack outbox batch
    adapter.ack(&batch).await.unwrap();

    // 6. Verify outbox is drained for this batch
    let outbox_check_sql = format!(
        "SELECT COUNT(*) as cnt FROM _{entity}_outbox WHERE tenant_id = $1;"
    );
    let row = sqlx::query(&outbox_check_sql)
        .bind(&tenant_id)
        .fetch_one(adapter.pool())
        .await
        .unwrap();
    let count: i64 = row.get("cnt");
    assert_eq!(count, 0, "Outbox table must be empty after ack");
}

