use adapter_mysql::MysqlAdapter;
use chrono::{Duration, TimeZone, Utc};
use platform_core::config::{AppConfig, SourceConfig};
use platform_core::traits::{BucketReader, ChangeSource};
use platform_core::types::BucketKey;
use sqlx::Row;
use std::path::Path;
use uuid::Uuid;

fn get_mysql_config() -> SourceConfig {
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
        .find(|s| s.id == "mysql_source")
        .expect("mysql_source config")
}

#[tokio::test]
async fn test_mysql_outbox_capture_and_recompute() {
    let config = get_mysql_config();
    let adapter = MysqlAdapter::new(config.clone()).await.unwrap();

    // 1. Setup schema & triggers
    adapter.setup_schema().await.unwrap();

    let cursor: Option<i64> = sqlx::query_scalar(&format!("SELECT MAX(id) FROM _{}_outbox", config.mapping.entity))
        .fetch_optional(adapter.pool())
        .await
        .unwrap()
        .flatten();
    let cursor_str = cursor.map(|c| c.to_string());

    let tenant_id = format!("mysql_ten_{}", Uuid::new_v4().simple());
    let sale_id = format!("sale_{}", Uuid::new_v4().simple());
    let hour = Utc.with_ymd_and_hms(2026, 11, 1, 15, 0, 0).unwrap();
    let created_at = (hour + Duration::minutes(20)).naive_utc();

    // 2. Insert order directly into MySQL source table
    let entity = config.mapping.entity.as_str();
    let sql = format!(
        "INSERT INTO {entity} (sale_id, tenant_id, product_id, category, amount_cents, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?);"
    );
    sqlx::query(&sql)
        .bind(&sale_id)
        .bind(&tenant_id)
        .bind("prod_mysql_1")
        .bind("electronics")
        .bind(9900i64)
        .bind("completed")
        .bind(created_at)
        .execute(adapter.pool())
        .await
        .unwrap();

    // 3. Poll outbox from recent cursor
    let batch = adapter.poll(cursor_str).await.unwrap();
    assert!(!batch.bucket_keys.is_empty());
    let expected_key = BucketKey::new(&config.id, &tenant_id, hour);
    assert!(
        batch.bucket_keys.contains(&expected_key),
        "MySQL Outbox must capture inserted bucket key: {:?}",
        expected_key
    );

    // 4. Compute bucket aggregate
    let agg = adapter.compute(&expected_key).await.unwrap();
    assert_eq!(agg.key, expected_key);
    assert_eq!(agg.status_aggregates.len(), 1);
    assert_eq!(agg.status_aggregates[0].status, "completed");
    assert_eq!(agg.status_aggregates[0].order_count, 1);
    assert_eq!(agg.status_aggregates[0].sales_units, 9900);

    assert_eq!(agg.product_aggregates.len(), 1);
    assert_eq!(agg.product_aggregates[0].product_id, "prod_mysql_1");
    assert_eq!(agg.product_aggregates[0].units, 1);
    assert_eq!(agg.product_aggregates[0].sales_units, 9900);

    // 5. Ack outbox batch
    adapter.ack(&batch).await.unwrap();

    // 6. Verify outbox is drained for this batch
    let outbox_check_sql = format!(
        "SELECT COUNT(*) as cnt FROM _{entity}_outbox WHERE tenant_id = ?;"
    );
    let row = sqlx::query(&outbox_check_sql)
        .bind(&tenant_id)
        .fetch_one(adapter.pool())
        .await
        .unwrap();
    let count: i64 = row.get("cnt");
    assert_eq!(count, 0, "MySQL outbox table must be empty after ack");
}

