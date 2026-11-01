use adapter_mongo::MongoAdapter;
use adapter_mysql::MysqlAdapter;
use adapter_postgres::PostgresAdapter;
use analytics_store::AnalyticsStore;
use capture::CaptureEngine;
use chrono::{Duration, TimeZone, Utc};
use platform_core::AppConfig;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::path::Path;
use uuid::Uuid;
use worker::WorkerEngine;

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
async fn test_heterogeneous_capture_and_worker_pipeline() {
    let config = load_test_config();
    let analytics_dsn = config.analytics.resolve_dsn().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&analytics_dsn)
        .await
        .unwrap();

    let store = AnalyticsStore::new(pool.clone());
    store.register_sources(&config).await.unwrap();

    // 1. Setup adapters and schemas
    let pg_cfg = config.sources.iter().find(|s| s.id == "pg_source_1").unwrap();
    let pg_adapter = PostgresAdapter::new(pg_cfg.clone()).await.unwrap();
    pg_adapter.setup_schema().await.unwrap();

    let mysql_cfg = config.sources.iter().find(|s| s.id == "mysql_source").unwrap();
    let mysql_adapter = MysqlAdapter::new(mysql_cfg.clone()).await.unwrap();
    mysql_adapter.setup_schema().await.unwrap();

    let mongo_cfg = config.sources.iter().find(|s| s.id == "mongo_source").unwrap();
    let mongo_adapter = MongoAdapter::new(mongo_cfg.clone()).await.unwrap();
    mongo_adapter.setup_schema().await.unwrap();

    // Advance source_cursors to current max outbox id so this test isolates newly generated events
    let pg_max: Option<i64> = sqlx::query_scalar("SELECT MAX(id) FROM _orders_outbox")
        .fetch_optional(pg_adapter.pool())
        .await
        .unwrap()
        .flatten();
    if let Some(max_id) = pg_max {
        sqlx::query("INSERT INTO source_cursors (source_id, cursor_value, updated_at) VALUES ('pg_source_1', $1, NOW()) ON CONFLICT (source_id) DO UPDATE SET cursor_value = $1;")
            .bind(max_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
    }

    let mysql_max: Option<i64> = sqlx::query_scalar("SELECT MAX(id) FROM _sales_outbox")
        .fetch_optional(mysql_adapter.pool())
        .await
        .unwrap()
        .flatten();
    if let Some(max_id) = mysql_max {
        sqlx::query("INSERT INTO source_cursors (source_id, cursor_value, updated_at) VALUES ('mysql_source', $1, NOW()) ON CONFLICT (source_id) DO UPDATE SET cursor_value = $1;")
            .bind(max_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
    }

    // 2. Insert records into each heterogeneous source
    let hour = Utc.with_ymd_and_hms(2026, 11, 1, 18, 0, 0).unwrap();
    let event_time = hour + Duration::minutes(10);

    // Postgres record
    let pg_tenant = format!("pg_pipe_{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO orders (id, tenant_id, product_id, category, amount_paise, status, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7);"
    )
    .bind(Uuid::new_v4())
    .bind(&pg_tenant)
    .bind("prod_pg")
    .bind("books")
    .bind(2500i64)
    .bind("completed")
    .bind(event_time)
    .execute(pg_adapter.pool())
    .await
    .unwrap();

    // MySQL record
    let mysql_tenant = format!("my_pipe_{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO sales (sale_id, tenant_id, product_id, category, amount_cents, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?);"
    )
    .bind(format!("sale_{}", Uuid::new_v4().simple()))
    .bind(&mysql_tenant)
    .bind("prod_mysql")
    .bind("gadgets")
    .bind(7500i64)
    .bind("completed")
    .bind(event_time.naive_utc())
    .execute(mysql_adapter.pool())
    .await
    .unwrap();

    // 3. Run Capture Engine to poll and commit dirty buckets to Analytics PG
    let capture_engine = CaptureEngine::from_config(&config, store.clone()).await.unwrap();
    let captured_pg = capture_engine.run_once("pg_source_1").await.unwrap();
    assert!(captured_pg >= 1, "Must capture at least 1 dirty bucket from Postgres");

    let captured_mysql = capture_engine.run_once("mysql_source").await.unwrap();
    assert!(captured_mysql >= 1, "Must capture at least 1 dirty bucket from MySQL");

    // 4. Run Worker Engine to claim, recompute consistent snapshots, and publish
    let worker_engine = WorkerEngine::from_config(&config, store.clone()).await.unwrap();
    let processed = worker_engine.drain_all().await.unwrap();
    assert!(processed >= 2, "Worker must process all claimed dirty buckets");

    // 5. Verify published rollups in Analytics PG
    // Check Postgres source tenant
    let pg_uuid = store.get_or_create_tenant("pg_source_1", &pg_tenant).await.unwrap();
    let pg_row = sqlx::query(
        "SELECT order_count, total_amount_minor FROM agg_hourly_status WHERE tenant_uuid = $1;"
    )
    .bind(pg_uuid)
    .fetch_one(&pool)
    .await
    .unwrap();
    let pg_count: i64 = pg_row.get("order_count");
    let pg_total: i64 = pg_row.get("total_amount_minor");
    assert_eq!(pg_count, 1);
    assert_eq!(pg_total, 2500);

    // Check MySQL source tenant
    let mysql_uuid = store.get_or_create_tenant("mysql_source", &mysql_tenant).await.unwrap();
    let my_row = sqlx::query(
        "SELECT order_count, total_amount_minor FROM agg_hourly_status WHERE tenant_uuid = $1;"
    )
    .bind(mysql_uuid)
    .fetch_one(&pool)
    .await
    .unwrap();
    let my_count: i64 = my_row.get("order_count");
    let my_total: i64 = my_row.get("total_amount_minor");
    assert_eq!(my_count, 1);
    assert_eq!(my_total, 7500);

    // 6. Verify dirty_buckets is fully drained for these tenants
    let remaining_dirty = sqlx::query(
        "SELECT COUNT(*) as cnt FROM dirty_buckets WHERE tenant_id IN ($1, $2);"
    )
    .bind(&pg_tenant)
    .bind(&mysql_tenant)
    .fetch_one(&pool)
    .await
    .unwrap();
    let cnt: i64 = remaining_dirty.get("cnt");
    assert_eq!(cnt, 0, "All processed dirty buckets must be removed from dirty_buckets");
}
