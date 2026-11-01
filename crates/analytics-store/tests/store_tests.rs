use analytics_store::{AnalyticsStore, ClaimedBucket, PublishResult};
use chrono::{TimeZone, Utc};
use platform_core::types::{BucketAggregate, BucketKey, ProductAggregate, StatusAggregate};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::env;
use uuid::Uuid;

async fn get_admin_pool() -> sqlx::PgPool {
    dotenvy::dotenv().ok();
    let dsn = env::var("ANALYTICS_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5434/analytics_db".to_string());
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("Failed to connect to analytics DB as admin")
}

async fn get_api_ro_pool() -> sqlx::PgPool {
    dotenvy::dotenv().ok();
    let dsn = env::var("ANALYTICS_RO_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://api_ro:api_ro_secret@localhost:5434/analytics_db".to_string());
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("Failed to connect to analytics DB as api_ro")
}

#[tokio::test]
async fn test_dirty_bucket_upsert_claim_and_heartbeat() {
    let pool = get_admin_pool().await;
    let store = AnalyticsStore::new(pool.clone());
    let src_id = format!("src_{}", Uuid::new_v4().simple());

    // Ensure source exists
    sqlx::query(
        "INSERT INTO sources (id, kind, capture_mode) VALUES ($1, 'postgres', 'outbox');"
    )
    .bind(&src_id)
    .execute(&pool)
    .await
    .unwrap();

    let hour = Utc.with_ymd_and_hms(2026, 11, 1, 10, 0, 0).unwrap();
    let key1 = BucketKey::new(&src_id, "ext_tenant_1", hour);
    let key2 = BucketKey::new(&src_id, "ext_tenant_2", hour);

    // 1. Upsert dirty buckets
    store
        .upsert_dirty_and_cursor(&src_id, &[key1.clone(), key2.clone()], Some(&"cursor_100".to_string()))
        .await
        .expect("upsert failed");

    // Check cursor
    let cur = store.get_source_cursor(&src_id).await.unwrap();
    assert_eq!(cur, Some("cursor_100".to_string()));

    // 2. Claim buckets (1 of 2)
    let claimed = store.claim_buckets(Some(&src_id), "worker_1", 1, 30).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let first = &claimed[0];
    assert!(first.dirty_seq > 0);
    assert!(!first.claim_token.is_empty());

    // 3. Heartbeat
    let extended = store.heartbeat(&first.claim_token, 60).await.unwrap();
    assert_eq!(extended, 1);

    // 4. Claim remaining (1 remaining)
    let claimed2 = store.claim_buckets(Some(&src_id), "worker_1", 10, 30).await.unwrap();
    assert_eq!(claimed2.len(), 1);
}

#[tokio::test]
async fn test_version_fenced_publishing_and_stale_rejection() {
    let pool = get_admin_pool().await;
    let store = AnalyticsStore::new(pool.clone());
    let src_id = format!("src_{}", Uuid::new_v4().simple());

    let hour = Utc.with_ymd_and_hms(2026, 11, 1, 11, 0, 0).unwrap();
    let key = BucketKey::new(&src_id, "ext_tenant_publish", hour);

    // Ensure source exists
    sqlx::query(
        "INSERT INTO sources (id, kind, capture_mode) VALUES ($1, 'postgres', 'outbox');"
    )
    .bind(&src_id)
    .execute(&pool)
    .await
    .unwrap();

    // Upsert dirty
    store.upsert_dirty_and_cursor(&src_id, &[key.clone()], None).await.unwrap();
    let claimed = store.claim_buckets(Some(&src_id), "worker_vfence", 1, 30).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let claim = &claimed[0];

    let aggregate = BucketAggregate {
        key: key.clone(),
        status_aggregates: vec![
            StatusAggregate {
                status: "completed".to_string(),
                order_count: 5,
                sales_units: 50000,
            },
            StatusAggregate {
                status: "pending".to_string(),
                order_count: 2,
                sales_units: 20000,
            },
        ],
        product_aggregates: vec![
            ProductAggregate {
                product_id: "prod_1".to_string(),
                category: "electronics".to_string(),
                units: 5,
                sales_units: 50000,
            },
        ],
    };

    // First publish succeeds
    let res = store.publish_bucket(claim, &aggregate).await.unwrap();
    assert_eq!(res, PublishResult::Published { published_seq: claim.dirty_seq });

    // Attempting to re-publish with the same or older dirty_seq must be rejected as STALE
    let stale_claim = ClaimedBucket {
        key: key.clone(),
        dirty_seq: claim.dirty_seq - 1,
        claim_token: claim.claim_token.clone(),
        attempts: 1,
    };
    let stale_res = store.publish_bucket(&stale_claim, &aggregate).await.unwrap();
    assert!(matches!(stale_res, PublishResult::Stale { .. }));
}

#[tokio::test]
async fn test_rls_security_enforcement() {
    let admin_pool = get_admin_pool().await;
    let store = AnalyticsStore::new(admin_pool.clone());
    let src_id = format!("src_{}", Uuid::new_v4().simple());

    // Register test source
    sqlx::query(
        "INSERT INTO sources (id, kind, capture_mode) VALUES ($1, 'postgres', 'outbox');"
    )
    .bind(&src_id)
    .execute(&admin_pool)
    .await
    .unwrap();

    let hour = Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap();
    let key_a = BucketKey::new(&src_id, "tenant_alpha", hour);
    let key_b = BucketKey::new(&src_id, "tenant_bravo", hour);

    // Create tenants & publish data as admin/worker
    let tenant_a_uuid = store.get_or_create_tenant(&src_id, "tenant_alpha").await.unwrap();
    let tenant_b_uuid = store.get_or_create_tenant(&src_id, "tenant_bravo").await.unwrap();
    assert_ne!(tenant_a_uuid, tenant_b_uuid);

    // Upsert and publish for Tenant A
    store.upsert_dirty_and_cursor(&src_id, &[key_a.clone()], None).await.unwrap();
    let claimed_a = store.claim_buckets(Some(&src_id), "worker_a", 1, 30).await.unwrap();
    assert_eq!(claimed_a.len(), 1);
    let agg_a = BucketAggregate {
        key: key_a.clone(),
        status_aggregates: vec![StatusAggregate {
            status: "completed".to_string(),
            order_count: 10,
            sales_units: 100000,
        }],
        product_aggregates: vec![],
    };
    store.publish_bucket(&claimed_a[0], &agg_a).await.unwrap();

    // Upsert and publish for Tenant B
    store.upsert_dirty_and_cursor(&src_id, &[key_b.clone()], None).await.unwrap();
    let claimed_b = store.claim_buckets(Some(&src_id), "worker_b", 1, 30).await.unwrap();
    assert_eq!(claimed_b.len(), 1);
    let agg_b = BucketAggregate {
        key: key_b.clone(),
        status_aggregates: vec![StatusAggregate {
            status: "completed".to_string(),
            order_count: 25,
            sales_units: 250000,
        }],
        product_aggregates: vec![],
    };
    store.publish_bucket(&claimed_b[0], &agg_b).await.unwrap();

    // Connect via api_ro (non-superuser, NOBYPASSRLS)
    let ro_pool = get_api_ro_pool().await;

    // 1. Query without setting app.tenant_id: MUST RETURN 0 ROWS
    let mut ro_tx = ro_pool.begin().await.unwrap();
    let rows_unauthed = sqlx::query("SELECT * FROM agg_hourly_status;")
        .fetch_all(&mut *ro_tx)
        .await
        .unwrap();
    assert_eq!(rows_unauthed.len(), 0, "Unauthenticated queries must return 0 rows under FORCE RLS");
    ro_tx.rollback().await.unwrap();

    // 2. Query with app.tenant_id = Tenant A: MUST ONLY RETURN TENANT A ROWS
    let mut ro_tx_a = ro_pool.begin().await.unwrap();
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{}';", tenant_a_uuid))
        .execute(&mut *ro_tx_a)
        .await
        .unwrap();

    let rows_a = sqlx::query("SELECT tenant_uuid, order_count, total_amount_minor FROM agg_hourly_status;")
        .fetch_all(&mut *ro_tx_a)
        .await
        .unwrap();
    assert_eq!(rows_a.len(), 1);
    let row_a = &rows_a[0];
    let found_uuid: Uuid = row_a.get("tenant_uuid");
    let count: i64 = row_a.get("order_count");
    assert_eq!(found_uuid, tenant_a_uuid);
    assert_eq!(count, 10);
    ro_tx_a.rollback().await.unwrap();

    // 3. Query with app.tenant_id = Tenant B: MUST ONLY RETURN TENANT B ROWS
    let mut ro_tx_b = ro_pool.begin().await.unwrap();
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{}';", tenant_b_uuid))
        .execute(&mut *ro_tx_b)
        .await
        .unwrap();

    let rows_b = sqlx::query("SELECT tenant_uuid, order_count, total_amount_minor FROM agg_hourly_status;")
        .fetch_all(&mut *ro_tx_b)
        .await
        .unwrap();
    assert_eq!(rows_b.len(), 1);
    let row_b = &rows_b[0];
    let found_uuid_b: Uuid = row_b.get("tenant_uuid");
    let count_b: i64 = row_b.get("order_count");
    assert_eq!(found_uuid_b, tenant_b_uuid);
    assert_eq!(count_b, 25);
    ro_tx_b.rollback().await.unwrap();

    // 4. Adversarial check: api_ro attempting to disable RLS fails with error 42501 (FORCE RLS prevents disabling)
    let mut ro_tx_force = ro_pool.begin().await.unwrap();
    let _ = sqlx::query("SET LOCAL row_security = off;").execute(&mut *ro_tx_force).await;
    let query_res = sqlx::query("SELECT * FROM agg_hourly_status;")
        .fetch_all(&mut *ro_tx_force)
        .await;
    assert!(
        query_res.is_err(),
        "PostgreSQL FORCE RLS must reject queries when row_security = off is attempted!"
    );
    ro_tx_force.rollback().await.unwrap();
}
