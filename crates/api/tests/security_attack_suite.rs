use api::{create_app, hash_api_key};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use chrono::{TimeZone, Utc};
use platform_core::AppConfig;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::path::Path;
use tower::ServiceExt;
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
async fn test_attack_suite_pool_size_1_connection_reuse() -> anyhow::Result<()> {
    let config = load_test_config();
    let admin_dsn = config.analytics.resolve_dsn()?;
    let ro_dsn = config.analytics.resolve_ro_dsn()?;

    let admin_pool = PgPool::connect(&admin_dsn).await?;

    // Create pool with MAX_CONNECTIONS = 1 (worst-case connection sharing)
    let ro_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&ro_dsn)
        .await?;

    let tenant_a = Uuid::new_v4();
    let tenant_b = Uuid::new_v4();

    sqlx::query("INSERT INTO sources (id, kind, capture_mode) VALUES ('pg_source_1', 'postgres', 'outbox') ON CONFLICT DO NOTHING;")
        .execute(&admin_pool)
        .await?;

    sqlx::query(
        "INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health) VALUES ($1, 'pg_source_1', $3, 'Tenant A', 'healthy'), ($2, 'pg_source_1', $4, 'Tenant B', 'healthy')",
    )
    .bind(tenant_a)
    .bind(tenant_b)
    .bind(format!("ext_{}", tenant_a))
    .bind(format!("ext_{}", tenant_b))
    .execute(&admin_pool)
    .await?;

    let token_a = format!("token-{}", tenant_a);
    let token_b = format!("token-{}", tenant_b);
    let hash_a = hash_api_key(&token_a);
    let hash_b = hash_api_key(&token_b);

    sqlx::query("INSERT INTO api_keys (key_hash, tenant_uuid, name) VALUES ($1, $2, 'Key A'), ($3, $4, 'Key B')")
        .bind(&hash_a)
        .bind(tenant_a)
        .bind(&hash_b)
        .bind(tenant_b)
        .execute(&admin_pool)
        .await?;

    let hour = Utc.timestamp_opt((Utc::now().timestamp() / 3600) * 3600, 0).unwrap();

    sqlx::query(
        "INSERT INTO agg_hourly_status (tenant_uuid, source_id, hour, status, order_count, total_amount_minor) VALUES ($1, 'pg_source_1', $2, 'completed', 111, 111000) ON CONFLICT DO NOTHING;",
    )
    .bind(tenant_a)
    .bind(hour)
    .execute(&admin_pool)
    .await?;

    sqlx::query(
        "INSERT INTO agg_hourly_status (tenant_uuid, source_id, hour, status, order_count, total_amount_minor) VALUES ($1, 'pg_source_1', $2, 'completed', 222, 222000) ON CONFLICT DO NOTHING;",
    )
    .bind(tenant_b)
    .bind(hour)
    .execute(&admin_pool)
    .await?;

    let app = create_app(ro_pool);

    // Rapidly interleave 50 requests across the single shared connection
    for i in 0..50 {
        if i % 2 == 0 {
            // Tenant A request
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/tenants/{}/orders/count", tenant_a))
                        .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                        .body(Body::empty())?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::OK);
            let bytes = to_bytes(res.into_body(), usize::MAX).await?;
            let body: serde_json::Value = serde_json::from_slice(&bytes)?;
            assert_eq!(
                body["order_count"], 111,
                "Request {} leaked data! Expected 111 for Tenant A, got {}",
                i, body["order_count"]
            );
        } else {
            // Tenant B request
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/tenants/{}/orders/count", tenant_b))
                        .header(header::AUTHORIZATION, format!("Bearer {}", token_b))
                        .body(Body::empty())?,
                )
                .await?;

            assert_eq!(res.status(), StatusCode::OK);
            let bytes = to_bytes(res.into_body(), usize::MAX).await?;
            let body: serde_json::Value = serde_json::from_slice(&bytes)?;
            assert_eq!(
                body["order_count"], 222,
                "Request {} leaked data! Expected 222 for Tenant B, got {}",
                i, body["order_count"]
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_attack_suite_cross_tenant_probe_returns_404() -> anyhow::Result<()> {
    let config = load_test_config();
    let admin_dsn = config.analytics.resolve_dsn()?;
    let ro_dsn = config.analytics.resolve_ro_dsn()?;

    let admin_pool = PgPool::connect(&admin_dsn).await?;
    let ro_pool = PgPool::connect(&ro_dsn).await?;

    let victim_tenant = Uuid::new_v4();
    let attacker_tenant = Uuid::new_v4();

    sqlx::query("INSERT INTO sources (id, kind, capture_mode) VALUES ('pg_source_1', 'postgres', 'outbox') ON CONFLICT DO NOTHING;")
        .execute(&admin_pool)
        .await?;

    sqlx::query(
        "INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health) VALUES ($1, 'pg_source_1', $3, 'Victim', 'healthy'), ($2, 'pg_source_1', $4, 'Attacker', 'healthy')",
    )
    .bind(victim_tenant)
    .bind(attacker_tenant)
    .bind(format!("ext_{}", victim_tenant))
    .bind(format!("ext_{}", attacker_tenant))
    .execute(&admin_pool)
    .await?;

    let attacker_token = format!("attacker-token-{}", attacker_tenant);
    let hash = hash_api_key(&attacker_token);

    sqlx::query("INSERT INTO api_keys (key_hash, tenant_uuid, name) VALUES ($1, $2, 'Attacker Key')")
        .bind(&hash)
        .bind(attacker_tenant)
        .execute(&admin_pool)
        .await?;

    let app = create_app(ro_pool);

    // Attacker passes THEIR valid Bearer token, but attempts to query the VICTIM's URL path:
    // /tenants/<victim_id>/orders/count
    let res = app
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", victim_tenant))
                .header(header::AUTHORIZATION, format!("Bearer {}", attacker_token))
                .body(Body::empty())?,
        )
        .await?;

    // MUST return 404 NOT FOUND (never 403, preventing ID enumeration)
    assert_eq!(
        res.status(),
        StatusCode::NOT_FOUND,
        "Cross-tenant access attempt must be rejected with 404 NOT FOUND to eliminate enumeration"
    );

    Ok(())
}

#[tokio::test]
async fn test_attack_suite_direct_db_bypass_attempt() -> anyhow::Result<()> {
    let config = load_test_config();
    let ro_dsn = config.analytics.resolve_ro_dsn()?;
    let ro_pool = PgPool::connect(&ro_dsn).await?;

    // 1. Attempt to bypass RLS via SET LOCAL row_security = off
    let mut tx = ro_pool.begin().await?;
    let _ = sqlx::query("SET LOCAL row_security = off;").execute(&mut *tx).await;
    let res = sqlx::query("SELECT count(*) FROM agg_hourly_status;").fetch_one(&mut *tx).await;

    assert!(res.is_err(), "Non-superuser api_ro must NOT be allowed to disable row_security");
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("42501") || err_str.contains("permission denied") || err_str.contains("row-level security"),
        "Expected error 42501 permission denied, got: {}",
        err_str
    );
    let _ = tx.rollback().await;

    // 2. Querying without app.tenant_id must fail closed (return 0 rows)
    let mut tx = ro_pool.begin().await?;
    let count: (i64,) = sqlx::query_as("SELECT count(*)::bigint FROM agg_hourly_status")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count.0, 0, "Query without tenant context must return 0 rows (fail closed)");
    tx.commit().await?;

    Ok(())
}

#[tokio::test]
async fn test_attack_suite_sql_injection_defense() -> anyhow::Result<()> {
    let config = load_test_config();
    let admin_dsn = config.analytics.resolve_dsn()?;
    let ro_dsn = config.analytics.resolve_ro_dsn()?;

    let admin_pool = PgPool::connect(&admin_dsn).await?;
    let ro_pool = PgPool::connect(&ro_dsn).await?;

    let tenant = Uuid::new_v4();
    let token = format!("token-sqli-{}", tenant);
    let key_hash = hash_api_key(&token);

    sqlx::query("INSERT INTO sources (id, kind, capture_mode) VALUES ('pg_source_1', 'postgres', 'outbox') ON CONFLICT DO NOTHING;")
        .execute(&admin_pool)
        .await?;

    sqlx::query("INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health) VALUES ($1, 'pg_source_1', $2, 'SQLi Test', 'healthy')")
        .bind(tenant)
        .bind(format!("ext_{}", tenant))
        .execute(&admin_pool)
        .await?;

    sqlx::query("INSERT INTO api_keys (key_hash, tenant_uuid, name) VALUES ($1, $2, 'SQLi Key')")
        .bind(&key_hash)
        .bind(tenant)
        .execute(&admin_pool)
        .await?;

    let app = create_app(ro_pool);

    // 1. Attack payload in status query param
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count?status=%27%20OR%20%271%27=%271", tenant))
                .header(header::AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = to_bytes(res.into_body(), usize::MAX).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["order_count"], 0, "SQL injection in status must not match anything");

    // 2. Attack payload in timestamp query param (must return 400 Bad Request)
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count?from=2026-01-01%27%20OR%201=1;--", tenant))
                .header(header::AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST, "Malformed timestamp must fail validation");

    // 3. Attack payload in Bearer auth header (must return 401 Unauthorized)
    let res = app
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant))
                .header(header::AUTHORIZATION, "Bearer ' OR 1=1;--")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "Forged auth header must return 401");

    Ok(())
}

#[tokio::test]
async fn test_attack_suite_unauthenticated_requests() -> anyhow::Result<()> {
    let config = load_test_config();
    let ro_dsn = config.analytics.resolve_ro_dsn()?;
    let ro_pool = PgPool::connect(&ro_dsn).await?;
    let app = create_app(ro_pool);
    let tenant = Uuid::new_v4();

    // 1. Missing Authorization header
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 2. Invalid Bearer token
    let res = app
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant))
                .header(header::AUTHORIZATION, "Bearer totally-fake-token-999")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}
