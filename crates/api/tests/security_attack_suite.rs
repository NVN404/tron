use api::{create_app, hash_api_key, self_check::SecuritySelfCheck};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use common::AppConfig;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn test_attack_suite_pool_size_1_connection_reuse() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let admin_pool = PgPool::connect(&config.analytics_database_url).await?;

    // Create pool with MAX_CONNECTIONS = 1 (worst-case connection sharing)
    let ro_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&config.analytics_ro_database_url)
        .await?;

    let tenant_a = Uuid::new_v4();
    let tenant_b = Uuid::new_v4();

    sqlx::query("INSERT INTO tenants (id, name, health) VALUES ($1, 'Tenant A', 'healthy'), ($2, 'Tenant B', 'healthy')")
        .bind(tenant_a)
        .bind(tenant_b)
        .execute(&admin_pool)
        .await?;

    let token_a = format!("token-{}", tenant_a);
    let token_b = format!("token-{}", tenant_b);
    let hash_a = hash_api_key(&token_a);
    let hash_b = hash_api_key(&token_b);

    sqlx::query("INSERT INTO api_keys (key_hash, tenant_id, name) VALUES ($1, $2, 'Key A'), ($3, $4, 'Key B')")
        .bind(&hash_a)
        .bind(tenant_a)
        .bind(&hash_b)
        .bind(tenant_b)
        .execute(&admin_pool)
        .await?;

    // Seed distinct data for Tenant A and Tenant B
    use chrono::{TimeZone, Utc};
    let hour = Utc.timestamp_opt(((Utc::now().timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();

    sqlx::query("INSERT INTO agg_hourly (tenant_id, hour, status, order_count, sales_paise) VALUES ($1, $2, 'completed', 111, 111000)")
        .bind(tenant_a)
        .bind(hour)
        .execute(&admin_pool)
        .await?;

    sqlx::query("INSERT INTO agg_hourly (tenant_id, hour, status, order_count, sales_paise) VALUES ($1, $2, 'completed', 222, 222000)")
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
            let body = to_bytes(res.into_body(), usize::MAX).await?;
            let json: serde_json::Value = serde_json::from_slice(&body)?;
            assert_eq!(
                json["order_count"].as_i64().unwrap(),
                111,
                "Tenant A on iteration {} got polluted data: {:?}",
                i,
                json
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
            let body = to_bytes(res.into_body(), usize::MAX).await?;
            let json: serde_json::Value = serde_json::from_slice(&body)?;
            assert_eq!(
                json["order_count"].as_i64().unwrap(),
                222,
                "Tenant B on iteration {} got polluted data: {:?}",
                i,
                json
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_attack_suite_unset_tenant_context() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let ro_pool = PgPool::connect(&config.analytics_ro_database_url).await?;

    // As api_ro with NO tenant setting set, queries must return 0 rows
    let count: (i64,) = sqlx::query_as("SELECT count(*) FROM agg_hourly")
        .fetch_one(&ro_pool)
        .await?;
    assert_eq!(
        count.0, 0,
        "Unset tenant context must return 0 rows due to RLS NULLIF fallback"
    );

    let prod_count: (i64,) = sqlx::query_as("SELECT count(*) FROM agg_hourly_product")
        .fetch_one(&ro_pool)
        .await?;
    assert_eq!(
        prod_count.0, 0,
        "Unset tenant context must return 0 rows for product rollups"
    );

    Ok(())
}

#[tokio::test]
async fn test_attack_suite_sql_injection_defense() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let ro_pool = PgPool::connect(&config.analytics_ro_database_url).await?;
    let app = create_app(ro_pool);

    // Attempt SQL injection in path
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tenants/'%20OR%20'1'='1/orders/count")
                .header(header::AUTHORIZATION, "Bearer whatever")
                .body(Body::empty())?,
        )
        .await?;
    // Path parser rejects non-UUID strings or unauthenticated requests
    assert!(
        res.status() == StatusCode::NOT_FOUND
            || res.status() == StatusCode::BAD_REQUEST
            || res.status() == StatusCode::UNAUTHORIZED
    );

    // Attempt SQL injection in Authorization header
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tenants/00000000-0000-0001-0000-000000000001/orders/count")
                .header(
                    header::AUTHORIZATION,
                    "Bearer ' OR 1=1; DROP TABLE agg_hourly; --",
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[tokio::test]
async fn test_attack_suite_superuser_refusal() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    // Admin pool is connected as superuser (postgres)
    let admin_pool = PgPool::connect(&config.analytics_database_url).await?;

    // Startup self check MUST FAIL when passed superuser pool!
    let result = SecuritySelfCheck::run(&admin_pool).await;
    assert!(
        result.is_err(),
        "SecuritySelfCheck must refuse to boot when connected as superuser"
    );
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("SUPERUSER") || err_str.contains("BYPASSRLS"),
        "Error message should mention superuser/bypassrls: {}",
        err_str
    );

    Ok(())
}
