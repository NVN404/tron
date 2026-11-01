use api::{create_app, hash_api_key};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use chrono::{TimeZone, Utc};
use platform_core::AppConfig;
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
async fn test_api_endpoints_auth_and_caching() -> anyhow::Result<()> {
    let config = load_test_config();
    let admin_dsn = config.analytics.resolve_dsn()?;
    let ro_dsn = config.analytics.resolve_ro_dsn()?;

    let admin_pool = PgPool::connect(&admin_dsn).await?;
    let ro_pool = PgPool::connect(&ro_dsn).await?;

    // Create 2 test tenants
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

    // Create API keys for Tenant A and Tenant B
    let token_a = "secret-token-tenant-a-12345";
    let token_b = "secret-token-tenant-b-67890";
    let hash_a = hash_api_key(token_a);
    let hash_b = hash_api_key(token_b);

    sqlx::query(
        "INSERT INTO api_keys (key_hash, tenant_uuid, name) VALUES ($1, $2, 'Key A'), ($3, $4, 'Key B') \
         ON CONFLICT (key_hash) DO UPDATE SET tenant_uuid = EXCLUDED.tenant_uuid, revoked = FALSE;"
    )
        .bind(&hash_a)
        .bind(tenant_a)
        .bind(&hash_b)
        .bind(tenant_b)
        .execute(&admin_pool)
        .await?;

    // Insert rollup test data for Tenant A
    let now = Utc.timestamp_opt((Utc::now().timestamp() / 3600) * 3600, 0).unwrap();
    sqlx::query(
        "INSERT INTO agg_hourly_status (tenant_uuid, source_id, hour, status, order_count, total_amount_minor) VALUES ($1, 'pg_source_1', $2, 'completed', 42, 420000) \
         ON CONFLICT (tenant_uuid, hour, status) DO UPDATE SET order_count = 42"
    )
    .bind(tenant_a)
    .bind(now)
    .execute(&admin_pool)
    .await?;

    let prod_id = "prod_books_01";
    sqlx::query(
        "INSERT INTO agg_hourly_product (tenant_uuid, source_id, hour, product_id, units_sold, gross_revenue_minor) VALUES ($1, 'pg_source_1', $2, $3, 10, 100000) \
         ON CONFLICT (tenant_uuid, hour, product_id) DO UPDATE SET units_sold = 10"
    )
    .bind(tenant_a)
    .bind(now)
    .bind(prod_id)
    .execute(&admin_pool)
    .await?;

    let app = create_app(ro_pool.clone());

    // 1. Health endpoint (no auth needed)
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::OK);

    // 2. Unauthenticated request -> 401 Unauthorized
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant_a))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 3. Invalid token -> 401 Unauthorized
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant_a))
                .header(header::AUTHORIZATION, "Bearer invalid-token")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 4. IDOR Attack Prevention: Tenant A token querying Tenant B URL -> 404 Not Found!
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant_b))
                .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(
        res.status(),
        StatusCode::NOT_FOUND,
        "IDOR probe must return 404 to eliminate enumeration"
    );

    // 5. Valid request -> 200 OK + Anti-Cache Headers Verified
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

    // Anti-leak cache defense assertions:
    let cache_control = res
        .headers()
        .get(header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        cache_control.contains("no-store"),
        "CRITICAL: Response missing Cache-Control: no-store! Headers: {:?}",
        res.headers()
    );

    let bytes = to_bytes(res.into_body(), usize::MAX).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["order_count"], 42);
    assert_eq!(body["total_sales_paise"], 420000);

    // 6. Test Product Breakdown Route
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/products/top", tenant_a))
                .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = to_bytes(res.into_body(), usize::MAX).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert!(body["top_products"].as_array().unwrap().len() >= 1);

    Ok(())
}
