use api::{create_app, hash_api_key};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use common::AppConfig;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn test_api_endpoints_auth_and_caching() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let admin_pool = PgPool::connect(&config.analytics_database_url).await?;
    let ro_pool = PgPool::connect(&config.analytics_ro_database_url).await?;

    // Create 2 test tenants
    let tenant_a = Uuid::new_v4();
    let tenant_b = Uuid::new_v4();

    sqlx::query("INSERT INTO tenants (id, name, health) VALUES ($1, 'Tenant A', 'healthy'), ($2, 'Tenant B', 'healthy') ON CONFLICT DO NOTHING")
        .bind(tenant_a)
        .bind(tenant_b)
        .execute(&admin_pool)
        .await?;

    // Create API keys for Tenant A and Tenant B
    let token_a = "secret-token-tenant-a-12345";
    let token_b = "secret-token-tenant-b-67890";
    let hash_a = hash_api_key(token_a);
    let hash_b = hash_api_key(token_b);

    sqlx::query("INSERT INTO api_keys (key_hash, tenant_id, name) VALUES ($1, $2, 'Key A'), ($3, $4, 'Key B') ON CONFLICT (key_hash) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, revoked = FALSE")
        .bind(&hash_a)
        .bind(tenant_a)
        .bind(&hash_b)
        .bind(tenant_b)
        .execute(&admin_pool)
        .await?;

    // Insert rollup test data for Tenant A
    use chrono::TimeZone;
    let now = chrono::Utc.timestamp_opt(((chrono::Utc::now().timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();
    sqlx::query(
        "INSERT INTO agg_hourly (tenant_id, hour, status, order_count, sales_paise) VALUES ($1, $2, 'completed', 42, 420000) \
         ON CONFLICT (tenant_id, hour, status) DO UPDATE SET order_count = 42"
    )
    .bind(tenant_a)
    .bind(now)
    .execute(&admin_pool)
    .await?;

    let prod_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO agg_hourly_product (tenant_id, hour, product_id, category, units, sales_paise) VALUES ($1, $2, $3, 'Books', 10, 100000) \
         ON CONFLICT (tenant_id, hour, product_id) DO UPDATE SET units = 10"
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
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // 5. Zero Cache Headers Verification (INC-02 defense)
    assert_eq!(
        res.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store, no-cache, must-revalidate, proxy-revalidate"
    );
    assert_eq!(res.headers().get(header::PRAGMA).unwrap(), "no-cache");
    assert_eq!(res.headers().get(header::EXPIRES).unwrap(), "0");

    // 6. Valid Count Query for Tenant A -> 200 OK
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant_a))
                .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                .body(Body::empty())?,
        )
        .await?;
    let status = res.status();
    let body = to_bytes(res.into_body(), usize::MAX).await?;
    let body_str = String::from_utf8_lossy(&body);
    assert_eq!(status, StatusCode::OK, "Body: {}", body_str);
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert!(json["order_count"].as_i64().unwrap() >= 42);
    assert!(json["data_as_of"].is_string());

    // 7. Valid Trend Query
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/trend?grain=day", tenant_a))
                .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::OK);

    // 8. Valid Top Products Query
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/products/top?n=5", tenant_a))
                .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert!(json["top_products"].as_array().is_some());

    // 9. Valid Breakdown Query
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/breakdown", tenant_a))
                .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert!(json["by_status"].as_array().is_some());
    assert!(json["by_category"].as_array().is_some());

    // 10. Quarantine Quarantine Enforcement -> 503 Service Unavailable
    sqlx::query("UPDATE tenants SET health = 'quarantined' WHERE id = $1")
        .bind(tenant_a)
        .execute(&admin_pool)
        .await?;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/tenants/{}/orders/count", tenant_a))
                .header(header::AUTHORIZATION, format!("Bearer {}", token_a))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Cleanup: restore health to healthy
    sqlx::query("UPDATE tenants SET health = 'healthy' WHERE id = $1")
        .bind(tenant_a)
        .execute(&admin_pool)
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_api_vs_oracle_real_tenant() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let admin_pool = PgPool::connect(&config.analytics_database_url).await?;
    let ro_pool = PgPool::connect(&config.analytics_ro_database_url).await?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;

    // Top tenant from 10M dataset
    let top_tenant_id = Uuid::parse_str("00000000-0000-0001-0000-000000000001")?;
    let test_token = "oracle-verify-token-top-tenant";
    let token_hash = hash_api_key(test_token);

    // Register API key
    sqlx::query("INSERT INTO api_keys (key_hash, tenant_id, name) VALUES ($1, $2, 'Oracle Test Key') ON CONFLICT (key_hash) DO NOTHING")
        .bind(&token_hash)
        .bind(top_tenant_id)
        .execute(&admin_pool)
        .await?;

    use chrono::{TimeZone, Utc};
    let now = Utc::now();
    let from = Utc.timestamp_opt(((now.timestamp() - 1800 - 30 * 86400) / 3600) * 3600 + 1800, 0).unwrap();
    let to = Utc.timestamp_opt(((now.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();

    let oracle_res = oracle::Oracle::count_and_total(&source_pool, top_tenant_id, from, to).await?;

    let app = create_app(ro_pool.clone());
    let res = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/tenants/{}/orders/count?from={}&to={}",
                    top_tenant_id,
                    from.format("%Y-%m-%dT%H:%M:%SZ"),
                    to.format("%Y-%m-%dT%H:%M:%SZ")
                ))
                .header(header::AUTHORIZATION, format!("Bearer {}", test_token))
                .body(Body::empty())?,
        )
        .await?;

    let status = res.status();
    let body = to_bytes(res.into_body(), usize::MAX).await?;
    let body_str = String::from_utf8_lossy(&body);
    assert_eq!(status, StatusCode::OK, "Body: {}", body_str);
    let json: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(
        json["order_count"].as_i64().unwrap(),
        oracle_res.order_count,
        "API order count must match Oracle ground truth"
    );
    assert_eq!(
        json["total_sales_paise"].as_i64().unwrap(),
        oracle_res.total_sales_paise,
        "API total sales paise must match Oracle ground truth"
    );

    Ok(())
}
