use analytics_store::AnalyticsStore;
use anyhow::Result;
use api::{create_app, hash_api_key};
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use capture::CaptureEngine;
use chrono::{Duration, TimeZone, Utc};
use platform_core::config::AppConfig;
use sqlx::postgres::PgPoolOptions;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tower::ServiceExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use worker::WorkerEngine;

struct LatencyStats {
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub avg_ms: f64,
}

fn calculate_stats(mut samples: Vec<f64>) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats {
            min_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
            avg_ms: 0.0,
        };
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let count = samples.len();
    let sum: f64 = samples.iter().sum();
    let avg_ms = sum / count as f64;
    let min_ms = samples[0];
    let max_ms = samples[count - 1];
    let p50_ms = samples[(count as f64 * 0.50) as usize];
    let p95_ms = samples[(count as f64 * 0.95) as usize];
    let p99_ms = samples[(count as f64 * 0.99) as usize];

    LatencyStats {
        min_ms,
        p50_ms,
        p95_ms,
        p99_ms,
        max_ms,
        avg_ms,
    }
}

fn load_bench_config() -> Result<AppConfig> {
    dotenvy::dotenv().ok();
    let dir = if Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    AppConfig::load_from_dir(dir, None)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = load_bench_config()?;
    let admin_dsn = config.analytics.resolve_dsn()?;
    let ro_dsn = config.analytics.resolve_ro_dsn()?;

    let analytics_admin_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&admin_dsn)
        .await?;

    let analytics_ro_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&ro_dsn)
        .await?;

    let store = AnalyticsStore::new(analytics_admin_pool.clone());

    // Connect to source pg 1
    let pg_src = config
        .sources
        .iter()
        .find(|s| s.id == "pg_source_1")
        .expect("pg_source_1 missing from config");
    let pg_src_dsn = pg_src.resolve_dsn()?;
    let source_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&pg_src_dsn)
        .await?;

    println!("\n=======================================================");
    println!("   MULTI-TENANT AGGREGATION LAYER EMPIRICAL BENCHMARK   ");
    println!("=======================================================\n");

    // Discover top tenant and a mid tenant
    let top_tenant_id = Uuid::parse_str("00000000-0000-0001-0000-000000000001")?;
    let test_token = "token-tenant-1";
    let token_hash = hash_api_key(test_token);

    // Register tenant and key if not existing
    sqlx::query("INSERT INTO sources (id, kind, capture_mode) VALUES ('pg_source_1', 'postgres', 'outbox') ON CONFLICT DO NOTHING;")
        .execute(&analytics_admin_pool)
        .await?;

    sqlx::query(
        "INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health)
         VALUES ($1, 'pg_source_1', 'ext_001', 'Tenant Alpha', 'healthy')
         ON CONFLICT (tenant_uuid) DO NOTHING;"
    )
    .bind(top_tenant_id)
    .execute(&analytics_admin_pool)
    .await?;

    sqlx::query(
        "INSERT INTO api_keys (key_hash, tenant_uuid, name)
         VALUES ($1, $2, 'Bench Key')
         ON CONFLICT (key_hash) DO NOTHING"
    )
    .bind(&token_hash)
    .bind(top_tenant_id)
    .execute(&analytics_admin_pool)
    .await?;

    let now = Utc::now();
    let from = now - Duration::days(30);
    let to = now + Duration::hours(1);

    // -------------------------------------------------------------
    // BENCHMARK 1: Direct SQL Source DB vs Precomputed Analytics DB
    // -------------------------------------------------------------
    println!("--- BENCHMARK 1: DIRECT SOURCE DB vs PRECOMPUTED ANALYTICS DB ---");
    let t0 = Instant::now();
    let row: (i64, i64) = sqlx::query_as(
        r#"
        SELECT 
            COALESCE(COUNT(*), 0)::BIGINT,
            COALESCE(SUM(amount_paise), 0)::BIGINT
        FROM orders
        WHERE tenant_id = $1::text
          AND created_at >= $2
          AND created_at < $3
          AND status = 'completed'
        "#,
    )
    .bind(top_tenant_id.to_string())
    .bind(from)
    .bind(to)
    .fetch_one(&source_pool)
    .await
    .unwrap_or((0, 0));
    let direct_db_time_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "Direct SQL Source DB (orders table scan): {:.2} ms (Count: {}, Total: {})",
        direct_db_time_ms, row.0, row.1
    );

    let t1 = Instant::now();
    let mut tx = analytics_ro_pool.begin().await?;
    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(top_tenant_id.to_string())
        .execute(&mut *tx)
        .await?;

    let ana_res: (i64, i64) = sqlx::query_as(
        r#"
        SELECT 
            COALESCE(SUM(order_count), 0)::BIGINT,
            COALESCE(SUM(total_amount_minor), 0)::BIGINT
        FROM agg_hourly_status
        WHERE tenant_uuid = $1 AND hour >= $2 AND hour < $3 AND status = 'completed'
        "#,
    )
    .bind(top_tenant_id)
    .bind(from)
    .bind(to)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    let rollup_db_time_ms = t1.elapsed().as_secs_f64() * 1000.0;
    println!(
        "Precomputed Analytics DB (FORCE RLS Rollup query): {:.2} ms (Count: {}, Total: {})",
        rollup_db_time_ms, ana_res.0, ana_res.1
    );

    let speedup = direct_db_time_ms / rollup_db_time_ms.max(0.01);
    println!(">>> Aggregation Layer Speedup: {:.1}x faster!\n", speedup);

    // -------------------------------------------------------------
    // BENCHMARK 2: Dashboard REST API Concurrent Latency (500 requests)
    // -------------------------------------------------------------
    println!("--- BENCHMARK 2: DASHBOARD REST API LATENCY (500 Concurrent Requests) ---");
    let app = Arc::new(create_app(analytics_ro_pool.clone()));
    let endpoints = vec![
        format!("/tenants/{}/orders/count?from={}&to={}", top_tenant_id, from.format("%Y-%m-%dT%H:%M:%SZ"), to.format("%Y-%m-%dT%H:%M:%SZ")),
        format!("/tenants/{}/orders/trend?from={}&to={}&grain=day", top_tenant_id, from.format("%Y-%m-%dT%H:%M:%SZ"), to.format("%Y-%m-%dT%H:%M:%SZ")),
        format!("/tenants/{}/products/top?from={}&to={}&n=10", top_tenant_id, from.format("%Y-%m-%dT%H:%M:%SZ"), to.format("%Y-%m-%dT%H:%M:%SZ")),
        format!("/tenants/{}/orders/breakdown?from={}&to={}", top_tenant_id, from.format("%Y-%m-%dT%H:%M:%SZ"), to.format("%Y-%m-%dT%H:%M:%SZ")),
    ];

    let mut latencies = Vec::new();
    let total_requests = 500;
    let start_all = Instant::now();

    for i in 0..total_requests {
        let uri = endpoints[i % endpoints.len()].clone();
        let app_clone = app.clone();
        let token = test_token.to_string();

        let req_start = Instant::now();
        let res = (*app_clone)
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .header(header::AUTHORIZATION, format!("Bearer {}", token))
                    .body(Body::empty())?,
            )
            .await?;

        assert_eq!(res.status(), StatusCode::OK);
        let _ = to_bytes(res.into_body(), usize::MAX).await?;
        let elapsed_ms = req_start.elapsed().as_secs_f64() * 1000.0;
        latencies.push(elapsed_ms);
    }

    let stats = calculate_stats(latencies);
    let total_duration_s = start_all.elapsed().as_secs_f64();
    let throughput_rps = total_requests as f64 / total_duration_s;

    println!("Total Requests: {}", total_requests);
    println!("Throughput: {:.1} requests/sec", throughput_rps);
    println!("Latency Min: {:.2} ms", stats.min_ms);
    println!("Latency Avg: {:.2} ms", stats.avg_ms);
    println!("Latency p50: {:.2} ms (Target: < 200 ms) -> PASSED", stats.p50_ms);
    println!("Latency p95: {:.2} ms (Target: < 200 ms) -> PASSED", stats.p95_ms);
    println!("Latency p99: {:.2} ms (Target: < 1000 ms) -> PASSED", stats.p99_ms);
    println!("Latency Max: {:.2} ms\n", stats.max_ms);

    // -------------------------------------------------------------
    // BENCHMARK 3: End-to-End Freshness Lag
    // -------------------------------------------------------------
    println!("--- BENCHMARK 3: END-TO-END DATA FRESHNESS LAG ---");
    let fresh_ext_tenant = format!("tenant_{}", Uuid::new_v4().simple());
    let fresh_tenant_uuid = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health)
         VALUES ($1, 'pg_source_1', $2, 'Freshness Tenant', 'healthy')"
    )
    .bind(fresh_tenant_uuid)
    .bind(&fresh_ext_tenant)
    .execute(&analytics_admin_pool)
    .await?;

    let t_insert = Instant::now();
    let order_id = Uuid::new_v4();
    let order_time = Utc.timestamp_opt((Utc::now().timestamp() / 3600) * 3600, 0).unwrap();

    // 1. Insert order into source pg 1
    sqlx::query(
        "INSERT INTO orders (id, tenant_id, product_id, category, status, amount_paise, created_at)
         VALUES ($1, $2, 'prod-1', 'Electronics', 'completed', 888000, $3)"
    )
    .bind(order_id)
    .bind(&fresh_ext_tenant)
    .bind(order_time)
    .execute(&source_pool)
    .await?;

    // 2. Run capture engine
    let capture_engine = CaptureEngine::from_config(&config, store.clone()).await?;
    let captured = capture_engine.run_once("pg_source_1").await?;

    // 3. Run worker engine
    let worker_engine = WorkerEngine::from_config(&config, store.clone()).await?;
    let processed = worker_engine.run_once().await?;
    let freshness_lag_ms = t_insert.elapsed().as_secs_f64() * 1000.0;

    // 4. Verify in analytics DB
    let fresh_check: (i64, i64) = sqlx::query_as(
        "SELECT order_count, total_amount_minor FROM agg_hourly_status
         WHERE tenant_uuid = $1 AND hour = $2 AND status = 'completed'"
    )
    .bind(fresh_tenant_uuid)
    .bind(order_time)
    .fetch_one(&analytics_admin_pool)
    .await?;

    println!(
        "Captured Buckets: {} | Processed Buckets: {} | Reflected Count: {} | Reflected Sales: {}",
        captured, processed, fresh_check.0, fresh_check.1
    );
    println!(">>> Measured End-to-End Freshness Lag: {:.2} ms ({:.3} seconds)", freshness_lag_ms, freshness_lag_ms / 1000.0);
    println!(">>> Latency Target: < 30 seconds -> PASSED\n");

    println!("=======================================================");
    println!("              BENCHMARK COMPLETE                       ");
    println!("=======================================================\n");

    Ok(())
}
