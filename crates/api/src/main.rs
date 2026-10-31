use api::self_check::SecuritySelfCheck;
use axum::{routing::get, Json, Router};
use common::{init_telemetry, AppConfig};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use tracing::info;

async fn health_check() -> Json<serde_json::Value> {
    Json(json!({ "status": "healthy", "service": "analytics-api" }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("api");
    let config = AppConfig::from_env()?;

    // Connect as restricted api_ro role
    info!("Connecting to Analytics DB as api_ro role: {}", config.analytics_ro_database_url);
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.analytics_ro_database_url)
        .await?;

    // Execute security self-check before binding HTTP listener
    SecuritySelfCheck::run(&pool).await?;

    let app = Router::new().route("/health", get(health_check));

    let addr = format!("{}:{}", config.api_host, config.api_port);
    info!("Starting Analytics API on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
