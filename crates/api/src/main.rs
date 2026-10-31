use axum::{routing::get, Json, Router};
use common::{init_telemetry, AppConfig};
use serde_json::json;
use tracing::info;

async fn health_check() -> Json<serde_json::Value> {
    Json(json!({ "status": "healthy", "service": "analytics-api" }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("api");
    let config = AppConfig::from_env()?;

    let app = Router::new().route("/health", get(health_check));

    let addr = format!("{}:{}", config.api_host, config.api_port);
    info!("Starting Analytics API on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
