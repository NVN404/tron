use api::{create_app, self_check::SecuritySelfCheck};
use anyhow::Context;
use platform_core::AppConfig;
use sqlx::postgres::PgPoolOptions;
use std::path::Path;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let config_dir = if Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    let app_config = AppConfig::load_from_dir(config_dir, None)
        .with_context(|| "Failed to load layered configuration")?;

    let ro_dsn = app_config.analytics.resolve_ro_dsn()?;
    info!("Connecting to Analytics DB as api_ro role: {}", ro_dsn);

    let pool = PgPoolOptions::new()
        .max_connections(app_config.api.pool_size)
        .connect(&ro_dsn)
        .await
        .with_context(|| "Failed to connect to Analytics DB as api_ro")?;

    // Execute security self-check before binding HTTP listener
    SecuritySelfCheck::run(&pool).await?;

    let app = create_app(pool);

    let addr = &app_config.api.bind;
    info!("Starting Analytics API and Dashboard on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
