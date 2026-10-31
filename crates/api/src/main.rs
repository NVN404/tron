use api::{create_app, self_check::SecuritySelfCheck};
use common::{init_telemetry, AppConfig};
use sqlx::postgres::PgPoolOptions;
use tracing::info;

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

    let app = create_app(pool);

    let addr = format!("{}:{}", config.api_host, config.api_port);
    info!("Starting Analytics API on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
