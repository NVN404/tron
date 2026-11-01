use analytics_store::AnalyticsStore;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use platform_core::AppConfig;
use sqlx::postgres::PgPoolOptions;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use worker::WorkerEngine;

#[derive(Parser, Debug)]
#[command(name = "worker", about = "Analytics Aggregation Worker")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the aggregation worker daemon (default)
    Run,
    /// Scan source databases and backfill all historic buckets
    Backfill,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let config_dir = if Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    let app_config = AppConfig::load_from_dir(config_dir, None)
        .with_context(|| "Failed to load layered configuration")?;

    let analytics_dsn = app_config.analytics.resolve_dsn()?;
    let pool = PgPoolOptions::new()
        .max_connections(app_config.analytics.max_connections)
        .connect(&analytics_dsn)
        .await
        .with_context(|| "Failed to connect to Analytics PostgreSQL")?;

    let store = AnalyticsStore::new(pool);
    store.register_sources(&app_config).await?;

    let engine = WorkerEngine::from_config(&app_config, store).await?;

    match cli.command.unwrap_or(Commands::Run) {
        Commands::Run => {
            let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.ok();
                let _ = shutdown_tx.send(());
            });

            WorkerEngine::start_daemon(Arc::new(engine), shutdown_rx).await;
        }
        Commands::Backfill => {
            tracing::info!("Starting backfill scan across all configured sources...");
            let count = engine.backfill().await?;
            tracing::info!("Backfill completed successfully. Processed {} buckets.", count);
        }
    }

    Ok(())
}
