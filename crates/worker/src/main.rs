use anyhow::Result;
use clap::{Parser, Subcommand};
use common::{init_telemetry, AppConfig};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;
use worker::WorkerEngine;

#[derive(Parser, Debug)]
#[command(name = "worker", about = "Multi-tenant aggregation background worker")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run worker continuous processing loop
    Run,
    /// Backfill historical data from source database
    Backfill {
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_telemetry("worker");
    let cli = Cli::parse();
    let config = AppConfig::from_env()?;

    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    let engine = WorkerEngine::new(
        source_pool,
        analytics_pool,
        "worker-01".to_string(),
        config.worker_batch_size,
        config.worker_lease_seconds,
    );

    match cli.command.unwrap_or(Commands::Run) {
        Commands::Run => {
            info!("Starting Aggregation Worker continuous service...");
            let running = Arc::new(AtomicBool::new(true));
            let r = running.clone();

            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
                info!("Shutdown signal received, draining worker loop...");
                r.store(false, Ordering::Relaxed);
            });

            engine.run_loop(running, config.worker_poll_interval_ms).await?;
        }
        Commands::Backfill { .. } => {
            info!("Worker backfill invoked. Backfill pipeline will be executed in Phase 7.");
        }
    }

    Ok(())
}
