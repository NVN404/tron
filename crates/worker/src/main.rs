use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use clap::{Parser, Subcommand};
use common::{init_telemetry, AppConfig};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;
use worker::{BackfillRunner, WorkerEngine};

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
        tenant: Option<Uuid>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
    },
    /// Truncate rollups and run full rebuild
    Rebuild {
        #[arg(long)]
        tenant: Option<Uuid>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
    },
    /// Reconcile and self-heal drift against source
    Reconcile {
        #[arg(long)]
        tenant: Uuid,
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

    match cli.command.unwrap_or(Commands::Run) {
        Commands::Run => {
            let engine = WorkerEngine::new(
                source_pool,
                analytics_pool,
                "worker-01".to_string(),
                config.worker_batch_size,
                config.worker_lease_seconds,
            );

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
        Commands::Backfill { tenant, from, to } => {
            let backfiller = BackfillRunner::new(source_pool, analytics_pool);
            let from_dt = from
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.to_utc()))
                .unwrap_or_else(|| Utc::now() - Duration::days(400));
            let to_dt = to
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.to_utc()))
                .unwrap_or_else(|| Utc::now() + Duration::days(1));

            info!("Starting Backfill across range [{}, {})...", from_dt, to_dt);
            let plans = backfiller.plan_tenants(tenant).await?;
            for p in plans {
                backfiller.backfill_tenant(p.tenant_id, from_dt, to_dt, 30).await?;
            }
            info!("Backfill completed successfully.");
        }
        Commands::Rebuild { tenant, from, to } => {
            let backfiller = BackfillRunner::new(source_pool, analytics_pool);
            let from_dt = from
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.to_utc()))
                .unwrap_or_else(|| Utc::now() - Duration::days(400));
            let to_dt = to
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.to_utc()))
                .unwrap_or_else(|| Utc::now() + Duration::days(1));

            backfiller.rebuild(tenant, from_dt, to_dt).await?;
        }
        Commands::Reconcile { tenant, from, to } => {
            let reconciler = worker::Reconciler::new(source_pool, analytics_pool);
            let from_dt = from
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.to_utc()))
                .unwrap_or_else(|| Utc::now() - Duration::days(7));
            let to_dt = to
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.to_utc()))
                .unwrap_or_else(Utc::now);

            info!("Starting reconciliation for tenant {} in [{}, {})...", tenant, from_dt, to_dt);
            let drifts = reconciler.reconcile_window(tenant, from_dt, to_dt).await?;
            info!("Reconciliation finished. Detected and repaired {} drifted buckets.", drifts.len());
        }
    }

    Ok(())
}
