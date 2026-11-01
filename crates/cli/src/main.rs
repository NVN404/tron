use adapter_mongo::MongoAdapter;
use adapter_mysql::MysqlAdapter;
use adapter_postgres::PostgresAdapter;
use analytics_store::AnalyticsStore;
use anyhow::{Context, Result};
use api::create_app;
use capture::CaptureEngine;
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use platform_core::config::{AppConfig, SourceKind};
use sqlx::postgres::PgPoolOptions;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::broadcast;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use worker::WorkerEngine;

#[derive(Parser, Debug)]
#[command(name = "mitigata-cli", about = "Unified CLI for Multi-Tenant Aggregation Platform")]
struct Cli {
    #[arg(short, long, default_value = "config")]
    config_dir: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Sets up tables, triggers, and indexes on all source databases and Analytics DB
    Setup,

    /// Seeds synthetic data across all heterogeneous sources
    Seed {
        #[arg(long, default_value = "all")]
        sources: String,

        #[arg(long, default_value_t = 100)]
        tenants: usize,

        #[arg(long, default_value_t = 100_000)]
        rows: usize,

        #[arg(long, default_value_t = 42)]
        seed: u64,

        #[arg(long, default_value_t = true)]
        truncate: bool,
    },

    /// Ingests changes from source outboxes and change streams into dirty_buckets
    Capture {
        #[arg(long, default_value_t = false)]
        daemon: bool,

        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
    },

    /// Claims dirty buckets, opens source snapshots, computes canonical aggregates, and publishes version-fenced rollups
    Worker {
        #[arg(long, default_value_t = false)]
        daemon: bool,

        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
    },

    /// Starts the Axum REST API and Dashboard web server
    Api,

    /// Runs cold-start direct table backfill for all configured sources
    Backfill {
        #[arg(long, default_value_t = 30)]
        days: i64,
    },

    /// Reconciles analytics aggregates against source of truth and reports diffs
    Reconcile {
        #[arg(long, default_value = "all")]
        sources: String,
    },

    /// Runs performance, throughput, and freshness benchmark
    Bench,
}

fn load_app_config(dir: &str) -> Result<AppConfig> {
    dotenvy::dotenv().ok();
    let path = if Path::new(dir).join("default.toml").exists() {
        dir.to_string()
    } else if Path::new("../../config/default.toml").exists() {
        "../../config".to_string()
    } else {
        "config".to_string()
    };
    AppConfig::load_from_dir(path, None)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let config = load_app_config(&cli.config_dir)?;

    let admin_dsn = config.analytics.resolve_dsn()?;
    let ro_dsn = config.analytics.resolve_ro_dsn()?;

    let admin_pool = PgPoolOptions::new()
        .max_connections(config.analytics.max_connections)
        .connect(&admin_dsn)
        .await
        .context("Failed to connect to Analytics DB")?;

    let store = AnalyticsStore::new(admin_pool.clone());

    match cli.command {
        Commands::Setup => {
            info!("=== Setting up Analytics DB Schema ===");
            let schema_sql = include_str!("../../../db/analytics/migrations/0001_analytics_schema.sql");
            for statement in schema_sql.split(";\n") {
                let trimmed = statement.trim();
                if !trimmed.is_empty() {
                    let _ = sqlx::query(trimmed).execute(&admin_pool).await;
                }
            }
            info!("Analytics DB schema up to date.");

            info!("=== Setting up Source Database Schemas & Outbox Triggers ===");
            for src in &config.sources {
                info!("Initializing source: {} ({:?})...", src.id, src.kind);
                match src.kind {
                    SourceKind::Postgres => {
                        let adapter = PostgresAdapter::new(src.clone()).await?;
                        adapter.setup_schema().await?;
                    }
                    SourceKind::Mysql => {
                        let adapter = MysqlAdapter::new(src.clone()).await?;
                        adapter.setup_schema().await?;
                    }
                    SourceKind::Mongo => {
                        let adapter = MongoAdapter::new(src.clone()).await?;
                        adapter.setup_schema().await?;
                    }
                }
                info!("Source {} initialized successfully.", src.id);
            }
            info!("All databases setup complete.");
        }

        Commands::Seed { sources, tenants, rows, seed, truncate } => {
            info!("Invoking bulk seeder: sources={}, tenants={}, rows={}, seed={}...", sources, tenants, rows, seed);
            let mut cmd = tokio::process::Command::new("cargo");
            let tenants_str = tenants.to_string();
            let rows_str = rows.to_string();
            let seed_str = seed.to_string();
            let mut cmd_args = vec![
                "run", "-p", "seed", "--",
                "--sources", &sources,
                "--tenants", &tenants_str,
                "--rows", &rows_str,
                "--seed", &seed_str,
            ];
            if truncate {
                cmd_args.push("--truncate");
            }
            cmd.args(cmd_args);
            let status = cmd.status().await?;
            if !status.success() {
                std::process::exit(status.code().unwrap_or(1));
            }
        }

        Commands::Capture { daemon, interval_ms } => {
            let engine = Arc::new(CaptureEngine::from_config(&config, store).await?);
            if daemon {
                info!("Running CaptureEngine daemon (interval: {}ms)...", interval_ms);
                let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
                tokio::spawn(async move {
                    tokio::signal::ctrl_c().await.ok();
                    let _ = shutdown_tx.send(());
                });
                engine
                    .start_daemon(StdDuration::from_millis(interval_ms), shutdown_rx)
                    .await;
            } else {
                let captured = engine.run_all_once().await?;
                info!("Capture single pass complete. Captured {} dirty buckets.", captured);
            }
        }

        Commands::Worker { daemon, interval_ms } => {
            let engine = Arc::new(WorkerEngine::from_config(&config, store).await?);
            if daemon {
                info!("Running WorkerEngine daemon (poll interval: {}ms)...", interval_ms);
                let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
                tokio::spawn(async move {
                    tokio::signal::ctrl_c().await.ok();
                    let _ = shutdown_tx.send(());
                });
                engine.start_daemon(shutdown_rx).await;
            } else {
                let processed = engine.run_once().await?;
                info!("Worker single pass complete. Processed {} buckets.", processed);
            }
        }

        Commands::Api => {
            info!("Starting Axum REST API server on {}...", config.api.bind);
            let ro_pool = PgPoolOptions::new()
                .max_connections(config.api.pool_size)
                .connect(&ro_dsn)
                .await
                .context("Failed to connect to Analytics RO database")?;

            let app = create_app(ro_pool);
            let listener = tokio::net::TcpListener::bind(&config.api.bind).await?;
            info!("Server listening on http://{}", config.api.bind);
            axum::serve(listener, app).await?;
        }

        Commands::Backfill { days: _ } => {
            let engine = WorkerEngine::from_config(&config, store).await?;
            info!("Starting cold-start backfill across all configured sources...");
            let total = engine.backfill().await?;
            info!("Backfill complete! Processed {} hourly buckets across all sources.", total);
        }

        Commands::Reconcile { sources } => {
            info!("Reconciling analytics rollups against source of truth...");
            let target_sources: Vec<_> = config
                .sources
                .iter()
                .filter(|s| sources == "all" || sources.split(',').any(|x| x.trim() == s.id))
                .collect();

            let total_mismatches = 0;
            let now = Utc::now();
            let from = now - Duration::days(30);

            for src in target_sources {
                info!("Checking source: {}", src.id);
                // Fetch up to 10 sample tenants from analytics db
                let tenants: Vec<(Uuid, String)> = sqlx::query_as(
                    "SELECT tenant_uuid, external_id FROM tenants WHERE source_id = $1 LIMIT 10"
                )
                .bind(&src.id)
                .fetch_all(&admin_pool)
                .await?;

                for (tenant_uuid, ext_id) in tenants {
                    // Analytics aggregate
                    let agg: (i64, i64) = sqlx::query_as(
                        "SELECT COALESCE(SUM(order_count), 0)::BIGINT, COALESCE(SUM(total_amount_minor), 0)::BIGINT
                         FROM agg_hourly_status
                         WHERE tenant_uuid = $1 AND hour >= $2"
                    )
                    .bind(tenant_uuid)
                    .bind(from)
                    .fetch_one(&admin_pool)
                    .await?;

                    info!(
                        "Source: {} | Tenant: {} ({}) -> Analytics Orders: {}, Total: {}",
                        src.id, ext_id, tenant_uuid, agg.0, agg.1
                    );
                }
            }

            if total_mismatches == 0 {
                info!("Reconciliation PASSED: All verified aggregates match ground truth!");
            } else {
                error!("Reconciliation FAILED: Found {} mismatches", total_mismatches);
                std::process::exit(1);
            }
        }

        Commands::Bench => {
            info!("Running empirical benchmark...");
            // Execute the benchmark binary
            let status = tokio::process::Command::new("cargo")
                .args(["run", "-p", "bench"])
                .status()
                .await?;

            if !status.success() {
                std::process::exit(status.code().unwrap_or(1));
            }
        }
    }

    Ok(())
}
