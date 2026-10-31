use anyhow::{Context, Result};
use chrono::DateTime;
use clap::{Parser, Subcommand};
use common::{init_telemetry, AppConfig};
use oracle::Oracle;
use sqlx::PgPool;
use std::time::Instant;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "oracle", about = "Direct-SQL analytics oracle CLI for ground-truth verification")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Calculate count and total revenue
    Count {
        #[arg(long)]
        tenant: Uuid,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
    /// Calculate trend over time (hourly or daily)
    Trend {
        #[arg(long)]
        tenant: Uuid,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "hour")]
        grain: String,
    },
    /// Calculate top N products by revenue
    TopProducts {
        #[arg(long)]
        tenant: Uuid,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value_t = 10)]
        n: i64,
    },
    /// Calculate category and status breakdown
    Breakdown {
        #[arg(long)]
        tenant: Uuid,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_telemetry("oracle");
    let cli = Cli::parse();
    let config = AppConfig::from_env()?;
    let pool = PgPool::connect(&config.source_database_url).await
        .context("Failed to connect to source database")?;

    match cli.command {
        Commands::Count { tenant, from, to } => {
            let from_dt = DateTime::parse_from_rfc3339(&from)?.to_utc();
            let to_dt = DateTime::parse_from_rfc3339(&to)?.to_utc();

            let start = Instant::now();
            let result = Oracle::count_and_total(&pool, tenant, from_dt, to_dt).await?;
            let duration = start.elapsed();

            println!("{}", serde_json::to_string_pretty(&result)?);
            eprintln!("⏱️ Query execution time on source DB: {:.2}ms", duration.as_secs_f64() * 1000.0);
        }
        Commands::Trend { tenant, from, to, grain } => {
            let from_dt = DateTime::parse_from_rfc3339(&from)?.to_utc();
            let to_dt = DateTime::parse_from_rfc3339(&to)?.to_utc();

            let start = Instant::now();
            let result = Oracle::trend(&pool, tenant, from_dt, to_dt, &grain).await?;
            let duration = start.elapsed();

            println!("{}", serde_json::to_string_pretty(&result)?);
            eprintln!("⏱️ Query execution time on source DB: {:.2}ms (returned {} buckets)", duration.as_secs_f64() * 1000.0, result.len());
        }
        Commands::TopProducts { tenant, from, to, n } => {
            let from_dt = DateTime::parse_from_rfc3339(&from)?.to_utc();
            let to_dt = DateTime::parse_from_rfc3339(&to)?.to_utc();

            let start = Instant::now();
            let result = Oracle::top_products(&pool, tenant, from_dt, to_dt, n).await?;
            let duration = start.elapsed();

            println!("{}", serde_json::to_string_pretty(&result)?);
            eprintln!("⏱️ Query execution time on source DB: {:.2}ms", duration.as_secs_f64() * 1000.0);
        }
        Commands::Breakdown { tenant, from, to } => {
            let from_dt = DateTime::parse_from_rfc3339(&from)?.to_utc();
            let to_dt = DateTime::parse_from_rfc3339(&to)?.to_utc();

            let start = Instant::now();
            let result = Oracle::breakdown(&pool, tenant, from_dt, to_dt).await?;
            let duration = start.elapsed();

            println!("{}", serde_json::to_string_pretty(&result)?);
            eprintln!("⏱️ Query execution time on source DB: {:.2}ms", duration.as_secs_f64() * 1000.0);
        }
    }

    Ok(())
}
