use clap::Parser;
use common::{init_telemetry, AppConfig};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "seed", about = "High-performance synthetic order seeder for multi-tenant aggregation")]
struct Args {
    #[arg(long, default_value_t = 100)]
    tenants: usize,

    #[arg(long, default_value_t = 1_000_000)]
    rows: usize,

    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("seed");
    let args = Args::parse();
    let config = AppConfig::from_env()?;
    info!("Seeder initialized with {} rows, {} tenants (seed={})", args.rows, args.tenants, args.seed);
    info!("Source URL: {}", config.source_database_url);
    info!("Full 10M COPY-based ingestion will be implemented in Phase 2.");
    Ok(())
}
