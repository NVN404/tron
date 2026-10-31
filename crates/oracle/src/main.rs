use clap::Parser;
use common::{init_telemetry, AppConfig};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "oracle", about = "Direct-SQL analytics oracle CLI for ground-truth verification")]
enum Commands {
    Count {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("oracle");
    let config = AppConfig::from_env()?;
    info!("Oracle initialized with source DB: {}", config.source_database_url);
    info!("Direct SQL calculation methods will be implemented in Phase 3.");
    Ok(())
}
