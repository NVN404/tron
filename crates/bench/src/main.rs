use common::{init_telemetry, AppConfig};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("bench");
    let config = AppConfig::from_env()?;
    info!("Benchmark harness initialized. Target API: http://{}:{}", config.api_host, config.api_port);
    info!("Benchmark suite will be executed in Phase 11.");
    Ok(())
}
