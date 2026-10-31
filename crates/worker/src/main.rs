use common::{init_telemetry, AppConfig};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_telemetry("worker");
    let config = AppConfig::from_env()?;
    info!("Aggregation worker initialized with config: {:?}", config);
    info!("Worker foundation ready. Core protocol will be implemented in Phase 6.");
    Ok(())
}
