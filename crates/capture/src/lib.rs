use adapter_mongo::MongoAdapter;
use adapter_mysql::MysqlAdapter;
use adapter_postgres::PostgresAdapter;
use analytics_store::AnalyticsStore;
use anyhow::{anyhow, Result};
use platform_core::config::{AppConfig, SourceKind};
use platform_core::traits::ChangeSource;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

pub struct CaptureEngine {
    store: AnalyticsStore,
    sources: HashMap<String, Box<dyn ChangeSource>>,
}

impl CaptureEngine {
    pub async fn from_config(config: &AppConfig, store: AnalyticsStore) -> Result<Self> {
        let mut sources: HashMap<String, Box<dyn ChangeSource>> = HashMap::new();

        for src in &config.sources {
            match src.kind {
                SourceKind::Postgres => {
                    let adapter = PostgresAdapter::new(src.clone()).await?;
                    sources.insert(src.id.clone(), Box::new(adapter));
                }
                SourceKind::Mysql => {
                    let adapter = MysqlAdapter::new(src.clone()).await?;
                    sources.insert(src.id.clone(), Box::new(adapter));
                }
                SourceKind::Mongo => {
                    let adapter = MongoAdapter::new(src.clone()).await?;
                    sources.insert(src.id.clone(), Box::new(adapter));
                }
            }
        }

        Ok(Self { store, sources })
    }

    /// Single poll-and-commit pass for a specific source.
    pub async fn run_once(&self, source_id: &str) -> Result<usize> {
        let source = self
            .sources
            .get(source_id)
            .ok_or_else(|| anyhow!("Source not found: {}", source_id))?;

        let cursor = self.store.get_source_cursor(source_id).await?;
        let batch = source.poll(cursor).await?;

        let count = batch.bucket_keys.len();
        if !batch.bucket_keys.is_empty() || batch.next_cursor.is_some() {
            // Commit to analytics store FIRST
            self.store
                .upsert_dirty_and_cursor(source_id, &batch.bucket_keys, batch.next_cursor.as_ref())
                .await?;

            // Acknowledge source ONLY after analytics transaction committed
            source.ack(&batch).await?;
        }

        Ok(count)
    }

    /// Single pass over all configured sources.
    pub async fn run_all_once(&self) -> Result<usize> {
        let mut total = 0;
        for src_id in self.sources.keys() {
            match self.run_once(src_id).await {
                Ok(n) => total += n,
                Err(e) => tracing::error!("Capture run_once error for {}: {}", src_id, e),
            }
        }
        Ok(total)
    }

    /// Runs continuous capture daemon across all sources until shutdown signal.
    pub async fn start_daemon(
        self: Arc<Self>,
        poll_interval: Duration,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        tracing::info!("Starting CaptureEngine daemon for {} sources...", self.sources.len());

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    tracing::info!("CaptureEngine received shutdown signal, stopping...");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {
                    let _ = self.run_all_once().await;
                }
            }
        }
    }
}
