use adapter_mongo::MongoAdapter;
use adapter_mysql::MysqlAdapter;
use adapter_postgres::PostgresAdapter;
use analytics_store::{AnalyticsStore, ClaimedBucket, PublishResult};
use anyhow::{anyhow, Result};
use chrono::TimeZone;
use platform_core::config::{AppConfig, SourceConfig, SourceKind, WorkerConfig};
use platform_core::traits::BucketReader;
use platform_core::types::BucketKey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Semaphore};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct WorkerEngine {
    store: AnalyticsStore,
    readers: Arc<HashMap<String, Box<dyn BucketReader>>>,
    source_configs: Arc<HashMap<String, SourceConfig>>,
    config: WorkerConfig,
    worker_id: String,
}

impl WorkerEngine {
    pub async fn from_config(config: &AppConfig, store: AnalyticsStore) -> Result<Self> {
        let mut readers: HashMap<String, Box<dyn BucketReader>> = HashMap::new();
        let mut source_configs: HashMap<String, SourceConfig> = HashMap::new();

        for src in &config.sources {
            source_configs.insert(src.id.clone(), src.clone());
            match src.kind {
                SourceKind::Postgres => {
                    let adapter = PostgresAdapter::new(src.clone()).await?;
                    readers.insert(src.id.clone(), Box::new(adapter));
                }
                SourceKind::Mysql => {
                    let adapter = MysqlAdapter::new(src.clone()).await?;
                    readers.insert(src.id.clone(), Box::new(adapter));
                }
                SourceKind::Mongo => {
                    let adapter = MongoAdapter::new(src.clone()).await?;
                    readers.insert(src.id.clone(), Box::new(adapter));
                }
            }
        }

        let worker_id = format!("worker_{}", Uuid::new_v4().simple());

        Ok(Self {
            store,
            readers: Arc::new(readers),
            source_configs: Arc::new(source_configs),
            config: config.worker.clone(),
            worker_id,
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn store(&self) -> &AnalyticsStore {
        &self.store
    }

    /// Processes a single claimed bucket with lease heartbeat and snapshot recompute.
    pub async fn process_bucket(&self, claim: ClaimedBucket) -> Result<PublishResult> {
        let (heartbeat_cancel_tx, mut heartbeat_cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let store = self.store.clone();
        let token = claim.claim_token.clone();
        let lease_secs = self.config.lease_secs as i64;
        let heartbeat_interval = Duration::from_secs(self.config.heartbeat_secs.max(1));

        // Background heartbeat task
        let heartbeat_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.tick().await; // consume initial tick
            loop {
                tokio::select! {
                    _ = &mut heartbeat_cancel_rx => break,
                    _ = ticker.tick() => {
                        if let Err(e) = store.heartbeat(&token, lease_secs).await {
                            warn!("Heartbeat failed for token {}: {}", token, e);
                            break;
                        }
                    }
                }
            }
        });

        // Reader lookup
        let reader = self
            .readers
            .get(&claim.key.source_id)
            .ok_or_else(|| anyhow!("No reader found for source {}", claim.key.source_id));

        let res = match reader {
            Ok(r) => {
                // Open consistent snapshot and recompute
                match r.begin_snapshot().await {
                    Ok(snapshot) => match snapshot.compute(&claim.key).await {
                        Ok(aggregate) => self.store.publish_bucket(&claim, &aggregate).await,
                        Err(e) => Err(anyhow!("Compute error: {:#}", e)),
                    },
                    Err(e) => Err(anyhow!("Snapshot begin error: {:#}", e)),
                }
            }
            Err(e) => Err(e),
        };

        // Terminate heartbeat
        let _ = heartbeat_cancel_tx.send(());
        let _ = heartbeat_handle.await;

        match res {
            Ok(pub_res) => Ok(pub_res),
            Err(err) => {
                let err_msg = format!("{:#}", err);
                error!("Worker error on bucket {:?}: {}", claim.key, err_msg);
                self.store
                    .handle_failure(
                        &claim,
                        &err_msg,
                        self.config.max_attempts as i32,
                        self.config.backoff_base_ms,
                    )
                    .await?;
                Err(err)
            }
        }
    }

    /// Claims and processes up to `batch_size` buckets.
    pub async fn run_once(&self) -> Result<usize> {
        let claims = self
            .store
            .claim_buckets(
                None,
                &self.worker_id,
                self.config.batch_size as i64,
                self.config.lease_secs as i64,
            )
            .await?;

        if claims.is_empty() {
            return Ok(0);
        }

        let total = claims.len();
        let semaphore = Arc::new(Semaphore::new(self.config.concurrency.max(1)));
        let mut handles = Vec::new();

        for claim in claims {
            let permit = semaphore.clone().acquire_owned().await?;
            let this = self.clone();

            handles.push(tokio::spawn(async move {
                let _permit = permit;
                let _ = this.process_bucket(claim).await;
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        Ok(total)
    }

    /// Drains all currently queued dirty buckets until empty.
    pub async fn drain_all(&self) -> Result<usize> {
        let mut total = 0;
        loop {
            let processed = self.run_once().await?;
            if processed == 0 {
                break;
            }
            total += processed;
        }
        Ok(total)
    }

    /// Initial backfill: populates dirty_buckets directly from raw source records.
    pub async fn backfill(&self) -> Result<usize> {
        let mut total_buckets = 0;

        for (src_id, src_cfg) in self.source_configs.as_ref() {
            let entity = src_cfg.mapping.entity.as_str();
            let tenant_field = src_cfg.mapping.tenant_id_field.as_str();
            let created_field = src_cfg.mapping.created_at_field.as_str();

            let mut bucket_keys = Vec::new();

            match src_cfg.kind {
                SourceKind::Postgres => {
                    let dsn = src_cfg.resolve_dsn()?;
                    let pool = sqlx::PgPool::connect(&dsn).await?;
                    let sql = format!(
                        "SELECT DISTINCT {tenant_field} as tenant_id, date_trunc('hour', {created_field}) as hour FROM {entity};"
                    );
                    let rows = sqlx::query(&sql).fetch_all(&pool).await?;
                    for r in rows {
                        let tenant_id: String = sqlx::Row::get(&r, "tenant_id");
                        let hour: chrono::DateTime<chrono::Utc> = sqlx::Row::get(&r, "hour");
                        bucket_keys.push(BucketKey::new(src_id, tenant_id, hour));
                    }
                }
                SourceKind::Mysql => {
                    let dsn = src_cfg.resolve_dsn()?;
                    let pool = sqlx::MySqlPool::connect(&dsn).await?;
                    let sql = format!(
                        "SELECT DISTINCT {tenant_field} as tenant_id, DATE_FORMAT({created_field}, '%Y-%m-%d %H:00:00') as hour FROM {entity};"
                    );
                    let rows = sqlx::query(&sql).fetch_all(&pool).await?;
                    for r in rows {
                        let tenant_id: String = sqlx::Row::get(&r, "tenant_id");
                        let hour_str: String = sqlx::Row::get(&r, "hour");
                        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&hour_str, "%Y-%m-%d %H:%M:%S") {
                            let hour = chrono::DateTime::from_naive_utc_and_offset(naive, chrono::Utc);
                            bucket_keys.push(BucketKey::new(src_id, tenant_id, hour));
                        }
                    }
                }
                SourceKind::Mongo => {
                    let dsn = src_cfg.resolve_dsn()?;
                    let client = mongodb::Client::with_uri_str(&dsn).await?;
                    let db_name = mongodb::options::ClientOptions::parse(&dsn)
                        .await?
                        .default_database
                        .unwrap_or_else(|| "source_db".to_string());
                    let coll = client.database(&db_name).collection::<mongodb::bson::Document>(entity);

                    let pipeline = vec![
                        mongodb::bson::doc! {
                            "$group": {
                                "_id": {
                                    "tenant_id": format!("${}", tenant_field),
                                    "year": { "$year": format!("${}", created_field) },
                                    "month": { "$month": format!("${}", created_field) },
                                    "day": { "$dayOfMonth": format!("${}", created_field) },
                                    "hour": { "$hour": format!("${}", created_field) }
                                }
                            }
                        }
                    ];
                    let mut cursor = coll.aggregate(pipeline).await?;
                    while let Some(Ok(doc)) = futures::StreamExt::next(&mut cursor).await {
                        if let Ok(id_doc) = doc.get_document("_id") {
                            let tenant_id = id_doc.get_str("tenant_id").unwrap_or_default().to_string();
                            let y = id_doc.get_i32("year").unwrap_or(2026);
                            let m = id_doc.get_i32("month").unwrap_or(1) as u32;
                            let d = id_doc.get_i32("day").unwrap_or(1) as u32;
                            let h = id_doc.get_i32("hour").unwrap_or(0) as u32;
                            if let Some(hour) = chrono::Utc.with_ymd_and_hms(y, m, d, h, 0, 0).single() {
                                bucket_keys.push(BucketKey::new(src_id, tenant_id, hour));
                            }
                        }
                    }
                }
            }

            if !bucket_keys.is_empty() {
                total_buckets += bucket_keys.len();
                self.store.upsert_dirty_and_cursor(src_id, &bucket_keys, None).await?;
            }
        }

        info!("Backfill marked {} distinct buckets dirty across all sources", total_buckets);
        self.drain_all().await?;

        Ok(total_buckets)
    }

    /// Daemon execution loop.
    pub async fn start_daemon(
        self: Arc<Self>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        info!("Starting WorkerEngine daemon (worker_id={})...", self.worker_id);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("WorkerEngine received shutdown signal, stopping...");
                    break;
                }
                res = self.run_once() => {
                    match res {
                        Ok(0) => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        Ok(n) => {
                            info!("Worker {} processed {} buckets", self.worker_id, n);
                        }
                        Err(e) => {
                            error!("Worker loop error: {}", e);
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                        }
                    }
                }
            }
        }
    }
}
