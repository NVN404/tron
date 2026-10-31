use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketTarget {
    pub tenant_id: Uuid,
    pub bucket_hour: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ClaimedRow {
    pub id: i64,
    pub tenant_id: Uuid,
    pub bucket_hour: DateTime<Utc>,
    pub attempts: i32,
}

#[derive(Debug, Clone)]
pub struct ClaimedBatch {
    pub claim_token: Uuid,
    pub claimed_by: String,
    pub rows: Vec<ClaimedRow>,
}

#[derive(Debug, Clone)]
pub struct StatusRollup {
    pub status: String,
    pub order_count: i64,
    pub sales_paise: i64,
}

#[derive(Debug, Clone)]
pub struct ProductRollup {
    pub product_id: Uuid,
    pub category: String,
    pub units: i64,
    pub sales_paise: i64,
}

#[derive(Debug, Clone)]
pub struct BucketComputation {
    pub target: BucketTarget,
    pub version: i64, // max outbox id covered
    pub status_rollups: Vec<StatusRollup>,
    pub product_rollups: Vec<ProductRollup>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PublishResult {
    Published,
    RejectedStale,
}

#[derive(Debug, Default, Clone)]
pub struct WorkerMetrics {
    pub claimed_count: u64,
    pub published_count: u64,
    pub rejected_stale_count: u64,
    pub failed_count: u64,
}

