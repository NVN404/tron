use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Canonical Bucket Key: uniquely identifies an hourly analytical partition
/// regardless of whether it originated from Postgres, MySQL, or MongoDB.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketKey {
    pub source_id: String,
    pub tenant_id: String, // External tenant ID from source
    pub hour_start_utc: DateTime<Utc>,
}

impl BucketKey {
    pub fn new(
        source_id: impl Into<String>,
        tenant_id: impl Into<String>,
        hour_start_utc: DateTime<Utc>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            tenant_id: tenant_id.into(),
            hour_start_utc,
        }
    }
}

/// Canonical Status Aggregate Row: minor integer units (e.g. cents/paise)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusAggregate {
    pub status: String,
    pub order_count: i64,
    pub sales_units: i64, // Exact integer minor currency units
}

/// Canonical Product Aggregate Row: minor integer units
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductAggregate {
    pub product_id: String,
    pub category: String,
    pub units: i64,
    pub sales_units: i64,
}

/// Complete Canonical Bucket Aggregate:
/// Status breakdowns and product breakdowns for a single (source, tenant, hour) bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketAggregate {
    pub key: BucketKey,
    pub status_aggregates: Vec<StatusAggregate>,
    pub product_aggregates: Vec<ProductAggregate>,
}

/// Opaque cursor representing the position in the change source stream
pub type Cursor = String;

/// Opaque ack handle representing source records/events to acknowledge
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckHandle {
    pub ids: Vec<String>,
}

/// Batch of changes returned by ChangeSource::poll
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeBatch {
    pub bucket_keys: Vec<BucketKey>,
    pub next_cursor: Option<Cursor>,
    pub ack_handle: AckHandle,
}

