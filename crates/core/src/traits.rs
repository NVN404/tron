use crate::types::{BucketAggregate, BucketKey, ChangeBatch, Cursor};
use anyhow::Result;
use async_trait::async_trait;

/// Core ChangeSource trait: isolates change detection (Outbox, Change Streams, Polling)
#[async_trait]
pub trait ChangeSource: Send + Sync {
    /// Poll for dirty bucket keys starting from cursor
    async fn poll(&self, cursor: Option<Cursor>) -> Result<ChangeBatch>;

    /// Acknowledge successfully committed changes
    async fn ack(&self, batch: &ChangeBatch) -> Result<()>;
}

/// Core BucketReader trait: isolates authoritative snapshot reads across Postgres, MySQL, and MongoDB
#[async_trait]
pub trait BucketReader: Send + Sync {
    /// Authoritatively compute an hourly bucket directly against the source
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate>;

    /// Open an isolated consistent snapshot transaction/session
    async fn begin_snapshot(&self) -> Result<Box<dyn Snapshot>>;
}

/// Consistent snapshot handle
#[async_trait]
pub trait Snapshot: Send + Sync {
    /// Compute bucket against this consistent snapshot view
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate>;
}

