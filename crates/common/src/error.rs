use thiserror::Error;

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Tenant isolation violation: {0}")]
    TenantViolation(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Stale version conflict: incoming version {incoming} <= published version {published}")]
    StaleVersion { incoming: i64, published: i64 },

    #[error("Worker lease error: {0}")]
    LeaseError(String),

    #[error("Reconciliation drift detected: {0}")]
    ReconciliationDrift(String),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}
