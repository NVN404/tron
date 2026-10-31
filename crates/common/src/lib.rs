//! Common types, configuration, error handling, and telemetry for the Multi-Tenant Aggregation Layer.

pub mod config;
pub mod error;
pub mod models;
pub mod telemetry;

pub use config::AppConfig;
pub use error::{AppError, Result};
pub use telemetry::init_telemetry;
