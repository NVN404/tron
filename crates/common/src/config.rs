use crate::error::{AppError, Result};
use std::env;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub source_database_url: String,
    pub analytics_database_url: String,
    pub analytics_ro_database_url: String,
    pub api_host: String,
    pub api_port: u16,
    pub worker_batch_size: i64,
    pub worker_lease_seconds: i64,
    pub worker_poll_interval_ms: u64,
    pub timezone: String,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        // Load .env if present
        let _ = dotenvy::dotenv();

        let source_database_url = env::var("SOURCE_DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@localhost:5433/source_db".to_string()
        });

        let analytics_database_url = env::var("ANALYTICS_DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@localhost:5434/analytics_db".to_string()
        });

        let analytics_ro_database_url = env::var("ANALYTICS_RO_DATABASE_URL").unwrap_or_else(|_| {
            "postgres://api_ro:api_ro_secret@localhost:5434/analytics_db".to_string()
        });

        let api_host = env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        
        let api_port = env::var("API_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .map_err(|e| AppError::Config(format!("Invalid API_PORT: {}", e)))?;

        let worker_batch_size = env::var("WORKER_BATCH_SIZE")
            .unwrap_or_else(|_| "1000".to_string())
            .parse::<i64>()
            .map_err(|e| AppError::Config(format!("Invalid WORKER_BATCH_SIZE: {}", e)))?;

        let worker_lease_seconds = env::var("WORKER_LEASE_SECONDS")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<i64>()
            .map_err(|e| AppError::Config(format!("Invalid WORKER_LEASE_SECONDS: {}", e)))?;

        let worker_poll_interval_ms = env::var("WORKER_POLL_INTERVAL_MS")
            .unwrap_or_else(|_| "500".to_string())
            .parse::<u64>()
            .map_err(|e| AppError::Config(format!("Invalid WORKER_POLL_INTERVAL_MS: {}", e)))?;

        let timezone = env::var("TIMEZONE").unwrap_or_else(|_| "Asia/Kolkata".to_string());

        Ok(Self {
            source_database_url,
            analytics_database_url,
            analytics_ro_database_url,
            api_host,
            api_port,
            worker_batch_size,
            worker_lease_seconds,
            worker_poll_interval_ms,
            timezone,
        })
    }
}
