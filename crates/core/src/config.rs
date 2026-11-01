use crate::identifier::ValidatedIdentifier;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Postgres,
    Mysql,
    Mongo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CaptureMode {
    Outbox,
    Changestream,
    Polling,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMappingConfig {
    pub entity: ValidatedIdentifier,
    pub tenant_id_field: ValidatedIdentifier,
    pub id_field: ValidatedIdentifier,
    pub status_field: ValidatedIdentifier,
    pub amount_field: ValidatedIdentifier,
    pub product_id_field: ValidatedIdentifier,
    pub category_field: ValidatedIdentifier,
    pub created_at_field: ValidatedIdentifier,
    #[serde(default = "default_amount_scale")]
    pub amount_scale: u32,
    pub sales_counting_statuses: Vec<String>,
}

fn default_amount_scale() -> u32 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub id: String,
    pub kind: SourceKind,
    pub dsn_env: String,
    pub capture: CaptureMode,
    #[serde(default = "default_timezone")]
    pub timezone: String,
    pub mapping: SourceMappingConfig,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

impl SourceConfig {
    /// Resolves the actual DSN value checking for `<dsn_env>_FILE` first, then `<dsn_env>`
    pub fn resolve_dsn(&self) -> Result<String> {
        resolve_secret_from_env(&self.dsn_env)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsConfig {
    pub dsn_env: String,
    pub ro_dsn_env: String,
    #[serde(default = "default_analytics_pool")]
    pub max_connections: u32,
}

fn default_analytics_pool() -> u32 {
    20
}

impl AnalyticsConfig {
    pub fn resolve_dsn(&self) -> Result<String> {
        resolve_secret_from_env(&self.dsn_env)
    }

    pub fn resolve_ro_dsn(&self) -> Result<String> {
        resolve_secret_from_env(&self.ro_dsn_env)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub batch_size: usize,
    pub lease_secs: u64,
    pub heartbeat_secs: u64,
    pub max_attempts: u32,
    pub backoff_base_ms: u64,
    pub concurrency: usize,
    #[serde(default)]
    pub per_source_limits: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub bind: String,
    pub pool_size: u32,
    pub rate_limit_per_min: u32,
    pub max_range_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedConfig {
    pub rng_seed: u64,
    pub tenants_per_source: usize,
    pub total_rows: usize,
    pub skew: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub concurrency: usize,
    pub requests: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub analytics: AnalyticsConfig,
    pub sources: Vec<SourceConfig>,
    pub worker: WorkerConfig,
    pub api: ApiConfig,
    pub seed: SeedConfig,
    pub benchmark: BenchmarkConfig,
}

/// Secret resolver: supports direct env vars or *_FILE file pointers for Docker/K8s secrets
pub fn resolve_secret_from_env(var_name: &str) -> Result<String> {
    let file_var = format!("{}_FILE", var_name);
    if let Ok(file_path) = env::var(&file_var) {
        let content = fs::read_to_string(&file_path)
            .with_context(|| format!("Failed to read secret from file pointed by {}", file_var))?;
        return Ok(content.trim().to_string());
    }

    env::var(var_name).with_context(|| {
        format!(
            "Missing secret environment variable '{}' (and '{}_FILE' is not set)",
            var_name, var_name
        )
    })
}

impl AppConfig {
    /// Layered config loader:
    /// 1. `config/default.toml`
    /// 2. `config/<env>.toml` (if APP_ENV or CONFIG_ENV is set)
    /// 3. Environment overrides
    pub fn load_from_dir<P: AsRef<Path>>(dir: P, env_name: Option<&str>) -> Result<Self> {
        let default_path = dir.as_ref().join("default.toml");
        let default_str = fs::read_to_string(&default_path)
            .with_context(|| format!("Failed to read {}", default_path.display()))?;

        let mut default_val: toml::Value = toml::from_str(&default_str)
            .with_context(|| format!("Failed to parse TOML in {}", default_path.display()))?;

        // Merge env-specific overlay if provided
        let target_env = env_name
            .map(|s| s.to_string())
            .or_else(|| env::var("APP_ENV").ok())
            .or_else(|| env::var("CONFIG_ENV").ok());

        if let Some(env_str) = target_env {
            let env_path = dir.as_ref().join(format!("{}.toml", env_str));
            if env_path.exists() {
                let env_str_content = fs::read_to_string(&env_path)
                    .with_context(|| format!("Failed to read {}", env_path.display()))?;
                let env_val: toml::Value = toml::from_str(&env_str_content)
                    .with_context(|| format!("Failed to parse TOML in {}", env_path.display()))?;
                merge_toml_values(&mut default_val, env_val);
            }
        }

        let config: AppConfig = default_val
            .try_into()
            .context("Configuration validation failed during deserialization")?;

        config.validate()?;
        Ok(config)
    }

    pub fn load() -> Result<Self> {
        Self::load_from_dir("config", None)
    }

    /// Explicit validator enforcing all architectural and security constraints
    pub fn validate(&self) -> Result<()> {
        if self.sources.is_empty() {
            return Err(anyhow!("Config validation error: [[sources]] must contain at least one configured database"));
        }

        let mut seen_ids = std::collections::HashSet::new();
        for src in &self.sources {
            if src.id.trim().is_empty() {
                return Err(anyhow!("Config validation error: source id cannot be empty"));
            }
            if !seen_ids.insert(&src.id) {
                return Err(anyhow!("Config validation error: duplicate source id '{}'", src.id));
            }
            if src.mapping.sales_counting_statuses.is_empty() {
                return Err(anyhow!("Config validation error: source '{}' must define at least one status in sales_counting_statuses", src.id));
            }
        }

        if self.worker.batch_size == 0 {
            return Err(anyhow!("Config validation error: worker.batch_size must be > 0"));
        }
        if self.worker.lease_secs == 0 {
            return Err(anyhow!("Config validation error: worker.lease_secs must be > 0"));
        }
        if self.api.max_range_days == 0 {
            return Err(anyhow!("Config validation error: api.max_range_days must be > 0"));
        }

        Ok(())
    }
}

fn merge_toml_values(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base_table), toml::Value::Table(overlay_table)) => {
            for (k, v) in overlay_table {
                if let Some(existing) = base_table.get_mut(&k) {
                    merge_toml_values(existing, v);
                } else {
                    base_table.insert(k, v);
                }
            }
        }
        (b, o) => *b = o,
    }
}

