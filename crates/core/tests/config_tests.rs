use platform_core::{resolve_secret_from_env, AppConfig};
use std::env;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_config_loading_and_env_overlay() {
    let dir = if std::path::Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    let config = AppConfig::load_from_dir(dir, Some("test")).expect("Failed to load test config");
    
    // Loaded from default.toml
    assert_eq!(config.analytics.dsn_env, "ANALYTICS_DATABASE_URL");
    assert_eq!(config.sources.len(), 4);
    assert_eq!(config.sources[0].id, "pg_source_1");
    assert_eq!(config.sources[2].id, "mysql_source");
    assert_eq!(config.sources[3].id, "mongo_source");

    // Overlaid from test.toml
    assert_eq!(config.worker.batch_size, 50);
    assert_eq!(config.worker.lease_secs, 5);
    assert_eq!(config.seed.total_rows, 100);
}

#[test]
fn test_missing_key_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let broken_toml = r#"
        [analytics]
        dsn_env = "ANALYTICS_DB"
        ro_dsn_env = "ANALYTICS_RO_DB"
        # missing [worker], [[sources]], [api], [seed], [benchmark]
    "#;
    std::fs::write(temp_dir.path().join("default.toml"), broken_toml).unwrap();

    let err = AppConfig::load_from_dir(temp_dir.path(), None).unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("missing field") || err_msg.contains("validation failed"),
        "Error must describe missing keys: {}",
        err_msg
    );
}

#[test]
fn test_bad_identifier_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let bad_ident_toml = r#"
        [analytics]
        dsn_env = "ANALYTICS_DB"
        ro_dsn_env = "ANALYTICS_RO_DB"

        [[sources]]
        id = "bad_source"
        kind = "postgres"
        dsn_env = "SRC_DB"
        capture = "outbox"
        [sources.mapping]
        entity = "orders; DROP TABLE;" # SQL INJECTION ATTEMPT
        tenant_id_field = "tenant_id"
        id_field = "id"
        status_field = "status"
        amount_field = "amount"
        product_id_field = "product_id"
        category_field = "category"
        created_at_field = "created_at"
        sales_counting_statuses = ["completed"]

        [worker]
        batch_size = 10
        lease_secs = 10
        heartbeat_secs = 5
        max_attempts = 3
        backoff_base_ms = 100
        concurrency = 2

        [api]
        bind = "0.0.0.0:8080"
        pool_size = 10
        rate_limit_per_min = 100
        max_range_days = 30

        [seed]
        rng_seed = 1
        tenants_per_source = 1
        total_rows = 10
        skew = 1.0

        [benchmark]
        concurrency = 1
        requests = 1
    "#;
    std::fs::write(temp_dir.path().join("default.toml"), bad_ident_toml).unwrap();

    let err = AppConfig::load_from_dir(temp_dir.path(), None).unwrap_err();
    let err_msg = format!("{:#}", err);
    assert!(
        err_msg.contains("invalid") || err_msg.contains("Identifier"),
        "Error must reject bad SQL identifier: {}",
        err_msg
    );
}

#[test]
fn test_secret_file_resolution() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "postgres://super_secret_user:super_secret_pw@localhost:5432/db").unwrap();

    let var_name = "TEST_DB_URL";
    let file_var = format!("{}_FILE", var_name);
    env::set_var(&file_var, temp_file.path());
    // Also set direct var with different value to verify _FILE takes priority
    env::set_var(var_name, "postgres://ignored@localhost/db");

    let resolved = resolve_secret_from_env(var_name).unwrap();
    assert_eq!(
        resolved,
        "postgres://super_secret_user:super_secret_pw@localhost:5432/db"
    );

    // Clean up
    env::remove_var(&file_var);
    env::remove_var(var_name);
}

#[test]
fn test_env_secret_fallback() {
    let var_name = "TEST_DIRECT_SECRET";
    env::set_var(var_name, "mysql://user:pass@localhost:3306/db");

    let resolved = resolve_secret_from_env(var_name).unwrap();
    assert_eq!(resolved, "mysql://user:pass@localhost:3306/db");

    env::remove_var(var_name);
}
