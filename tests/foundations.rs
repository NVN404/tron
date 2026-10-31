use common::{AppConfig, Result};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn test_source_db_connectivity() -> Result<()> {
    let config = AppConfig::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.source_database_url)
        .await?;

    let row: (i64,) = sqlx::query_as("SELECT 1")
        .fetch_one(&pool)
        .await?;

    assert_eq!(row.0, 1);
    Ok(())
}

#[tokio::test]
async fn test_analytics_db_connectivity() -> Result<()> {
    let config = AppConfig::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.analytics_database_url)
        .await?;

    let row: (i64,) = sqlx::query_as("SELECT 1")
        .fetch_one(&pool)
        .await?;

    assert_eq!(row.0, 1);
    Ok(())
}

#[tokio::test]
async fn test_analytics_ro_role_connectivity() -> Result<()> {
    let config = AppConfig::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.analytics_ro_database_url)
        .await?;

    let row: (String,) = sqlx::query_as("SELECT current_user")
        .fetch_one(&pool)
        .await?;

    assert_eq!(row.0, "api_ro");
    Ok(())
}
