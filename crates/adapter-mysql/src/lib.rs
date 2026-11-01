use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use platform_core::config::SourceConfig;
use platform_core::traits::{BucketReader, ChangeSource, Snapshot};
use platform_core::types::{
    AckHandle, BucketAggregate, BucketKey, ChangeBatch, Cursor, ProductAggregate, StatusAggregate,
};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::Row;
use std::collections::HashSet;

pub struct MysqlAdapter {
    config: SourceConfig,
    pool: MySqlPool,
}

impl MysqlAdapter {
    pub async fn new(config: SourceConfig) -> Result<Self> {
        let dsn = config.resolve_dsn()?;
        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect(&dsn)
            .await
            .with_context(|| format!("Failed to connect to MySQL source {}", config.id))?;

        Ok(Self { config, pool })
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Sets up tables, outbox, and triggers on the MySQL source database.
    pub async fn setup_schema(&self) -> Result<()> {
        let entity = self.config.mapping.entity.as_str();
        let tenant_field = self.config.mapping.tenant_id_field.as_str();
        let id_field = self.config.mapping.id_field.as_str();
        let status_field = self.config.mapping.status_field.as_str();
        let amount_field = self.config.mapping.amount_field.as_str();
        let product_field = self.config.mapping.product_id_field.as_str();
        let category_field = self.config.mapping.category_field.as_str();
        let created_field = self.config.mapping.created_at_field.as_str();

        // 1. Source entity table
        let create_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {entity} (
                {id_field} VARCHAR(64) PRIMARY KEY,
                {tenant_field} VARCHAR(64) NOT NULL,
                {product_field} VARCHAR(64) NOT NULL,
                {category_field} VARCHAR(64) NOT NULL,
                {amount_field} BIGINT NOT NULL,
                {status_field} VARCHAR(32) NOT NULL,
                {created_field} DATETIME NOT NULL,
                INDEX idx_{entity}_tenant_created ({tenant_field}, {created_field})
            ) ENGINE=InnoDB;
            "#
        );
        sqlx::query(&create_table_sql).execute(&self.pool).await?;

        // 2. Outbox table
        let outbox_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS _{entity}_outbox (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tenant_id VARCHAR(64) NOT NULL,
                hour_start DATETIME NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_{entity}_outbox_id (id)
            ) ENGINE=InnoDB;
            "#
        );
        sqlx::query(&outbox_table_sql).execute(&self.pool).await?;

        // 3. Triggers (Drop and Recreate using raw_sql text protocol)
        let drop_insert_trg = format!("DROP TRIGGER IF EXISTS trg_{entity}_ai;");
        let drop_update_trg = format!("DROP TRIGGER IF EXISTS trg_{entity}_au;");
        let drop_delete_trg = format!("DROP TRIGGER IF EXISTS trg_{entity}_ad;");
        sqlx::raw_sql(&drop_insert_trg).execute(&self.pool).await.ok();
        sqlx::raw_sql(&drop_update_trg).execute(&self.pool).await.ok();
        sqlx::raw_sql(&drop_delete_trg).execute(&self.pool).await.ok();

        let trg_insert = format!(
            r#"
            CREATE TRIGGER trg_{entity}_ai AFTER INSERT ON {entity}
            FOR EACH ROW
            BEGIN
                INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                VALUES (NEW.{tenant_field}, DATE_FORMAT(NEW.{created_field}, '%Y-%m-%d %H:00:00'));
            END;
            "#
        );
        sqlx::raw_sql(&trg_insert).execute(&self.pool).await?;

        let trg_update = format!(
            r#"
            CREATE TRIGGER trg_{entity}_au AFTER UPDATE ON {entity}
            FOR EACH ROW
            BEGIN
                INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                VALUES (OLD.{tenant_field}, DATE_FORMAT(OLD.{created_field}, '%Y-%m-%d %H:00:00'));
                IF OLD.{tenant_field} != NEW.{tenant_field} OR DATE_FORMAT(OLD.{created_field}, '%Y-%m-%d %H:00:00') != DATE_FORMAT(NEW.{created_field}, '%Y-%m-%d %H:00:00') THEN
                    INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                    VALUES (NEW.{tenant_field}, DATE_FORMAT(NEW.{created_field}, '%Y-%m-%d %H:00:00'));
                END IF;
            END;
            "#
        );
        sqlx::raw_sql(&trg_update).execute(&self.pool).await?;

        let trg_delete = format!(
            r#"
            CREATE TRIGGER trg_{entity}_ad AFTER DELETE ON {entity}
            FOR EACH ROW
            BEGIN
                INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                VALUES (OLD.{tenant_field}, DATE_FORMAT(OLD.{created_field}, '%Y-%m-%d %H:00:00'));
            END;
            "#
        );
        sqlx::raw_sql(&trg_delete).execute(&self.pool).await?;

        Ok(())
    }
}

#[async_trait]
impl ChangeSource for MysqlAdapter {
    async fn poll(&self, cursor: Option<Cursor>) -> Result<ChangeBatch> {
        let entity = self.config.mapping.entity.as_str();
        let last_id: i64 = cursor.as_deref().unwrap_or("0").parse().unwrap_or(0);

        let max_id: (i64,) = sqlx::query_as(&format!("SELECT CAST(COALESCE(MAX(id), 0) AS SIGNED) FROM _{entity}_outbox;"))
            .fetch_one(&self.pool)
            .await?;
        let effective_cursor = if last_id > max_id.0 { 0 } else { last_id };

        let query_sql = format!(
            r#"
            SELECT id, tenant_id, hour_start
            FROM _{entity}_outbox
            WHERE id > ?
            ORDER BY id ASC
            LIMIT 1000;
            "#
        );

        let rows = sqlx::query(&query_sql)
            .bind(effective_cursor)
            .fetch_all(&self.pool)
            .await?;

        let mut next_cursor = None;
        let mut ack_ids = Vec::new();
        let mut seen = HashSet::new();
        let mut bucket_keys = Vec::new();

        for r in rows {
            let id: i64 = r.get("id");
            let tenant_id: String = r.get("tenant_id");
            let naive_hour: NaiveDateTime = r.get("hour_start");
            let hour: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive_hour, Utc);

            ack_ids.push(id.to_string());
            next_cursor = Some(id.to_string());

            let key = BucketKey::new(&self.config.id, tenant_id, hour);
            if seen.insert(key.clone()) {
                bucket_keys.push(key);
            }
        }

        Ok(ChangeBatch {
            bucket_keys,
            next_cursor,
            ack_handle: AckHandle { ids: ack_ids },
        })
    }

    async fn ack(&self, batch: &ChangeBatch) -> Result<()> {
        if batch.ack_handle.ids.is_empty() {
            return Ok(());
        }

        let entity = self.config.mapping.entity.as_str();
        let ids: Vec<i64> = batch
            .ack_handle
            .ids
            .iter()
            .filter_map(|s| s.parse::<i64>().ok())
            .collect();

        if ids.is_empty() {
            return Ok(());
        }

        let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
        let delete_sql = format!(
            "DELETE FROM _{entity}_outbox WHERE id IN ({});",
            placeholders.join(",")
        );

        let mut query = sqlx::query(&delete_sql);
        for id in ids {
            query = query.bind(id);
        }
        query.execute(&self.pool).await?;

        Ok(())
    }
}

pub struct MysqlSnapshot {
    config: SourceConfig,
    pool: MySqlPool,
}

#[async_trait]
impl Snapshot for MysqlSnapshot {
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate> {
        let entity = self.config.mapping.entity.as_str();
        let tenant_field = self.config.mapping.tenant_id_field.as_str();
        let status_field = self.config.mapping.status_field.as_str();
        let amount_field = self.config.mapping.amount_field.as_str();
        let product_field = self.config.mapping.product_id_field.as_str();
        let category_field = self.config.mapping.category_field.as_str();
        let created_field = self.config.mapping.created_at_field.as_str();

        let start_hour = key.hour_start_utc.naive_utc();
        let end_hour = (key.hour_start_utc + Duration::hours(1)).naive_utc();

        let mut tx = self.pool.begin().await?;

        // Status Breakdown
        let status_sql = format!(
            r#"
            SELECT {status_field} as status,
                   COUNT(*) as order_count,
                   CAST(COALESCE(SUM({amount_field}), 0) AS SIGNED) as total_amount
            FROM {entity}
            WHERE {tenant_field} = ? AND {created_field} >= ? AND {created_field} < ?
            GROUP BY {status_field};
            "#
        );
        let status_rows = sqlx::query(&status_sql)
            .bind(&key.tenant_id)
            .bind(start_hour)
            .bind(end_hour)
            .fetch_all(&mut *tx)
            .await?;

        let status_aggregates = status_rows
            .into_iter()
            .map(|r| StatusAggregate {
                status: r.get("status"),
                order_count: r.get("order_count"),
                sales_units: r.get("total_amount"),
            })
            .collect();

        // Product Breakdown
        let product_sql = format!(
            r#"
            SELECT {product_field} as product_id,
                   {category_field} as category,
                   COUNT(*) as units,
                   CAST(COALESCE(SUM({amount_field}), 0) AS SIGNED) as sales_units
            FROM {entity}
            WHERE {tenant_field} = ? AND {created_field} >= ? AND {created_field} < ?
            GROUP BY {product_field}, {category_field};
            "#
        );
        let product_rows = sqlx::query(&product_sql)
            .bind(&key.tenant_id)
            .bind(start_hour)
            .bind(end_hour)
            .fetch_all(&mut *tx)
            .await?;

        let product_aggregates = product_rows
            .into_iter()
            .map(|r| ProductAggregate {
                product_id: r.get("product_id"),
                category: r.get("category"),
                units: r.get("units"),
                sales_units: r.get("sales_units"),
            })
            .collect();

        tx.commit().await?;

        Ok(BucketAggregate {
            key: key.clone(),
            status_aggregates,
            product_aggregates,
        })
    }
}

#[async_trait]
impl BucketReader for MysqlAdapter {
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate> {
        let snapshot = self.begin_snapshot().await?;
        snapshot.compute(key).await
    }

    async fn begin_snapshot(&self) -> Result<Box<dyn Snapshot>> {
        Ok(Box::new(MysqlSnapshot {
            config: self.config.clone(),
            pool: self.pool.clone(),
        }))
    }
}
