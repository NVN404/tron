use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use platform_core::config::SourceConfig;
use platform_core::traits::{BucketReader, ChangeSource, Snapshot};
use platform_core::types::{
    AckHandle, BucketAggregate, BucketKey, ChangeBatch, Cursor, ProductAggregate, StatusAggregate,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use std::collections::HashSet;

pub struct PostgresAdapter {
    config: SourceConfig,
    pool: PgPool,
}

impl PostgresAdapter {
    pub async fn new(config: SourceConfig) -> Result<Self> {
        let dsn = config.resolve_dsn()?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&dsn)
            .await
            .with_context(|| format!("Failed to connect to Postgres source {}", config.id))?;

        Ok(Self { config, pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Sets up tables, outbox, and triggers on the Postgres source database.
    pub async fn setup_schema(&self) -> Result<()> {
        let entity = self.config.mapping.entity.as_str();
        let tenant_field = self.config.mapping.tenant_id_field.as_str();
        let id_field = self.config.mapping.id_field.as_str();
        let status_field = self.config.mapping.status_field.as_str();
        let amount_field = self.config.mapping.amount_field.as_str();
        let product_field = self.config.mapping.product_id_field.as_str();
        let category_field = self.config.mapping.category_field.as_str();
        let created_field = self.config.mapping.created_at_field.as_str();

        let mut tx = self.pool.begin().await?;

        // 1. Source entity table (if not exists)
        let create_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {entity} (
                {id_field} UUID PRIMARY KEY,
                {tenant_field} TEXT NOT NULL,
                {product_field} TEXT NOT NULL,
                {category_field} TEXT NOT NULL,
                {amount_field} BIGINT NOT NULL,
                {status_field} TEXT NOT NULL,
                {created_field} TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#
        );
        sqlx::query(&create_table_sql).execute(&mut *tx).await?;

        let create_idx_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{entity}_tenant_created ON {entity} ({tenant_field}, {created_field});"
        );
        sqlx::query(&create_idx_sql).execute(&mut *tx).await?;

        // 2. Outbox table
        let outbox_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS _{entity}_outbox (
                id BIGSERIAL PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                hour_start TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#
        );
        sqlx::query(&outbox_table_sql).execute(&mut *tx).await?;

        let outbox_idx_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{entity}_outbox_id ON _{entity}_outbox (id ASC);"
        );
        sqlx::query(&outbox_idx_sql).execute(&mut *tx).await?;

        // 3. Trigger function
        let trigger_fn_sql = format!(
            r#"
            CREATE OR REPLACE FUNCTION trg_{entity}_outbox_notify()
            RETURNS TRIGGER AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                    VALUES (NEW.{tenant_field}::text, date_trunc('hour', NEW.{created_field}));
                    RETURN NEW;
                ELSIF TG_OP = 'UPDATE' THEN
                    INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                    VALUES (OLD.{tenant_field}::text, date_trunc('hour', OLD.{created_field}));
                    IF OLD.{tenant_field} != NEW.{tenant_field} OR date_trunc('hour', OLD.{created_field}) != date_trunc('hour', NEW.{created_field}) THEN
                        INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                        VALUES (NEW.{tenant_field}::text, date_trunc('hour', NEW.{created_field}));
                    END IF;
                    RETURN NEW;
                ELSIF TG_OP = 'DELETE' THEN
                    INSERT INTO _{entity}_outbox (tenant_id, hour_start)
                    VALUES (OLD.{tenant_field}::text, date_trunc('hour', OLD.{created_field}));
                    RETURN OLD;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            "#
        );
        sqlx::query(&trigger_fn_sql).execute(&mut *tx).await?;

        // 4. Trigger
        let drop_trg_sql = format!("DROP TRIGGER IF EXISTS trg_{entity}_outbox_change ON {entity};");
        sqlx::query(&drop_trg_sql).execute(&mut *tx).await?;

        let create_trg_sql = format!(
            r#"
            CREATE TRIGGER trg_{entity}_outbox_change
            AFTER INSERT OR UPDATE OR DELETE ON {entity}
            FOR EACH ROW EXECUTE FUNCTION trg_{entity}_outbox_notify();
            "#
        );
        sqlx::query(&create_trg_sql).execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl ChangeSource for PostgresAdapter {
    async fn poll(&self, cursor: Option<Cursor>) -> Result<ChangeBatch> {
        let entity = self.config.mapping.entity.as_str();
        let last_id: i64 = cursor.as_deref().unwrap_or("0").parse().unwrap_or(0);

        let max_id: (i64,) = sqlx::query_as(&format!("SELECT COALESCE(MAX(id), 0)::bigint FROM _{entity}_outbox;"))
            .fetch_one(&self.pool)
            .await?;
        let effective_cursor = if last_id > max_id.0 { 0 } else { last_id };

        let query_sql = format!(
            r#"
            SELECT id, tenant_id, hour_start
            FROM _{entity}_outbox
            WHERE id > $1
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
            let hour: DateTime<Utc> = r.get("hour_start");

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

        let delete_sql = format!(
            r#"
            DELETE FROM _{entity}_outbox
            WHERE id = ANY($1);
            "#
        );

        sqlx::query(&delete_sql)
            .bind(&ids)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

pub struct PostgresSnapshot {
    config: SourceConfig,
    pool: PgPool,
}

#[async_trait]
impl Snapshot for PostgresSnapshot {
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate> {
        let entity = self.config.mapping.entity.as_str();
        let tenant_field = self.config.mapping.tenant_id_field.as_str();
        let status_field = self.config.mapping.status_field.as_str();
        let amount_field = self.config.mapping.amount_field.as_str();
        let product_field = self.config.mapping.product_id_field.as_str();
        let category_field = self.config.mapping.category_field.as_str();
        let created_field = self.config.mapping.created_at_field.as_str();

        let start_hour = key.hour_start_utc;
        let end_hour = start_hour + Duration::hours(1);

        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;")
            .execute(&mut *tx)
            .await?;

        // Status Breakdown
        let status_sql = format!(
            r#"
            SELECT {status_field} as status,
                   COUNT(*)::bigint as order_count,
                   COALESCE(SUM({amount_field}), 0)::bigint as total_amount
            FROM {entity}
            WHERE {tenant_field} = $1 AND {created_field} >= $2 AND {created_field} < $3
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
            SELECT {product_field}::text as product_id,
                   {category_field} as category,
                   COUNT(*)::bigint as units,
                   COALESCE(SUM({amount_field}), 0)::bigint as sales_units
            FROM {entity}
            WHERE {tenant_field} = $1 AND {created_field} >= $2 AND {created_field} < $3
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
impl BucketReader for PostgresAdapter {
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate> {
        let snapshot = self.begin_snapshot().await?;
        snapshot.compute(key).await
    }

    async fn begin_snapshot(&self) -> Result<Box<dyn Snapshot>> {
        Ok(Box::new(PostgresSnapshot {
            config: self.config.clone(),
            pool: self.pool.clone(),
        }))
    }
}
