//! Direct-SQL oracle engine: calculates ground-truth analytics directly against the source database.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct CountAndTotalResult {
    pub tenant_id: Uuid,
    pub order_count: i64,
    pub total_sales_paise: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrendResult {
    pub bucket: String,
    pub order_count: i64,
    pub total_sales_paise: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct TopProductResult {
    pub product_id: String,
    pub category: String,
    pub units: i64,
    pub total_sales_paise: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct BreakdownResult {
    pub category: String,
    pub status: String,
    pub order_count: i64,
    pub total_sales_paise: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredDiff {
    pub tenant_id: Uuid,
    pub endpoint: String,
    pub range_from: String,
    pub range_to: String,
    pub expected: serde_json::Value,
    pub actual: serde_json::Value,
    pub mismatches: Vec<String>,
}

pub struct Oracle;

impl Oracle {
    /// 1. Counts and Totals: Returns order_count and sum of amount_paise for completed orders.
    pub async fn count_and_total(
        pool: &PgPool,
        tenant_id: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<CountAndTotalResult> {
        let row: (i64, i64) = sqlx::query_as(
            r#"
            SELECT 
                COALESCE(COUNT(*), 0)::BIGINT AS order_count,
                COALESCE(SUM(amount_paise), 0)::BIGINT AS total_sales_paise
            FROM orders
            WHERE tenant_id = $1::text
              AND created_at >= $2
              AND created_at < $3
              AND status = 'completed'
            "#,
        )
        .bind(tenant_id)
        .bind(from)
        .bind(to)
        .fetch_one(pool)
        .await?;

        Ok(CountAndTotalResult {
            tenant_id,
            order_count: row.0,
            total_sales_paise: row.1,
        })
    }

    /// 2. Trends: Returns hourly or daily rollups, padding missing buckets with 0.
    pub async fn trend(
        pool: &PgPool,
        tenant_id: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        grain: &str,
    ) -> Result<Vec<TrendResult>> {
        let grain_lower = grain.to_lowercase();
        let valid_grain = match grain_lower.as_str() {
            "hour" => "hour",
            "day" => "day",
            _ => return Err(anyhow!("Invalid grain: must be 'hour' or 'day'")),
        };

        let query = format!(
            r#"
            WITH time_series AS (
                SELECT generate_series(
                    date_trunc('{grain}', $2 AT TIME ZONE 'Asia/Kolkata'),
                    date_trunc('{grain}', ($3 - interval '1 microsecond') AT TIME ZONE 'Asia/Kolkata'),
                    ('1 ' || '{grain}')::interval
                ) AS bucket_start
            ),
            actual_aggregates AS (
                SELECT 
                    date_trunc('{grain}', created_at AT TIME ZONE 'Asia/Kolkata') AS bucket_start,
                    COUNT(*)::BIGINT AS order_count,
                    SUM(amount_paise)::BIGINT AS total_sales_paise
                FROM orders
                WHERE tenant_id = $1::text
                  AND created_at >= $2
                  AND created_at < $3
                  AND status = 'completed'
                GROUP BY 1
            )
            SELECT 
                to_char(ts.bucket_start, 'YYYY-MM-DD"T"HH24:MI:SS') AS bucket_str,
                COALESCE(aa.order_count, 0)::BIGINT AS order_count,
                COALESCE(aa.total_sales_paise, 0)::BIGINT AS total_sales_paise
            FROM time_series ts
            LEFT JOIN actual_aggregates aa ON ts.bucket_start = aa.bucket_start
            ORDER BY ts.bucket_start ASC
            "#,
            grain = valid_grain
        );

        let rows: Vec<(String, i64, i64)> = sqlx::query_as(&query)
            .bind(tenant_id)
            .bind(from)
            .bind(to)
            .fetch_all(pool)
            .await?;

        let results = rows
            .into_iter()
            .map(|(bucket, order_count, total_sales_paise)| TrendResult {
                bucket,
                order_count,
                total_sales_paise,
            })
            .collect();

        Ok(results)
    }

    /// 3. Top Products: Returns top-N products by revenue with deterministic tie-breaking (product_id ASC).
    pub async fn top_products(
        pool: &PgPool,
        tenant_id: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        n: i64,
    ) -> Result<Vec<TopProductResult>> {
        let rows = sqlx::query_as::<_, TopProductResult>(
            r#"
            SELECT 
                product_id,
                category,
                COUNT(*)::BIGINT AS units,
                SUM(amount_paise)::BIGINT AS total_sales_paise
            FROM orders
            WHERE tenant_id = $1::text
              AND created_at >= $2
              AND created_at < $3
              AND status = 'completed'
            GROUP BY product_id, category
            ORDER BY total_sales_paise DESC, product_id ASC
            LIMIT $4
            "#,
        )
        .bind(tenant_id)
        .bind(from)
        .bind(to)
        .bind(n)
        .fetch_all(pool)
        .await?;

        Ok(rows)
    }

    /// 4. Category & Status Breakdown: Returns distribution across status and category.
    pub async fn breakdown(
        pool: &PgPool,
        tenant_id: Uuid,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<BreakdownResult>> {
        let rows = sqlx::query_as::<_, BreakdownResult>(
            r#"
            SELECT 
                category,
                status,
                COUNT(*)::BIGINT AS order_count,
                COALESCE(SUM(amount_paise), 0)::BIGINT AS total_sales_paise
            FROM orders
            WHERE tenant_id = $1::text
              AND created_at >= $2
              AND created_at < $3
            GROUP BY category, status
            ORDER BY category ASC, status ASC
            "#,
        )
        .bind(tenant_id)
        .bind(from)
        .bind(to)
        .fetch_all(pool)
        .await?;

        Ok(rows)
    }
}
