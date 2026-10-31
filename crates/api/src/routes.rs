use crate::auth::AuthenticatedTenant;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct DateRangeQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct TrendQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub grain: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TopProductsQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub n: Option<i64>,
}

fn internal_error<E: std::fmt::Display>(e: E) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": format!("Database error: {}", e) })),
    )
}

fn resolve_window(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> (DateTime<Utc>, DateTime<Utc>) {
    let now = Utc::now();
    let to_dt = to.unwrap_or_else(|| now + Duration::hours(1));
    let from_dt = from.unwrap_or_else(|| now - Duration::days(30));

    // Align to IST hourly boundary (+1800s offset from UTC hour)
    let from_aligned = Utc.timestamp_opt(((from_dt.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();
    let to_aligned = Utc.timestamp_opt(((to_dt.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();

    (from_aligned, to_aligned)
}

/// 1. Counts and Totals
pub async fn get_order_count(
    State(pool): State<PgPool>,
    Path(tenant_id): Path<Uuid>,
    Query(params): Query<DateRangeQuery>,
    Extension(_auth): Extension<AuthenticatedTenant>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let (from_aligned, to_aligned) = resolve_window(params.from, params.to);

    let mut tx = pool.begin().await.map_err(internal_error)?;

    // Set transaction-scoped RLS context
    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

    let watermark: DateTime<Utc> = sqlx::query_scalar(
        "SELECT COALESCE(MAX(computed_at), NOW()) FROM bucket_state WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal_error)?;

    let (order_count, total_sales_paise): (i64, i64) = sqlx::query_as(
        r#"
        SELECT 
            COALESCE(SUM(order_count), 0)::BIGINT,
            COALESCE(SUM(sales_paise), 0)::BIGINT
        FROM agg_hourly
        WHERE tenant_id = $1 AND hour >= $2 AND hour < $3 AND status = 'completed'
        "#,
    )
    .bind(tenant_id)
    .bind(from_aligned)
    .bind(to_aligned)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal_error)?;

    tx.commit().await.map_err(internal_error)?;

    Ok(Json(json!({
        "tenant_id": tenant_id,
        "from": from_aligned.to_rfc3339(),
        "to": to_aligned.to_rfc3339(),
        "order_count": order_count,
        "total_sales_paise": total_sales_paise,
        "data_as_of": watermark.to_rfc3339(),
    }))
    .into_response())
}

/// 2. Trends (hourly or daily)
pub async fn get_order_trend(
    State(pool): State<PgPool>,
    Path(tenant_id): Path<Uuid>,
    Query(params): Query<TrendQuery>,
    Extension(_auth): Extension<AuthenticatedTenant>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let (from_aligned, to_aligned) = resolve_window(params.from, params.to);
    let grain = params.grain.unwrap_or_else(|| "day".to_string()).to_lowercase();

    let mut tx = pool.begin().await.map_err(internal_error)?;

    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

    let watermark: DateTime<Utc> = sqlx::query_scalar(
        "SELECT COALESCE(MAX(computed_at), NOW()) FROM bucket_state WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal_error)?;

    let points: Vec<serde_json::Value> = match grain.as_str() {
        "hour" => {
            let rows: Vec<(DateTime<Utc>, i64, i64)> = sqlx::query_as(
                r#"
                SELECT 
                    hour,
                    COALESCE(SUM(order_count), 0)::BIGINT,
                    COALESCE(SUM(sales_paise), 0)::BIGINT
                FROM agg_hourly
                WHERE tenant_id = $1 AND hour >= $2 AND hour < $3 AND status = 'completed'
                GROUP BY hour
                ORDER BY hour ASC
                "#,
            )
            .bind(tenant_id)
            .bind(from_aligned)
            .bind(to_aligned)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal_error)?;

            rows.into_iter()
                .map(|(hour, cnt, sum)| {
                    json!({
                        "bucket": hour.to_rfc3339(),
                        "order_count": cnt,
                        "total_sales_paise": sum,
                    })
                })
                .collect()
        }
        _ => {
            // Day grain
            let rows: Vec<(String, i64, i64)> = sqlx::query_as(
                r#"
                SELECT 
                    to_char(date_trunc('day', hour AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD') AS day_bucket,
                    COALESCE(SUM(order_count), 0)::BIGINT,
                    COALESCE(SUM(sales_paise), 0)::BIGINT
                FROM agg_hourly
                WHERE tenant_id = $1 AND hour >= $2 AND hour < $3 AND status = 'completed'
                GROUP BY 1
                ORDER BY 1 ASC
                "#,
            )
            .bind(tenant_id)
            .bind(from_aligned)
            .bind(to_aligned)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal_error)?;

            rows.into_iter()
                .map(|(day, cnt, sum)| {
                    json!({
                        "bucket": day,
                        "order_count": cnt,
                        "total_sales_paise": sum,
                    })
                })
                .collect()
        }
    };

    tx.commit().await.map_err(internal_error)?;

    Ok(Json(json!({
        "tenant_id": tenant_id,
        "from": from_aligned.to_rfc3339(),
        "to": to_aligned.to_rfc3339(),
        "grain": grain,
        "points": points,
        "data_as_of": watermark.to_rfc3339(),
    }))
    .into_response())
}

/// 3. Top-N Products
pub async fn get_top_products(
    State(pool): State<PgPool>,
    Path(tenant_id): Path<Uuid>,
    Query(params): Query<TopProductsQuery>,
    Extension(_auth): Extension<AuthenticatedTenant>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let (from_aligned, to_aligned) = resolve_window(params.from, params.to);
    let n = params.n.unwrap_or(10).clamp(1, 100);

    let mut tx = pool.begin().await.map_err(internal_error)?;

    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

    let watermark: DateTime<Utc> = sqlx::query_scalar(
        "SELECT COALESCE(MAX(computed_at), NOW()) FROM bucket_state WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal_error)?;

    let rows: Vec<(Uuid, String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT 
            product_id,
            category,
            COALESCE(SUM(units), 0)::BIGINT,
            COALESCE(SUM(sales_paise), 0)::BIGINT
        FROM agg_hourly_product
        WHERE tenant_id = $1 AND hour >= $2 AND hour < $3
        GROUP BY product_id, category
        ORDER BY 4 DESC, 1 ASC
        LIMIT $4
        "#,
    )
    .bind(tenant_id)
    .bind(from_aligned)
    .bind(to_aligned)
    .bind(n)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal_error)?;

    tx.commit().await.map_err(internal_error)?;

    let products: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(prod_id, cat, units, sales)| {
            json!({
                "product_id": prod_id,
                "category": cat,
                "units": units,
                "total_sales_paise": sales,
            })
        })
        .collect();

    Ok(Json(json!({
        "tenant_id": tenant_id,
        "from": from_aligned.to_rfc3339(),
        "to": to_aligned.to_rfc3339(),
        "top_products": products,
        "data_as_of": watermark.to_rfc3339(),
    }))
    .into_response())
}

/// 4. Category & Status Breakdown
pub async fn get_order_breakdown(
    State(pool): State<PgPool>,
    Path(tenant_id): Path<Uuid>,
    Query(params): Query<DateRangeQuery>,
    Extension(_auth): Extension<AuthenticatedTenant>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let (from_aligned, to_aligned) = resolve_window(params.from, params.to);

    let mut tx = pool.begin().await.map_err(internal_error)?;

    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;

    let watermark: DateTime<Utc> = sqlx::query_scalar(
        "SELECT COALESCE(MAX(computed_at), NOW()) FROM bucket_state WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal_error)?;

    let status_rows: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT 
            status,
            COALESCE(SUM(order_count), 0)::BIGINT,
            COALESCE(SUM(sales_paise), 0)::BIGINT
        FROM agg_hourly
        WHERE tenant_id = $1 AND hour >= $2 AND hour < $3
        GROUP BY status
        ORDER BY status ASC
        "#,
    )
    .bind(tenant_id)
    .bind(from_aligned)
    .bind(to_aligned)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal_error)?;

    let category_rows: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT 
            category,
            COALESCE(SUM(units), 0)::BIGINT,
            COALESCE(SUM(sales_paise), 0)::BIGINT
        FROM agg_hourly_product
        WHERE tenant_id = $1 AND hour >= $2 AND hour < $3
        GROUP BY category
        ORDER BY 3 DESC, 1 ASC
        "#,
    )
    .bind(tenant_id)
    .bind(from_aligned)
    .bind(to_aligned)
    .fetch_all(&mut *tx)
    .await
    .map_err(internal_error)?;

    tx.commit().await.map_err(internal_error)?;

    let by_status: Vec<serde_json::Value> = status_rows
        .into_iter()
        .map(|(status, cnt, sum)| {
            json!({
                "status": status,
                "order_count": cnt,
                "total_sales_paise": sum,
            })
        })
        .collect();

    let by_category: Vec<serde_json::Value> = category_rows
        .into_iter()
        .map(|(cat, units, sum)| {
            json!({
                "category": cat,
                "units": units,
                "total_sales_paise": sum,
            })
        })
        .collect();

    Ok(Json(json!({
        "tenant_id": tenant_id,
        "from": from_aligned.to_rfc3339(),
        "to": to_aligned.to_rfc3339(),
        "by_status": by_status,
        "by_category": by_category,
        "data_as_of": watermark.to_rfc3339(),
    }))
    .into_response())
}
