use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use common::AppConfig;
use oracle::Oracle;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
async fn test_oracle_vs_analytics_accuracy() -> Result<()> {
    let config = AppConfig::from_env()?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    // Select diverse tenants (top tenant, mid tenant, small tenant)
    let tenants: Vec<Uuid> = sqlx::query_scalar(
        "SELECT tenant_id FROM orders GROUP BY tenant_id ORDER BY COUNT(*) DESC LIMIT 5"
    )
    .fetch_all(&source_pool)
    .await?;

    let now = Utc::now();
    let test_windows = vec![
        (now - Duration::days(7), now),
        (now - Duration::days(30), now),
        (now - Duration::days(90), now - Duration::days(30)),
    ];

    for tenant_id in tenants {
        for (from, to) in &test_windows {
            // Align to 30-minute offset (IST hourly boundary: 00:30, 01:30, ...)
            let from_aligned = Utc.timestamp_opt(((from.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();
            let to_aligned = Utc.timestamp_opt(((to.timestamp() - 1800) / 3600) * 3600 + 1800, 0).unwrap();

            // 1. Count and Total comparison
            let oracle_cnt = Oracle::count_and_total(&source_pool, tenant_id, from_aligned, to_aligned).await?;

            let ana_cnt: (i64, i64) = sqlx::query_as(
                r#"
                SELECT 
                    COALESCE(SUM(order_count), 0)::BIGINT,
                    COALESCE(SUM(sales_paise), 0)::BIGINT
                FROM agg_hourly
                WHERE tenant_id = $1 AND hour >= $2 AND hour < $3 AND status = 'completed'
                "#
            )
            .bind(tenant_id)
            .bind(from_aligned)
            .bind(to_aligned)
            .fetch_one(&analytics_pool)
            .await?;

            assert_eq!(
                oracle_cnt.order_count, ana_cnt.0,
                "Count mismatch for tenant {} in [{}, {}]: oracle={}, ana={}",
                tenant_id, from_aligned, to_aligned, oracle_cnt.order_count, ana_cnt.0
            );
            assert_eq!(
                oracle_cnt.total_sales_paise, ana_cnt.1,
                "Sales paise mismatch for tenant {} in [{}, {}]: oracle={}, ana={}",
                tenant_id, from_aligned, to_aligned, oracle_cnt.total_sales_paise, ana_cnt.1
            );

            // 2. Top-N Products comparison (top 5)
            let oracle_top = Oracle::top_products(&source_pool, tenant_id, from_aligned, to_aligned, 5).await?;

            let ana_top: Vec<(Uuid, String, i64, i64)> = sqlx::query_as(
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
                LIMIT 5
                "#
            )
            .bind(tenant_id)
            .bind(from_aligned)
            .bind(to_aligned)
            .fetch_all(&analytics_pool)
            .await?;

            assert_eq!(
                oracle_top.len(), ana_top.len(),
                "Top product length mismatch for tenant {}", tenant_id
            );

            for (o_prod, a_prod) in oracle_top.iter().zip(ana_top.iter()) {
                assert_eq!(o_prod.product_id, a_prod.0, "Product ID mismatch");
                assert_eq!(o_prod.category, a_prod.1, "Category mismatch");
                assert_eq!(o_prod.units, a_prod.2, "Units mismatch");
                assert_eq!(o_prod.total_sales_paise, a_prod.3, "Sales paise mismatch");
            }

            // 3. Trend comparison (daily grain)
            let oracle_trend = Oracle::trend(&source_pool, tenant_id, from_aligned, to_aligned, "day").await?;
            let ana_trend: Vec<(String, i64, i64)> = sqlx::query_as(
                r#"
                SELECT 
                    to_char(date_trunc('day', hour AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD') AS bucket_str,
                    COALESCE(SUM(order_count), 0)::BIGINT,
                    COALESCE(SUM(sales_paise), 0)::BIGINT
                FROM agg_hourly
                WHERE tenant_id = $1 AND hour >= $2 AND hour < $3 AND status = 'completed'
                GROUP BY 1
                ORDER BY 1 ASC
                "#
            )
            .bind(tenant_id)
            .bind(from_aligned)
            .bind(to_aligned)
            .fetch_all(&analytics_pool)
            .await?;

            // Filter out 0-padded days from oracle for 1:1 match with rollup days
            let oracle_active: Vec<&oracle::TrendResult> = oracle_trend.iter().filter(|t| t.order_count > 0).collect();
            assert_eq!(oracle_active.len(), ana_trend.len(), "Trend days count mismatch");
            for (o_t, a_t) in oracle_active.iter().zip(ana_trend.iter()) {
                assert_eq!(&o_t.bucket[0..10], &a_t.0, "Bucket date mismatch");
                assert_eq!(o_t.order_count, a_t.1, "Bucket order count mismatch");
                assert_eq!(o_t.total_sales_paise, a_t.2, "Bucket sales paise mismatch");
            }
        }
    }

    Ok(())
}
