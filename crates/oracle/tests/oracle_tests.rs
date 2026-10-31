use chrono::DateTime;
use common::AppConfig;
use oracle::Oracle;
use sqlx::PgPool;
use uuid::Uuid;

#[tokio::test]
async fn test_oracle_handbuilt_dataset_edge_cases() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let pool = PgPool::connect(&config.source_database_url).await?;

    // Create unique test tenants
    let tenant_a = Uuid::new_v4();
    let tenant_zero = Uuid::new_v4();

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Oracle-Test-Tenant-A'), ($2, 'Oracle-Test-Zero')")
        .bind(tenant_a)
        .bind(tenant_zero)
        .execute(&pool)
        .await?;

    // Products
    let prod_electronics = Uuid::new_v4();
    let prod_apparel = Uuid::new_v4();

    sqlx::query("INSERT INTO products (tenant_id, product_id, category) VALUES ($1, $2, 'Electronics'), ($1, $3, 'Apparel')")
        .bind(tenant_a)
        .bind(prod_electronics)
        .bind(prod_apparel)
        .execute(&pool)
        .await?;

    // Customer
    let customer_id = Uuid::new_v4();
    sqlx::query("INSERT INTO customers (tenant_id, customer_id, name) VALUES ($1, $2, 'Test Customer')")
        .bind(tenant_a)
        .bind(customer_id)
        .execute(&pool)
        .await?;

    // Hand-crafted edge case orders:
    // Order 1: 2026-03-31T18:29:59Z (23:59:59 IST - 1 sec before IST midnight). Completed. Amount = 10,000.
    let t_o1 = DateTime::parse_from_rfc3339("2026-03-31T18:29:59Z")?.to_utc();
    // Order 2: 2026-03-31T18:30:00Z (00:00:00 IST - exact IST midnight, start of 2026-04-01). Completed. Amount = 20,000.
    let t_o2 = DateTime::parse_from_rfc3339("2026-03-31T18:30:00Z")?.to_utc();
    // Order 3: 2026-04-01T06:30:00Z (12:00:00 IST). Completed. Amount = 15,000.
    let t_o3 = DateTime::parse_from_rfc3339("2026-04-01T06:30:00Z")?.to_utc();
    // Order 4: 2026-04-01T07:30:00Z (13:00:00 IST). CANCELLED. Amount = 50,000.
    let t_o4 = DateTime::parse_from_rfc3339("2026-04-01T07:30:00Z")?.to_utc();
    // Order 5: 2026-04-01T08:30:00Z (14:00:00 IST). Completed, but SOFT DELETED. Amount = 30,000.
    let t_o5 = DateTime::parse_from_rfc3339("2026-04-01T08:30:00Z")?.to_utc();
    // Order 6: 2026-04-01T12:00:00Z (17:30:00 IST). Exact upper boundary 'to'. Amount = 77,000.
    let t_o6 = DateTime::parse_from_rfc3339("2026-04-01T12:00:00Z")?.to_utc();

    // Insert orders
    sqlx::query("INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) VALUES ($1, $2, 'Electronics', 'completed', 10000, $3)")
        .bind(tenant_a).bind(prod_electronics).bind(t_o1).execute(&pool).await?;

    sqlx::query("INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) VALUES ($1, $2, 'Electronics', 'completed', 20000, $3)")
        .bind(tenant_a).bind(prod_electronics).bind(t_o2).execute(&pool).await?;

    sqlx::query("INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) VALUES ($1, $2, 'Apparel', 'completed', 15000, $3)")
        .bind(tenant_a).bind(prod_apparel).bind(t_o3).execute(&pool).await?;

    sqlx::query("INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) VALUES ($1, $2, 'Electronics', 'cancelled', 50000, $3)")
        .bind(tenant_a).bind(prod_electronics).bind(t_o4).execute(&pool).await?;

    sqlx::query("INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at, deleted_at) VALUES ($1, $2, 'Apparel', 'completed', 30000, $3, NOW())")
        .bind(tenant_a).bind(prod_apparel).bind(t_o5).execute(&pool).await?;

    sqlx::query("INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) VALUES ($1, $2, 'Electronics', 'completed', 77000, $3)")
        .bind(tenant_a).bind(prod_electronics).bind(t_o6).execute(&pool).await?;

    // --- TEST 1: Count and Totals with Range Edge Inclusion & Exclusion ---
    // Range: [t_o1, t_o6) -> Must include t_o1 (inclusive), exclude t_o6 (exclusive).
    // Must exclude cancelled order (t_o4) and soft-deleted order (t_o5).
    // Included: Order 1 (10,000), Order 2 (20,000), Order 3 (15,000). Total = 3 orders, 45,000 paise.
    let count_res = Oracle::count_and_total(&pool, tenant_a, t_o1, t_o6).await?;
    assert_eq!(count_res.order_count, 3, "Should include exactly 3 completed non-deleted orders in [from, to)");
    assert_eq!(count_res.total_sales_paise, 45000, "Should sum to 45000 paise");

    // --- TEST 2: Daily Trend & IST Midnight Boundary ---
    // In IST, Order 1 is 2026-03-31 23:59:59 (Day: 2026-03-31)
    // Order 2 is 2026-04-01 00:00:00 (Day: 2026-04-01)
    // Order 3 is 2026-04-01 12:00:00 (Day: 2026-04-01)
    let trend_day = Oracle::trend(&pool, tenant_a, t_o1, t_o6, "day").await?;
    assert_eq!(trend_day.len(), 2, "Should span 2 distinct IST calendar days");
    assert_eq!(trend_day[0].order_count, 1, "IST Day 2026-03-31 should have 1 order (Order 1)");
    assert_eq!(trend_day[0].total_sales_paise, 10000);
    assert_eq!(trend_day[1].order_count, 2, "IST Day 2026-04-01 should have 2 orders (Order 2 + Order 3)");
    assert_eq!(trend_day[1].total_sales_paise, 35000);

    // --- TEST 3: Hourly Trend & Empty Bucket Zero-Padding ---
    // 4 hours span: from 04:30 UTC to 08:30 UTC (10:00 to 14:00 IST)
    // In this window, only Order 3 (06:30 UTC / 12:00 IST) is completed.
    let range_start = DateTime::parse_from_rfc3339("2026-04-01T04:30:00Z")?.to_utc();
    let range_end = DateTime::parse_from_rfc3339("2026-04-01T08:30:00Z")?.to_utc();
    let trend_hour = Oracle::trend(&pool, tenant_a, range_start, range_end, "hour").await?;
    assert_eq!(trend_hour.len(), 4, "Must generate exactly 4 hourly buckets");
    let active_buckets: Vec<&oracle::TrendResult> = trend_hour.iter().filter(|b| b.order_count > 0).collect();
    assert_eq!(active_buckets.len(), 1, "Only 1 hour bucket should have orders");
    assert_eq!(active_buckets[0].order_count, 1);
    assert_eq!(active_buckets[0].total_sales_paise, 15000);

    let empty_buckets: Vec<&oracle::TrendResult> = trend_hour.iter().filter(|b| b.order_count == 0).collect();
    assert_eq!(empty_buckets.len(), 3, "3 hour buckets must be padded with count = 0 and sales = 0");
    for eb in empty_buckets {
        assert_eq!(eb.total_sales_paise, 0);
    }

    // --- TEST 4: Top Products & Deterministic Tie-Break ---
    let top_prods = Oracle::top_products(&pool, tenant_a, t_o1, t_o6, 10).await?;
    assert_eq!(top_prods.len(), 2);
    // Electronics has Order 1 (10k) + Order 2 (20k) = 30k total sales, 2 units
    assert_eq!(top_prods[0].product_id, prod_electronics);
    assert_eq!(top_prods[0].units, 2);
    assert_eq!(top_prods[0].total_sales_paise, 30000);
    // Apparel has Order 3 (15k) = 15k total sales, 1 unit
    assert_eq!(top_prods[1].product_id, prod_apparel);
    assert_eq!(top_prods[1].units, 1);
    assert_eq!(top_prods[1].total_sales_paise, 15000);

    // --- TEST 5: Breakdown by Category and Status ---
    let breakdown = Oracle::breakdown(&pool, tenant_a, t_o1, t_o6).await?;
    // Should contain:
    // Apparel completed: 1 order, 15000
    // Electronics cancelled: 1 order, 50000
    // Electronics completed: 2 orders, 30000
    // Soft-deleted order (deleted_at IS NOT NULL) must NOT appear
    assert_eq!(breakdown.len(), 3);
    assert_eq!(breakdown[0].category, "Apparel");
    assert_eq!(breakdown[0].status, "completed");
    assert_eq!(breakdown[0].order_count, 1);
    assert_eq!(breakdown[0].total_sales_paise, 15000);

    assert_eq!(breakdown[1].category, "Electronics");
    assert_eq!(breakdown[1].status, "cancelled");
    assert_eq!(breakdown[1].order_count, 1);
    assert_eq!(breakdown[1].total_sales_paise, 50000);

    assert_eq!(breakdown[2].category, "Electronics");
    assert_eq!(breakdown[2].status, "completed");
    assert_eq!(breakdown[2].order_count, 2);
    assert_eq!(breakdown[2].total_sales_paise, 30000);

    // --- TEST 6: Tenant with Zero Orders ---
    let zero_count = Oracle::count_and_total(&pool, tenant_zero, t_o1, t_o6).await?;
    assert_eq!(zero_count.order_count, 0);
    assert_eq!(zero_count.total_sales_paise, 0);

    let zero_top = Oracle::top_products(&pool, tenant_zero, t_o1, t_o6, 10).await?;
    assert!(zero_top.is_empty());

    let zero_breakdown = Oracle::breakdown(&pool, tenant_zero, t_o1, t_o6).await?;
    assert!(zero_breakdown.is_empty());

    let zero_trend = Oracle::trend(&pool, tenant_zero, range_start, range_end, "hour").await?;
    assert_eq!(zero_trend.len(), 4, "Zero-order tenant must still receive padded hourly buckets");
    for b in zero_trend {
        assert_eq!(b.order_count, 0);
        assert_eq!(b.total_sales_paise, 0);
    }

    // --- TEST 7: Structured Diff Helper ---
    let json_expected = serde_json::json!({ "order_count": 3, "total_sales_paise": 45000 });
    let json_actual_match = serde_json::json!({ "order_count": 3, "total_sales_paise": 45000 });
    let diff_none = Oracle::compare(tenant_a, "/orders/count", "from", "to", &json_expected, &json_actual_match);
    assert!(diff_none.is_none());

    let json_actual_mismatch = serde_json::json!({ "order_count": 2, "total_sales_paise": 45000 });
    let diff_some = Oracle::compare(tenant_a, "/orders/count", "from", "to", &json_expected, &json_actual_mismatch);
    assert!(diff_some.is_some());
    let diff = diff_some.unwrap();
    assert_eq!(diff.tenant_id, tenant_a);
    assert!(!diff.mismatches.is_empty());

    println!("✅ All Oracle unit tests on edge-case dataset passed successfully!");
    Ok(())
}
