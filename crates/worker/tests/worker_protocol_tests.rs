use chrono::{DateTime, Duration};
use common::AppConfig;
use oracle::Oracle;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use uuid::Uuid;
use worker::{BucketTarget, PublishResult, WorkerEngine};

#[tokio::test]
async fn test_worker_a_correctness_vs_oracle() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    let tenant = Uuid::new_v4();
    let prod_1 = Uuid::new_v4();
    let prod_2 = Uuid::new_v4();

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Correctness-Tenant')")
        .bind(tenant).execute(&source_pool).await?;
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Correctness-Tenant')")
        .bind(tenant).execute(&analytics_pool).await?;

    sqlx::query("INSERT INTO products (tenant_id, product_id, category) VALUES ($1, $2, 'Electronics'), ($1, $3, 'Food')")
        .bind(tenant).bind(prod_1).bind(prod_2).execute(&source_pool).await?;

    let worker = WorkerEngine::new(
        source_pool.clone(),
        analytics_pool.clone(),
        "worker-test-a".to_string(),
        100,
        30,
    );

    // Drain any pre-existing outbox rows first
    while worker.process_one_batch().await? > 0 {}

    // In IST, 09:30 UTC is exactly 15:00:00 IST (an aligned IST hour)
    let t_ist_hour = DateTime::parse_from_rfc3339("2026-06-15T09:30:00Z")?.to_utc();
    let t_order_1 = t_ist_hour + Duration::minutes(15);
    let t_order_2 = t_ist_hour + Duration::minutes(30);

    let o1_id: (i64,) = sqlx::query_as(
        "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, 'Electronics', 'completed', 10000, $3) RETURNING id"
    ).bind(tenant).bind(prod_1).bind(t_order_1).fetch_one(&source_pool).await?;

    let o2_id: (i64,) = sqlx::query_as(
        "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, 'Electronics', 'completed', 20000, $3) RETURNING id"
    ).bind(tenant).bind(prod_1).bind(t_order_2).fetch_one(&source_pool).await?;

    let o3_id: (i64,) = sqlx::query_as(
        "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, 'Food', 'pending', 5000, $3) RETURNING id"
    ).bind(tenant).bind(prod_2).bind(t_order_1).fetch_one(&source_pool).await?;

    // Drain initial batch
    while worker.process_one_batch().await? > 0 {}

    // 2. Perform updates: mutate amount, change status, soft delete, backdate
    sqlx::query("UPDATE orders SET amount_paise = 25000 WHERE id = $1")
        .bind(o1_id.0).execute(&source_pool).await?;
    sqlx::query("UPDATE orders SET status = 'completed' WHERE id = $1")
        .bind(o3_id.0).execute(&source_pool).await?;
    sqlx::query("UPDATE orders SET deleted_at = NOW() WHERE id = $1")
        .bind(o2_id.0).execute(&source_pool).await?;

    // Backdated order into the same hour
    sqlx::query(
        "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, 'Food', 'completed', 7500, $3)"
    ).bind(tenant).bind(prod_2).bind(t_order_2).execute(&source_pool).await?;

    // Drain worker until all dirty outbox rows are aggregated
    while worker.process_one_batch().await? > 0 {}

    // 3. Compare rollup in Analytics DB against Direct-SQL Oracle on Source DB
    let oracle_res = Oracle::count_and_total(&source_pool, tenant, t_ist_hour, t_ist_hour + Duration::hours(1)).await?;

    let rollup_res: (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(order_count), 0)::BIGINT, COALESCE(SUM(sales_paise), 0)::BIGINT \
         FROM agg_hourly WHERE tenant_id = $1 AND hour = $2 AND status = 'completed'"
    )
    .bind(tenant)
    .bind(t_ist_hour)
    .fetch_one(&analytics_pool)
    .await?;

    assert_eq!(rollup_res.0, oracle_res.order_count, "Analytics order_count must match Oracle ground truth");
    assert_eq!(rollup_res.1, oracle_res.total_sales_paise, "Analytics total_sales_paise must match Oracle ground truth");
    println!("✅ Test A Passed: Rollup matches Oracle exactly: {} orders, {} paise", rollup_res.0, rollup_res.1);
    Ok(())
}

#[tokio::test]
async fn test_worker_c_crash_and_restart_convergence() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    let tenant = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Crash-Test')")
        .bind(tenant).execute(&source_pool).await?;
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Crash-Test')")
        .bind(tenant).execute(&analytics_pool).await?;

    let prod = Uuid::new_v4();
    sqlx::query("INSERT INTO products (tenant_id, product_id, category) VALUES ($1, $2, 'Electronics')")
        .bind(tenant).bind(prod).execute(&source_pool).await?;

    // IST aligned hour: 11:30 UTC = 17:00 IST
    let base_hour = DateTime::parse_from_rfc3339("2026-08-01T11:30:00Z")?.to_utc();

    let worker_1 = WorkerEngine::new(source_pool.clone(), analytics_pool.clone(), "worker-c1".to_string(), 10, 1);
    while worker_1.process_one_batch().await? > 0 {}

    sqlx::query(
        "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, 'Electronics', 'completed', 45000, $3)"
    ).bind(tenant).bind(prod).bind(base_hour + Duration::minutes(15)).execute(&source_pool).await?;

    // Worker 1 claims with 1-second lease and publishes, but crashes before acking
    let batch = worker_1.claim_batch().await?.expect("Should claim batch");
    let bucket_hour = batch.rows[0].bucket_hour;

    let targets = worker_1.deduplicate_targets(&batch);
    for (t, ver) in targets {
        let comp = worker_1.recompute_bucket(&t, ver).await?;
        worker_1.publish_bucket(&comp).await?;
    }
    // "CRASH": Worker 1 drops without calling acknowledge_batch!

    // Wait for Worker 1's lease to expire
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Worker 2 starts up (restart recovery)
    let worker_2 = WorkerEngine::new(source_pool.clone(), analytics_pool.clone(), "worker-c2".to_string(), 10, 30);
    let processed = worker_2.process_one_batch().await?;
    assert!(processed > 0, "Worker 2 must reclaim the un-acked expired work");

    // Verify outbox row is now 'done'
    let status: (String,) = sqlx::query_as("SELECT status::text FROM outbox WHERE id = $1")
        .bind(batch.rows[0].id)
        .fetch_one(&source_pool)
        .await?;
    assert_eq!(status.0, "done");

    // Verify analytics table has exactly 1 row (no double counting!)
    let count: (i64, i64) = sqlx::query_as(
        "SELECT order_count, sales_paise FROM agg_hourly WHERE tenant_id = $1 AND hour = $2"
    ).bind(tenant).bind(bucket_hour).fetch_one(&analytics_pool).await?;

    assert_eq!(count.0, 1, "Must have exactly 1 order count despite retry");
    assert_eq!(count.1, 45000, "Must have exactly 45000 sales paise despite retry");

    println!("✅ Test C Passed: Crash recovery converged with zero double counting");
    Ok(())
}

#[tokio::test]
async fn test_worker_d_stale_worker_rejection() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    let tenant = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Stale-Test')")
        .bind(tenant).execute(&source_pool).await?;
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Stale-Test')")
        .bind(tenant).execute(&analytics_pool).await?;

    let prod = Uuid::new_v4();
    sqlx::query("INSERT INTO products (tenant_id, product_id, category) VALUES ($1, $2, 'Electronics')")
        .bind(tenant).bind(prod).execute(&source_pool).await?;

    // IST aligned hour: 11:30 UTC = 17:00 IST
    let base_hour = DateTime::parse_from_rfc3339("2026-07-01T11:30:00Z")?.to_utc();

    let worker_a = WorkerEngine::new(
        source_pool.clone(),
        analytics_pool.clone(),
        "worker-a".to_string(),
        10,
        1, // 1 second lease
    );
    while worker_a.process_one_batch().await? > 0 {}

    // 1. Insert order to create outbox row #1 (Version 1)
    sqlx::query(
        "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, 'Electronics', 'completed', 10000, $3)"
    ).bind(tenant).bind(prod).bind(base_hour + Duration::minutes(10)).execute(&source_pool).await?;

    // Worker A claims batch
    let batch_a = worker_a.claim_batch().await?.expect("Worker A should claim batch");
    let version_a = batch_a.rows[0].id;
    let bucket_hour = batch_a.rows[0].bucket_hour;

    // Simulate Worker A pausing: sleep 2 seconds so lease expires
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // 2. Insert another order in the same hour to create outbox row #2 (Version 2)
    sqlx::query(
        "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
         VALUES ($1, $2, 'Electronics', 'completed', 20000, $3)"
    ).bind(tenant).bind(prod).bind(base_hour + Duration::minutes(20)).execute(&source_pool).await?;

    let worker_b = WorkerEngine::new(
        source_pool.clone(),
        analytics_pool.clone(),
        "worker-b".to_string(),
        10,
        30,
    );

    // Worker B processes and publishes newer version
    let batch_b = worker_b.claim_batch().await?.expect("Worker B should claim batch including expired lease");
    let targets_b = worker_b.deduplicate_targets(&batch_b);
    for (t, ver) in targets_b {
        let comp = worker_b.recompute_bucket(&t, ver).await?;
        let pub_res = worker_b.publish_bucket(&comp).await?;
        assert_eq!(pub_res, PublishResult::Published, "Worker B should successfully publish");
    }
    worker_b.acknowledge_batch(&batch_b).await?;

    // 3. Worker A resumes with its stale computation and tries to publish version_a
    let stale_comp = worker_a.recompute_bucket(
        &BucketTarget { tenant_id: tenant, bucket_hour },
        version_a,
    ).await?;

    let pub_res_a = worker_a.publish_bucket(&stale_comp).await?;
    assert_eq!(pub_res_a, PublishResult::RejectedStale, "Worker A's stale publish MUST be rejected!");

    let acks_a = worker_a.acknowledge_batch(&batch_a).await?;
    assert_eq!(acks_a, 0, "Worker A must not be able to acknowledge stolen batch");

    println!("✅ Test D Passed: Stale worker was rejected cleanly on both publish and ack");
    Ok(())
}

#[tokio::test]
async fn test_worker_b_concurrency_fuzz() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    let tenant = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Fuzz-Test')")
        .bind(tenant).execute(&source_pool).await?;
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Fuzz-Test')")
        .bind(tenant).execute(&analytics_pool).await?;

    let prod = Uuid::new_v4();
    sqlx::query("INSERT INTO products (tenant_id, product_id, category) VALUES ($1, $2, 'Electronics')")
        .bind(tenant).bind(prod).execute(&source_pool).await?;

    // IST aligned hour: 11:30 UTC = 17:00 IST
    let base_hour = DateTime::parse_from_rfc3339("2026-09-01T11:30:00Z")?.to_utc();

    let worker = Arc::new(WorkerEngine::new(
        source_pool.clone(),
        analytics_pool.clone(),
        "worker-fuzz".to_string(),
        50,
        30,
    ));
    while worker.process_one_batch().await? > 0 {}

    // Spawn 4 writer tasks making 25 mutations each (100 mutations total)
    let mut handles = Vec::new();
    for writer_id in 0..4 {
        let pool = source_pool.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..25 {
                let amt = ((writer_id * 100) + i + 10) as i64 * 1000;
                sqlx::query(
                    "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
                     VALUES ($1, $2, 'Electronics', 'completed', $3, $4)"
                )
                .bind(tenant)
                .bind(prod)
                .bind(amt)
                .bind(base_hour + Duration::minutes(i % 50))
                .execute(&pool)
                .await
                .unwrap();
            }
        }));
    }

    let w_clone = worker.clone();
    let stop_signal = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_signal.clone();
    let worker_handle = tokio::spawn(async move {
        while !stop_clone.load(Ordering::Relaxed) {
            let _ = w_clone.process_one_batch().await;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    });

    for h in handles {
        h.await?;
    }
    stop_signal.store(true, Ordering::Relaxed);
    let _ = worker_handle.await;

    // Drain any remaining outbox rows
    while worker.process_one_batch().await? > 0 {}

    // Assert final rollup in Analytics DB matches Oracle directly
    let oracle = Oracle::count_and_total(&source_pool, tenant, base_hour, base_hour + Duration::hours(1)).await?;
    let rollup: (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(order_count), 0)::BIGINT, COALESCE(SUM(sales_paise), 0)::BIGINT \
         FROM agg_hourly WHERE tenant_id = $1 AND hour = $2 AND status = 'completed'"
    ).bind(tenant).bind(base_hour).fetch_one(&analytics_pool).await?;

    assert_eq!(rollup.0, oracle.order_count, "Fuzz test: order count must match Oracle");
    assert_eq!(rollup.1, oracle.total_sales_paise, "Fuzz test: total sales must match Oracle");

    println!("✅ Test B Passed: Concurrency fuzz converged perfectly: {} orders, {} paise", rollup.0, rollup.1);
    Ok(())
}

#[tokio::test]
async fn test_worker_e_concurrent_workers_same_bucket() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    let source_pool = PgPool::connect(&config.source_database_url).await?;
    let analytics_pool = PgPool::connect(&config.analytics_database_url).await?;

    let tenant = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Two-Workers-Test')")
        .bind(tenant).execute(&source_pool).await?;
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, 'Worker-Two-Workers-Test')")
        .bind(tenant).execute(&analytics_pool).await?;

    let prod = Uuid::new_v4();
    sqlx::query("INSERT INTO products (tenant_id, product_id, category) VALUES ($1, $2, 'Electronics')")
        .bind(tenant).bind(prod).execute(&source_pool).await?;

    let base_hour = DateTime::parse_from_rfc3339("2026-10-15T09:30:00Z")?.to_utc();

    let worker_1 = Arc::new(WorkerEngine::new(
        source_pool.clone(),
        analytics_pool.clone(),
        "worker-concurrent-1".to_string(),
        10,
        30,
    ));
    let worker_2 = Arc::new(WorkerEngine::new(
        source_pool.clone(),
        analytics_pool.clone(),
        "worker-concurrent-2".to_string(),
        10,
        30,
    ));

    // Drain initial
    while worker_1.process_one_batch().await? > 0 {}

    // Insert 5 orders in rapid succession
    for i in 1..=5 {
        sqlx::query(
            "INSERT INTO orders (tenant_id, product_id, category, status, amount_paise, created_at) \
             VALUES ($1, $2, 'Electronics', 'completed', $3, $4)"
        )
        .bind(tenant)
        .bind(prod)
        .bind((i * 10000) as i64)
        .bind(base_hour + Duration::minutes(i * 5))
        .execute(&source_pool)
        .await?;
    }

    // Run both workers concurrently
    let w1 = worker_1.clone();
    let w2 = worker_2.clone();

    let h1 = tokio::spawn(async move {
        for _ in 0..5 {
            let _ = w1.process_one_batch().await;
        }
    });
    let h2 = tokio::spawn(async move {
        for _ in 0..5 {
            let _ = w2.process_one_batch().await;
        }
    });

    h1.await?;
    h2.await?;

    // Drain any remaining
    while worker_1.process_one_batch().await? > 0 {}

    // Verify convergence against Oracle
    let oracle = Oracle::count_and_total(&source_pool, tenant, base_hour, base_hour + Duration::hours(1)).await?;
    let rollup: (i64, i64) = sqlx::query_as(
        "SELECT order_count, sales_paise FROM agg_hourly WHERE tenant_id = $1 AND hour = $2"
    ).bind(tenant).bind(base_hour).fetch_one(&analytics_pool).await?;

    assert_eq!(rollup.0, 5, "Must aggregate all 5 orders");
    assert_eq!(rollup.0, oracle.order_count);
    assert_eq!(rollup.1, oracle.total_sales_paise);

    println!("✅ Test E Passed: Two concurrent workers on same bucket converged with no stale overwrite");
    Ok(())
}
