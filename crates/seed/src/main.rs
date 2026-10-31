mod updater;
mod zipf;

use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use common::{init_telemetry, AppConfig};
use futures_util::SinkExt;
use rand::{rngs::StdRng, Rng, SeedableRng};
use sqlx::PgPool;
use std::time::Instant;
use tokio_postgres::NoTls;
use tracing::info;
use uuid::Uuid;
use zipf::ZipfSampler;

#[derive(Parser, Debug)]
#[command(name = "seed", about = "High-performance synthetic order seeder for multi-tenant aggregation")]
struct Args {
    #[arg(long, default_value = "seed")]
    mode: String,

    #[arg(long, default_value_t = 100)]
    tenants: usize,

    #[arg(long, default_value_t = 1_000_000)]
    rows: usize,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    #[arg(long, default_value_t = 1000)]
    update_count: usize,

    #[arg(long, default_value_t = true)]
    truncate: bool,

    #[arg(long, default_value_t = 100_000)]
    chunk_size: usize,
}

struct ProductInfo {
    product_id: Uuid,
    category: &'static str,
    base_price_paise: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_telemetry("seed");
    let args = Args::parse();
    let config = AppConfig::from_env()?;

    let pool = PgPool::connect(&config.source_database_url).await?;

    if args.mode == "update" {
        updater::run_update_mutations(&pool, args.update_count, args.seed).await?;
        return Ok(());
    }

    info!(
        "Starting high-speed ingestion: {} rows across {} tenants (seed={})...",
        args.rows, args.tenants, args.seed
    );

    let start_total = Instant::now();
    let mut rng = StdRng::seed_from_u64(args.seed);

    // 1. Clean existing state if requested
    if args.truncate {
        info!("Truncating existing tables in source_db...");
        sqlx::query("TRUNCATE TABLE orders, products, customers, tenants CASCADE")
            .execute(&pool)
            .await?;
    }

    // 2. Generate and persist deterministic Tenants, Products, Customers in fast batches
    info!("Generating {} tenants with products and customers...", args.tenants);
    let mut tenant_ids = Vec::with_capacity(args.tenants);
    let mut tenant_products: Vec<Vec<ProductInfo>> = Vec::with_capacity(args.tenants);

    let categories = ["Electronics", "Apparel", "Home", "Books", "Food"];

    // Build tenant batch query
    let mut tenant_query = String::from("INSERT INTO tenants (id, name) VALUES ");
    for t_idx in 0..args.tenants {
        let tenant_id = Uuid::from_u128(((t_idx as u128 + 1) << 64) | 0x01);
        let tenant_name = format!("Tenant-{:03}", t_idx + 1);
        tenant_ids.push(tenant_id);

        if t_idx > 0 {
            tenant_query.push_str(", ");
        }
        tenant_query.push_str(&format!("('{}', '{}')", tenant_id, tenant_name));
    }
    tenant_query.push_str(" ON CONFLICT (id) DO NOTHING");
    sqlx::query(&tenant_query).execute(&pool).await?;

    // Build product and customer batch queries
    let mut product_query = String::from("INSERT INTO products (tenant_id, product_id, category) VALUES ");
    let mut customer_query = String::from("INSERT INTO customers (tenant_id, customer_id, name) VALUES ");

    let mut first_p = true;
    let mut first_c = true;

    for t_idx in 0..args.tenants {
        let tenant_id = tenant_ids[t_idx];
        let mut products = Vec::with_capacity(20);

        for p_idx in 0..20 {
            let product_id = Uuid::from_u128(
                ((t_idx as u128 + 1) << 64) | ((p_idx as u128 + 1) << 16) | 0x02
            );
            let category = categories[p_idx % categories.len()];
            let price_points = [49900, 99900, 149900, 199900, 299900, 499900, 999900];
            let base_price = price_points[p_idx % price_points.len()];

            if !first_p {
                product_query.push_str(", ");
            }
            product_query.push_str(&format!("('{}', '{}', '{}')", tenant_id, product_id, category));
            first_p = false;

            products.push(ProductInfo {
                product_id,
                category,
                base_price_paise: base_price,
            });
        }
        tenant_products.push(products);

        for c_idx in 0..50 {
            let customer_id = Uuid::from_u128(
                ((t_idx as u128 + 1) << 64) | ((c_idx as u128 + 1) << 16) | 0x03
            );
            let customer_name = format!("Customer-{:03}-{:03}", t_idx + 1, c_idx + 1);

            if !first_c {
                customer_query.push_str(", ");
            }
            customer_query.push_str(&format!("('{}', '{}', '{}')", tenant_id, customer_id, customer_name));
            first_c = false;
        }
    }

    product_query.push_str(" ON CONFLICT DO NOTHING");
    customer_query.push_str(" ON CONFLICT DO NOTHING");

    sqlx::query(&product_query).execute(&pool).await?;
    sqlx::query(&customer_query).execute(&pool).await?;

    // 3. Drop secondary indexes for bulk COPY speed
    info!("Dropping secondary indexes for deferred indexing...");
    sqlx::query("DROP INDEX IF EXISTS idx_orders_tenant_created").execute(&pool).await?;
    sqlx::query("DROP INDEX IF EXISTS idx_orders_tenant_product_created").execute(&pool).await?;
    sqlx::query("DROP INDEX IF EXISTS idx_orders_tenant_status_created").execute(&pool).await?;

    // 4. Setup high-speed tokio-postgres connection for streaming COPY
    let (client, connection) = tokio_postgres::connect(&config.source_database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Connection error in background copy connection: {}", e);
        }
    });

    let copy_sink = client
        .copy_in(
            "COPY orders (tenant_id, product_id, category, status, amount_paise, created_at, updated_at, deleted_at) \
             FROM STDIN WITH (FORMAT text)"
        )
        .await?;
    tokio::pin!(copy_sink);

    // 5. Setup distributions
    // Zipf parameter s = 0.85 yields ~40% for the top 5 tenants out of 100
    let tenant_zipf = ZipfSampler::new(args.tenants, 0.85);
    // Product Zipf for uneven product popularity
    let product_zipf = ZipfSampler::new(20, 0.90);

    let statuses = ["completed", "completed", "completed", "completed", "completed", "pending", "cancelled", "refunded"];

    // Base timestamp: 365 days ago up to now
    let now = Utc::now();
    let start_date = now - Duration::days(365);
    let total_seconds = 365 * 86400;

    info!("Streaming {} rows via PostgreSQL COPY in batches of {}...", args.rows, args.chunk_size);

    let copy_start = Instant::now();
    let mut total_rows = 0;
    let mut buffer = String::with_capacity(args.chunk_size * 128);

    while total_rows < args.rows {
        let batch_size = (args.rows - total_rows).min(args.chunk_size);
        buffer.clear();

        for _ in 0..batch_size {
            // Select tenant via Zipf
            let t_rank = tenant_zipf.sample(&mut rng);
            let tenant_id = tenant_ids[t_rank];

            // Select product via Zipf
            let p_rank = product_zipf.sample(&mut rng);
            let product = &tenant_products[t_rank][p_rank];

            // Status selection
            let status = statuses[rng.gen_range(0..statuses.len())];

            // Amount: base price with occasional quantity multiplier (1x to 4x)
            let qty = rng.gen_range(1..=4);
            let amount_paise = product.base_price_paise * qty;

            // Timestamp generation:
            // 20% of timestamps cluster around IST midnight (23:30 to 00:30 IST)
            let is_midnight_edge = rng.gen_bool(0.20);
            let created_at: DateTime<Utc> = if is_midnight_edge {
                // Random day within the year
                let day_offset = rng.gen_range(0..365);
                let day = start_date + Duration::days(day_offset);
                // Cluster around 23:30 to 00:30 IST (which is 18:00 to 19:00 UTC)
                let minute_offset = rng.gen_range(-30..=30);
                day.date_naive().and_hms_opt(18, 30, 0).unwrap().and_utc() + Duration::minutes(minute_offset)
            } else {
                let sec_offset = rng.gen_range(0..total_seconds);
                start_date + Duration::seconds(sec_offset)
            };

            // Format TSV row:
            // tenant_id \t product_id \t category \t status \t amount_paise \t created_at \t updated_at \t \N
            use std::fmt::Write;
            let _ = writeln!(
                buffer,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t\\N",
                tenant_id,
                product.product_id,
                product.category,
                status,
                amount_paise,
                created_at.to_rfc3339(),
                created_at.to_rfc3339()
            );
        }

        copy_sink.send(Bytes::from(buffer.clone())).await?;
        total_rows += batch_size;

        if total_rows % 500_000 == 0 || total_rows == args.rows {
            let elapsed_sec = copy_start.elapsed().as_secs_f64();
            let rps = (total_rows as f64) / elapsed_sec;
            info!(
                "Progress: {} / {} rows ({:.1}%) - {:.0} rows/sec",
                total_rows,
                args.rows,
                (total_rows as f64 / args.rows as f64) * 100.0,
                rps
            );
        }
    }

    // Finish COPY stream
    copy_sink.finish().await?;
    let copy_elapsed = copy_start.elapsed();
    info!(
        "✅ COPY phase completed: {} rows inserted in {:.2}s ({:.0} rows/sec)",
        total_rows,
        copy_elapsed.as_secs_f64(),
        (total_rows as f64) / copy_elapsed.as_secs_f64()
    );

    // 6. Recreate indexes concurrently/sequentially
    info!("Rebuilding composite indexes on orders table...");
    let idx_start = Instant::now();
    sqlx::query("CREATE INDEX idx_orders_tenant_created ON orders (tenant_id, created_at)")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE INDEX idx_orders_tenant_product_created ON orders (tenant_id, product_id, created_at)")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE INDEX idx_orders_tenant_status_created ON orders (tenant_id, status, created_at)")
        .execute(&pool)
        .await?;
    info!("Indexes rebuilt in {:.2}s", idx_start.elapsed().as_secs_f64());

    // 7. Run ANALYZE
    info!("Running ANALYZE on source database tables...");
    let analyze_start = Instant::now();
    sqlx::query("ANALYZE orders").execute(&pool).await?;
    sqlx::query("ANALYZE products").execute(&pool).await?;
    sqlx::query("ANALYZE tenants").execute(&pool).await?;
    info!("ANALYZE completed in {:.2}s", analyze_start.elapsed().as_secs_f64());

    // 8. Print Skew Verification Report
    info!("=== Distribution & Skew Report ===");
    let top_tenants: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT tenant_id, COUNT(*) as cnt FROM orders GROUP BY tenant_id ORDER BY cnt DESC LIMIT 5"
    )
    .fetch_all(&pool)
    .await?;

    let top_5_sum: i64 = top_tenants.iter().map(|t| t.1).sum();
    let top_5_pct = (top_5_sum as f64 / total_rows as f64) * 100.0;

    for (rank, (tid, cnt)) in top_tenants.iter().enumerate() {
        let pct = (*cnt as f64 / total_rows as f64) * 100.0;
        info!("  Rank #{}: Tenant {} => {} rows ({:.2}%)", rank + 1, tid, cnt, pct);
    }
    info!("  👉 Top 5 tenants total: {} / {} rows ({:.2}% of all rows - target ~40%)", top_5_sum, total_rows, top_5_pct);

    let bottom_tenants: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT tenant_id, COUNT(*) as cnt FROM orders GROUP BY tenant_id ORDER BY cnt ASC LIMIT 5"
    )
    .fetch_all(&pool)
    .await?;

    info!("  Bottom 5 tenants:");
    for (rank, (tid, cnt)) in bottom_tenants.iter().enumerate() {
        let pct = (*cnt as f64 / total_rows as f64) * 100.0;
        info!("  Tail #{}: Tenant {} => {} rows ({:.3}%)", rank + 1, tid, cnt, pct);
    }

    let status_breakdown: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY cnt DESC"
    )
    .fetch_all(&pool)
    .await?;

    info!("Status Distribution:");
    for (st, cnt) in status_breakdown {
        let pct = (cnt as f64 / total_rows as f64) * 100.0;
        info!("  Status '{}' => {} rows ({:.1}%)", st, cnt, pct);
    }

    let total_elapsed = start_total.elapsed();
    info!(
        "🎉 High-volume seeding complete in {:.2}s ({:.0} effective rows/sec total)",
        total_elapsed.as_secs_f64(),
        (total_rows as f64) / total_elapsed.as_secs_f64()
    );

    Ok(())
}
