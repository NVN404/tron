mod zipf;

use adapter_mongo::MongoAdapter;
use adapter_mysql::MysqlAdapter;
use adapter_postgres::PostgresAdapter;
use anyhow::{Context, Result};
use api::hash_api_key;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use futures_util::SinkExt;
use mongodb::bson::{doc, DateTime as BsonDateTime};
use platform_core::config::{AppConfig, SourceKind};
use rand::{rngs::StdRng, Rng, SeedableRng};
use sqlx::postgres::PgPoolOptions;
use std::path::Path;
use std::time::Instant;
use tokio_postgres::NoTls;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use zipf::ZipfSampler;

#[derive(Parser, Debug)]
#[command(name = "seed", about = "High-performance multi-source synthetic order seeder")]
pub struct Args {
    #[arg(long, default_value = "all")]
    pub sources: String,

    #[arg(long, default_value_t = 100)]
    pub tenants: usize,

    #[arg(long, default_value_t = 100_000)]
    pub rows: usize,

    #[arg(long, default_value_t = 42)]
    pub seed: u64,

    #[arg(long, default_value_t = true)]
    pub truncate: bool,

    #[arg(long, default_value_t = 5000)]
    pub chunk_size: usize,
}

fn load_seed_config() -> Result<AppConfig> {
    dotenvy::dotenv().ok();
    let dir = if Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    AppConfig::load_from_dir(dir, None)
}

struct ProductInfo {
    product_id: String,
    category: &'static str,
    base_price_minor: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    let config = load_seed_config()?;

    info!("Connecting to Analytics DB to register sources and tenants...");
    let admin_dsn = config.analytics.resolve_dsn()?;
    let analytics_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&admin_dsn)
        .await
        .context("Failed to connect to analytics DB")?;

    let target_sources: Vec<_> = config
        .sources
        .iter()
        .filter(|s| args.sources == "all" || args.sources.split(',').any(|x| x.trim() == s.id))
        .collect();

    if target_sources.is_empty() {
        warn!("No matching sources found for '{}'", args.sources);
        return Ok(());
    }

    let tenants_per_source = (args.tenants / target_sources.len()).max(1);
    let rows_per_source = args.rows / target_sources.len();

    info!(
        "Seeding {} sources ({} tenants/source, {} rows/source, total rows={}, seed={})",
        target_sources.len(),
        tenants_per_source,
        rows_per_source,
        args.rows,
        args.seed
    );

    let categories = ["Electronics", "Apparel", "Home", "Books", "Food"];
    let statuses = [
        "completed", "completed", "completed", "completed", "completed",
        "pending", "cancelled", "refunded",
    ];
    let price_points = [49900, 99900, 149900, 199900, 299900, 499900, 999900];

    let now = Utc::now();
    let start_date = now - Duration::days(30);
    let total_seconds = 30 * 86400;

    for (src_idx, src) in target_sources.iter().enumerate() {
        let src_rng_seed = args.seed + (src_idx as u64 * 1000);
        let mut rng = StdRng::seed_from_u64(src_rng_seed);
        let tenant_zipf = ZipfSampler::new(tenants_per_source, 0.85);
        let product_zipf = ZipfSampler::new(20, 0.90);

        info!("=== Setting up and seeding source: {} ({:?}) ===", src.id, src.kind);

        // 1. Register source in Analytics DB
        sqlx::query("INSERT INTO sources (id, kind, capture_mode) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET capture_mode = EXCLUDED.capture_mode")
            .bind(&src.id)
            .bind(format!("{:?}", src.kind).to_lowercase())
            .bind(format!("{:?}", src.capture).to_lowercase())
            .execute(&analytics_pool)
            .await?;

        // 2. Generate tenants and register in Analytics DB + API Keys
        let mut ext_tenant_ids = Vec::with_capacity(tenants_per_source);
        let mut tenant_products: Vec<Vec<ProductInfo>> = Vec::with_capacity(tenants_per_source);

        for t_idx in 0..tenants_per_source {
            let ext_id = format!("ext_{}_{:03}", src.id, t_idx + 1);
            let tenant_uuid = Uuid::new_v5(&Uuid::NAMESPACE_DNS, format!("{}:{}", src.id, ext_id).as_bytes());
            ext_tenant_ids.push(ext_id.clone());

            let tenant_name = format!("Tenant-{}-{:03}", src.id, t_idx + 1);
            sqlx::query(
                "INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health)
                 VALUES ($1, $2, $3, $4, 'healthy')
                 ON CONFLICT (tenant_uuid) DO NOTHING"
            )
            .bind(tenant_uuid)
            .bind(&src.id)
            .bind(&ext_id)
            .bind(&tenant_name)
            .execute(&analytics_pool)
            .await?;

            let token = format!("token-{}", tenant_uuid);
            let key_hash = hash_api_key(&token);
            sqlx::query(
                "INSERT INTO api_keys (key_hash, tenant_uuid, name)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (key_hash) DO NOTHING"
            )
            .bind(&key_hash)
            .bind(tenant_uuid)
            .bind(format!("Key for {}", tenant_name))
            .execute(&analytics_pool)
            .await?;

            let mut prods = Vec::with_capacity(20);
            for p_idx in 0..20 {
                let p_id = format!("prod_{}_{}", t_idx + 1, p_idx + 1);
                let cat = categories[p_idx % categories.len()];
                let base = price_points[p_idx % price_points.len()];
                prods.push(ProductInfo {
                    product_id: p_id,
                    category: cat,
                    base_price_minor: base,
                });
            }
            tenant_products.push(prods);
        }

        // 3. Setup source schema & Truncate if requested
        match src.kind {
            SourceKind::Postgres => {
                let adapter = PostgresAdapter::new((*src).clone()).await?;
                adapter.setup_schema().await?;
                let pool = adapter.pool();
                let entity = src.mapping.entity.as_str();

                if args.truncate {
                    info!("Truncating Postgres table {}...", entity);
                    sqlx::query(&format!("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;", entity))
                        .execute(pool)
                        .await?;
                    sqlx::query(&format!("TRUNCATE TABLE _{}_outbox RESTART IDENTITY CASCADE;", entity))
                        .execute(pool)
                        .await?;
                }

                info!("Seeding {} rows into Postgres {}...", rows_per_source, src.id);
                let dsn = src.resolve_dsn()?;
                let (client, conn) = tokio_postgres::connect(&dsn, NoTls).await?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::error!("Background connection error: {}", e);
                    }
                });

                let copy_query = format!(
                    "COPY {} ({}, {}, {}, {}, {}, {}, {}) FROM STDIN WITH (FORMAT text)",
                    entity,
                    src.mapping.id_field.as_str(),
                    src.mapping.tenant_id_field.as_str(),
                    src.mapping.product_id_field.as_str(),
                    src.mapping.category_field.as_str(),
                    src.mapping.status_field.as_str(),
                    src.mapping.amount_field.as_str(),
                    src.mapping.created_at_field.as_str(),
                );

                let sink = client.copy_in(&copy_query).await?;
                tokio::pin!(sink);

                let mut total_written = 0;
                let mut buffer = String::with_capacity(args.chunk_size * 128);
                let start = Instant::now();

                while total_written < rows_per_source {
                    let batch = (rows_per_source - total_written).min(args.chunk_size);
                    buffer.clear();

                    for _ in 0..batch {
                        let t_rank = tenant_zipf.sample(&mut rng);
                        let ext_tenant = &ext_tenant_ids[t_rank];
                        let p_rank = product_zipf.sample(&mut rng);
                        let prod = &tenant_products[t_rank][p_rank];
                        let status = statuses[rng.gen_range(0..statuses.len())];
                        let qty = rng.gen_range(1..=4);
                        let amount = prod.base_price_minor * qty;
                        let order_id = Uuid::new_v4();

                        let is_edge = rng.gen_bool(0.20);
                        let dt: DateTime<Utc> = if is_edge {
                            let day_offset = rng.gen_range(0..30);
                            let day = start_date + Duration::days(day_offset);
                            let min_offset = rng.gen_range(-30..=30);
                            day.date_naive().and_hms_opt(18, 30, 0).unwrap().and_utc() + Duration::minutes(min_offset)
                        } else {
                            start_date + Duration::seconds(rng.gen_range(0..total_seconds))
                        };

                        use std::fmt::Write;
                        let _ = writeln!(
                            buffer,
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                            order_id,
                            ext_tenant,
                            prod.product_id,
                            prod.category,
                            status,
                            amount,
                            dt.to_rfc3339()
                        );
                    }

                    sink.send(Bytes::from(buffer.clone())).await?;
                    total_written += batch;
                }

                sink.finish().await?;
                let elapsed = start.elapsed().as_secs_f64();
                info!(
                    "Postgres {} seeded: {} rows in {:.2}s ({:.0} rows/s)",
                    src.id, total_written, elapsed, total_written as f64 / elapsed.max(0.001)
                );
            }
            SourceKind::Mysql => {
                let adapter = MysqlAdapter::new((*src).clone()).await?;
                adapter.setup_schema().await?;
                let pool = adapter.pool();
                let entity = src.mapping.entity.as_str();

                if args.truncate {
                    info!("Truncating MySQL table {}...", entity);
                    sqlx::query(&format!("TRUNCATE TABLE {};", entity))
                        .execute(pool)
                        .await?;
                    sqlx::query(&format!("TRUNCATE TABLE _{}_outbox;", entity))
                        .execute(pool)
                        .await?;
                }

                info!("Seeding {} rows into MySQL {}...", rows_per_source, src.id);
                let start = Instant::now();
                let mut total_written = 0;
                let chunk = args.chunk_size.min(1000); // MySQL param limit

                while total_written < rows_per_source {
                    let batch = (rows_per_source - total_written).min(chunk);
                    let mut query = format!(
                        "INSERT INTO {} ({}, {}, {}, {}, {}, {}, {}) VALUES ",
                        entity,
                        src.mapping.id_field.as_str(),
                        src.mapping.tenant_id_field.as_str(),
                        src.mapping.product_id_field.as_str(),
                        src.mapping.category_field.as_str(),
                        src.mapping.status_field.as_str(),
                        src.mapping.amount_field.as_str(),
                        src.mapping.created_at_field.as_str(),
                    );

                    for b_idx in 0..batch {
                        let t_rank = tenant_zipf.sample(&mut rng);
                        let ext_tenant = &ext_tenant_ids[t_rank];
                        let p_rank = product_zipf.sample(&mut rng);
                        let prod = &tenant_products[t_rank][p_rank];
                        let status = statuses[rng.gen_range(0..statuses.len())];
                        let qty = rng.gen_range(1..=4);
                        let amount = prod.base_price_minor * qty;
                        let order_id = format!("my_{}", Uuid::new_v4().simple());
                        let dt = start_date + Duration::seconds(rng.gen_range(0..total_seconds));
                        let dt_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();

                        if b_idx > 0 {
                            query.push_str(", ");
                        }
                        query.push_str(&format!(
                            "('{}', '{}', '{}', '{}', '{}', {}, '{}')",
                            order_id, ext_tenant, prod.product_id, prod.category, status, amount, dt_str
                        ));
                    }

                    sqlx::query(&query).execute(pool).await?;
                    total_written += batch;
                }

                let elapsed = start.elapsed().as_secs_f64();
                info!(
                    "MySQL {} seeded: {} rows in {:.2}s ({:.0} rows/s)",
                    src.id, total_written, elapsed, total_written as f64 / elapsed.max(0.001)
                );
            }
            SourceKind::Mongo => {
                let adapter = MongoAdapter::new((*src).clone()).await?;
                adapter.setup_schema().await?;
                let coll = adapter.collection();

                if args.truncate {
                    info!("Truncating Mongo collection {}...", src.mapping.entity.as_str());
                    coll.delete_many(doc! {}).await?;
                }

                info!("Seeding {} rows into MongoDB {}...", rows_per_source, src.id);
                let start = Instant::now();
                let mut total_written = 0;
                let chunk = args.chunk_size.min(1000);

                while total_written < rows_per_source {
                    let batch = (rows_per_source - total_written).min(chunk);
                    let mut docs = Vec::with_capacity(batch);

                    for _ in 0..batch {
                        let t_rank = tenant_zipf.sample(&mut rng);
                        let ext_tenant = &ext_tenant_ids[t_rank];
                        let p_rank = product_zipf.sample(&mut rng);
                        let prod = &tenant_products[t_rank][p_rank];
                        let status = statuses[rng.gen_range(0..statuses.len())];
                        let qty = rng.gen_range(1..=4);
                        let amount = prod.base_price_minor * qty;
                        let order_id = format!("mg_{}", Uuid::new_v4().simple());
                        let dt = start_date + Duration::seconds(rng.gen_range(0..total_seconds));

                        let d = doc! {
                            src.mapping.id_field.as_str(): order_id,
                            src.mapping.tenant_id_field.as_str(): ext_tenant,
                            src.mapping.product_id_field.as_str(): &prod.product_id,
                            src.mapping.category_field.as_str(): prod.category,
                            src.mapping.status_field.as_str(): status,
                            src.mapping.amount_field.as_str(): amount,
                            src.mapping.created_at_field.as_str(): BsonDateTime::from_millis(dt.timestamp_millis()),
                        };
                        docs.push(d);
                    }

                    coll.insert_many(docs).await?;
                    total_written += batch;
                }

                let elapsed = start.elapsed().as_secs_f64();
                info!(
                    "MongoDB {} seeded: {} rows in {:.2}s ({:.0} rows/s)",
                    src.id, total_written, elapsed, total_written as f64 / elapsed.max(0.001)
                );
            }
        }
    }

    info!("All sources seeded successfully!");
    Ok(())
}
