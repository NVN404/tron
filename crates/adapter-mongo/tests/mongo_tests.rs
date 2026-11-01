use adapter_mongo::MongoAdapter;
use chrono::{Duration, TimeZone, Utc};
use mongodb::bson::{doc, DateTime as BsonDateTime};
use platform_core::config::{AppConfig, SourceConfig};
use platform_core::traits::BucketReader;
use platform_core::types::BucketKey;
use std::path::Path;
use uuid::Uuid;

fn get_mongo_config() -> SourceConfig {
    dotenvy::dotenv().ok();
    let dir = if Path::new("config/default.toml").exists() {
        "config"
    } else {
        "../../config"
    };
    let app_config = AppConfig::load_from_dir(dir, Some("test")).expect("load config");
    app_config
        .sources
        .into_iter()
        .find(|s| s.id == "mongo_source")
        .expect("mongo_source config")
}

#[tokio::test]
async fn test_mongo_aggregation_recompute() {
    let config = get_mongo_config();
    let adapter = MongoAdapter::new(config.clone()).await.unwrap();

    // 1. Setup schema & indexes
    adapter.setup_schema().await.unwrap();

    let tenant_id = format!("mongo_ten_{}", Uuid::new_v4().simple());
    let hour = Utc.with_ymd_and_hms(2026, 11, 1, 16, 0, 0).unwrap();
    let created_at = hour + Duration::minutes(30);

    let coll = adapter.collection();

    // 2. Insert documents into Mongo collection
    let doc1 = doc! {
        "order_id": format!("order_{}", Uuid::new_v4().simple()),
        "tenant_id": &tenant_id,
        "product_id": "prod_mongo_1",
        "category": "apparel",
        "amount_cents": 4500i64,
        "status": "completed",
        "created_at": BsonDateTime::from_millis(created_at.timestamp_millis())
    };

    let doc2 = doc! {
        "order_id": format!("order_{}", Uuid::new_v4().simple()),
        "tenant_id": &tenant_id,
        "product_id": "prod_mongo_2",
        "category": "footwear",
        "amount_cents": 8500i64,
        "status": "completed",
        "created_at": BsonDateTime::from_millis(created_at.timestamp_millis())
    };

    coll.insert_many(vec![doc1, doc2]).await.unwrap();

    // 3. Compute bucket aggregate
    let expected_key = BucketKey::new(&config.id, &tenant_id, hour);
    let agg = adapter.compute(&expected_key).await.unwrap();

    assert_eq!(agg.key, expected_key);
    assert_eq!(agg.status_aggregates.len(), 1);
    assert_eq!(agg.status_aggregates[0].status, "completed");
    assert_eq!(agg.status_aggregates[0].order_count, 2);
    assert_eq!(agg.status_aggregates[0].sales_units, 13000);

    assert_eq!(agg.product_aggregates.len(), 2);
    let total_prod_sales: i64 = agg.product_aggregates.iter().map(|p| p.sales_units).sum();
    assert_eq!(total_prod_sales, 13000);
    let total_units: i64 = agg.product_aggregates.iter().map(|p| p.units).sum();
    assert_eq!(total_units, 2);
}

