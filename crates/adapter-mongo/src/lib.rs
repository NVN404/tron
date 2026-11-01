use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use mongodb::bson::{doc, Bson, DateTime as BsonDateTime, Document};
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::{ChangeStreamOptions, ClientOptions, FullDocumentType};
use mongodb::{Client, Collection, Database};
use platform_core::config::SourceConfig;
use platform_core::traits::{BucketReader, ChangeSource, Snapshot};
use platform_core::types::{
    AckHandle, BucketAggregate, BucketKey, ChangeBatch, Cursor, ProductAggregate, StatusAggregate,
};
use std::collections::HashSet;
use std::time::Duration as StdDuration;

pub struct MongoAdapter {
    config: SourceConfig,
    client: Client,
    db: Database,
}

impl MongoAdapter {
    pub async fn new(config: SourceConfig) -> Result<Self> {
        let dsn = config.resolve_dsn()?;
        let client_options = ClientOptions::parse(&dsn)
            .await
            .with_context(|| format!("Failed to parse Mongo DSN for {}", config.id))?;
        let client = Client::with_options(client_options.clone())?;
        let db_name = client_options
            .default_database
            .clone()
            .unwrap_or_else(|| "source_db".to_string());
        let db = client.database(&db_name);

        Ok(Self { config, client, db })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn collection(&self) -> Collection<Document> {
        let entity = self.config.mapping.entity.as_str();
        self.db.collection(entity)
    }

    /// Sets up collection, indexes, and enables pre/post images on MongoDB.
    pub async fn setup_schema(&self) -> Result<()> {
        let entity = self.config.mapping.entity.as_str();
        let tenant_field = self.config.mapping.tenant_id_field.as_str();
        let created_field = self.config.mapping.created_at_field.as_str();

        let coll = self.collection();

        // 1. Create compound index on { tenant_id: 1, created_at: 1 }
        let index_model = mongodb::IndexModel::builder()
            .keys(doc! { tenant_field: 1, created_field: 1 })
            .build();
        let _ = coll.create_index(index_model).await;

        // 2. Enable pre/post images on MongoDB 6+ for change streams
        let coll_mod = doc! {
            "collMod": entity,
            "changeStreamPreAndPostImages": { "enabled": true }
        };
        let _ = self.db.run_command(coll_mod).await;

        Ok(())
    }
}

#[async_trait]
impl ChangeSource for MongoAdapter {
    async fn poll(&self, cursor: Option<Cursor>) -> Result<ChangeBatch> {
        let coll = self.collection();
        let mut options = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();

        if let Some(c) = cursor {
            if let Ok(resume_token) = serde_json::from_str::<ResumeToken>(&c) {
                options.resume_after = Some(resume_token);
            }
        }

        let mut stream = match coll.watch().with_options(options).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to open Mongo change stream, opening fresh: {}", e);
                coll.watch().await?
            }
        };

        let mut next_cursor = None;
        let mut seen = HashSet::new();
        let mut bucket_keys = Vec::new();

        // Poll with 500ms timeout up to 1000 items
        let start = std::time::Instant::now();
        while start.elapsed() < StdDuration::from_millis(500) && bucket_keys.len() < 1000 {
            let next_item = tokio::time::timeout(StdDuration::from_millis(100), stream.next()).await;
            match next_item {
                Ok(Some(Ok(event))) => {
                    next_cursor = Some(serde_json::to_string(&event.id).unwrap_or_default());

                    let extract_bucket = |doc_opt: Option<&Document>| -> Option<BucketKey> {
                        let doc = doc_opt?;
                        let tenant_field = self.config.mapping.tenant_id_field.as_str();
                        let created_field = self.config.mapping.created_at_field.as_str();

                        let tenant_id = doc.get_str(tenant_field).ok()?.to_string();
                        let dt = match doc.get(created_field) {
                            Some(Bson::DateTime(bdt)) => DateTime::from_timestamp_millis(bdt.timestamp_millis())?,
                            Some(Bson::String(s)) => DateTime::parse_from_rfc3339(s).ok()?.with_timezone(&Utc),
                            _ => return None,
                        };

                        let timestamp = dt.timestamp();
                        let hour_secs = timestamp - (timestamp % 3600);
                        let hour_start = DateTime::from_timestamp(hour_secs, 0)?;
                        Some(BucketKey::new(&self.config.id, tenant_id, hour_start))
                    };

                    // New bucket key from full_document
                    if let Some(key) = extract_bucket(event.full_document.as_ref()) {
                        if seen.insert(key.clone()) {
                            bucket_keys.push(key);
                        }
                    }

                    // Old bucket key from full_document_before_change
                    if let Some(key) = extract_bucket(event.full_document_before_change.as_ref()) {
                        if seen.insert(key.clone()) {
                            bucket_keys.push(key);
                        }
                    }
                }
                _ => break,
            }
        }

        Ok(ChangeBatch {
            bucket_keys,
            next_cursor,
            ack_handle: AckHandle { ids: vec![] },
        })
    }

    async fn ack(&self, _batch: &ChangeBatch) -> Result<()> {
        // Mongo resume tokens are durable and persisted in source_cursors on analytics PG
        Ok(())
    }
}

pub struct MongoSnapshot {
    config: SourceConfig,
    collection: Collection<Document>,
}

#[async_trait]
impl Snapshot for MongoSnapshot {
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate> {
        let tenant_field = self.config.mapping.tenant_id_field.as_str();
        let status_field = self.config.mapping.status_field.as_str();
        let amount_field = self.config.mapping.amount_field.as_str();
        let product_field = self.config.mapping.product_id_field.as_str();
        let category_field = self.config.mapping.category_field.as_str();
        let created_field = self.config.mapping.created_at_field.as_str();

        let start_ms = key.hour_start_utc.timestamp_millis();
        let end_ms = (key.hour_start_utc + Duration::hours(1)).timestamp_millis();

        let pipeline = vec![
            doc! {
                "$match": {
                    tenant_field: &key.tenant_id,
                    created_field: {
                        "$gte": BsonDateTime::from_millis(start_ms),
                        "$lt": BsonDateTime::from_millis(end_ms)
                    }
                }
            },
            doc! {
                "$facet": {
                    "statuses": [
                        {
                            "$group": {
                                "_id": format!("${}", status_field),
                                "order_count": { "$sum": 1 },
                                "total_amount": { "$sum": format!("${}", amount_field) }
                            }
                        }
                    ],
                    "products": [
                        {
                            "$group": {
                                "_id": {
                                    "product_id": format!("${}", product_field),
                                    "category": format!("${}", category_field)
                                },
                                "units": { "$sum": 1 },
                                "sales_units": { "$sum": format!("${}", amount_field) }
                            }
                        }
                    ]
                }
            },
        ];

        let mut cursor = self.collection.aggregate(pipeline).await?;
        let mut status_aggregates = Vec::new();
        let mut product_aggregates = Vec::new();

        if let Some(doc) = cursor.next().await {
            let doc = doc?;
            if let Ok(statuses) = doc.get_array("statuses") {
                for s in statuses {
                    if let Some(s_doc) = s.as_document() {
                        let status = s_doc.get_str("_id").unwrap_or("unknown").to_string();
                        let order_count = s_doc.get_i64("order_count").or_else(|_| s_doc.get_i32("order_count").map(|i| i as i64)).unwrap_or(0);
                        let total_amount = s_doc.get_i64("total_amount").or_else(|_| s_doc.get_i32("total_amount").map(|i| i as i64)).unwrap_or(0);
                        status_aggregates.push(StatusAggregate {
                            status,
                            order_count,
                            sales_units: total_amount,
                        });
                    }
                }
            }

            if let Ok(products) = doc.get_array("products") {
                for p in products {
                    if let Some(p_doc) = p.as_document() {
                        let id_obj = p_doc.get_document("_id").ok();
                        let product_id = id_obj.and_then(|d| d.get_str("product_id").ok()).unwrap_or("unknown").to_string();
                        let category = id_obj.and_then(|d| d.get_str("category").ok()).unwrap_or("general").to_string();
                        let units = p_doc.get_i64("units").or_else(|_| p_doc.get_i32("units").map(|i| i as i64)).unwrap_or(0);
                        let sales_units = p_doc.get_i64("sales_units").or_else(|_| p_doc.get_i32("sales_units").map(|i| i as i64)).unwrap_or(0);

                        product_aggregates.push(ProductAggregate {
                            product_id,
                            category,
                            units,
                            sales_units,
                        });
                    }
                }
            }
        }

        Ok(BucketAggregate {
            key: key.clone(),
            status_aggregates,
            product_aggregates,
        })
    }
}

#[async_trait]
impl BucketReader for MongoAdapter {
    async fn compute(&self, key: &BucketKey) -> Result<BucketAggregate> {
        let snapshot = self.begin_snapshot().await?;
        snapshot.compute(key).await
    }

    async fn begin_snapshot(&self) -> Result<Box<dyn Snapshot>> {
        Ok(Box::new(MongoSnapshot {
            config: self.config.clone(),
            collection: self.collection(),
        }))
    }
}
