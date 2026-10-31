//! Direct-SQL oracle engine: calculates ground-truth analytics directly against the source database.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountAndTotalResult {
    pub tenant_id: Uuid,
    pub order_count: i64,
    pub total_sales_paise: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendResult {
    pub bucket: String,
    pub order_count: i64,
    pub total_sales_paise: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopProductResult {
    pub product_id: Uuid,
    pub category: String,
    pub units: i64,
    pub total_sales_paise: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakdownResult {
    pub category: String,
    pub status: String,
    pub order_count: i64,
    pub total_sales_paise: i64,
}
