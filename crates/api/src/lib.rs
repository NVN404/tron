use axum::{
    http::{header, HeaderValue},
    middleware::{self, Next},
    response::{Html, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;
use sqlx::PgPool;
use tower_http::cors::CorsLayer;

pub mod auth;
pub mod routes;
pub mod self_check;

pub use auth::{hash_api_key, AuthenticatedTenant};

/// Anti-leak cache defense (INC-02 prevention):
/// Strictly forces every downstream client, CDN, and intermediate proxy to never store or cache responses.
pub async fn zero_cache_middleware(req: axum::extract::Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    let headers = res.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate, proxy-revalidate"),
    );
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(header::EXPIRES, HeaderValue::from_static("0"));
    res
}

async fn health_check() -> Json<serde_json::Value> {
    Json(json!({ "status": "healthy", "service": "analytics-api" }))
}

async fn serve_dashboard() -> Html<&'static str> {
    Html(include_str!("../static/dashboard.html"))
}

pub fn create_app(pool: PgPool) -> Router {
    let api_routes = Router::new()
        .route("/tenants/:id/orders/count", get(routes::get_order_count))
        .route("/tenants/:id/orders/trend", get(routes::get_order_trend))
        .route("/tenants/:id/products/top", get(routes::get_top_products))
        .route("/tenants/:id/orders/breakdown", get(routes::get_order_breakdown))
        .layer(middleware::from_fn_with_state(pool.clone(), auth::auth_middleware));

    let admin_routes = Router::new()
        .route("/admin/tenants/:id/quarantine", axum::routing::post(routes::set_quarantine))
        .route("/admin/tenants/:id/unquarantine", axum::routing::post(routes::remove_quarantine));

    Router::new()
        .route("/", get(serve_dashboard))
        .route("/dashboard", get(serve_dashboard))
        .route("/health", get(health_check))
        .route("/metrics", get(routes::get_metrics))
        .merge(api_routes)
        .merge(admin_routes)
        .layer(middleware::from_fn(zero_cache_middleware))
        .layer(CorsLayer::permissive())
        .with_state(pool)
}
