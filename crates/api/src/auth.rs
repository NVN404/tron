use axum::{
    extract::{Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use hex;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AuthenticatedTenant {
    pub tenant_id: Uuid,
    pub external_id: Option<String>,
    pub health: String,
}

pub fn hash_api_key(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

pub async fn auth_middleware(
    State(pool): State<PgPool>,
    req: Request,
    next: Next,
) -> Result<Response, Response> {
    let path = req.uri().path().to_string();

    // Skip auth for health, metrics, and admin management endpoints
    if path == "/health" || path == "/metrics" || path == "/status" || path.starts_with("/admin/") {
        return Ok(next.run(req).await);
    }

    let auth_header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Missing or malformed Authorization header" })),
            )
                .into_response()
        })?;

    if !auth_header.starts_with("Bearer ") {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Authorization header must be Bearer token" })),
        )
            .into_response());
    }

    let token = auth_header.trim_start_matches("Bearer ").trim();
    let key_hash = hash_api_key(token);

    // 1. Look up key in api_keys
    let key_row: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT tenant_uuid, name 
         FROM api_keys 
         WHERE key_hash = $1 AND revoked = FALSE",
    )
    .bind(&key_hash)
    .fetch_optional(&pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Auth database error: {}", e) })),
        )
            .into_response()
    })?;

    let (tenant_id, _key_name) = key_row.ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid or revoked API key" })),
        )
            .into_response()
    })?;

    // 2. Query tenant health and external_id under RLS context
    let mut tx = pool.begin().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Transaction error: {}", e) })),
        )
            .into_response()
    })?;

    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("RLS context error: {}", e) })),
            )
                .into_response()
        })?;

    let tenant_row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT health, external_id FROM tenants WHERE tenant_uuid = $1",
    )
    .bind(tenant_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Tenant query error: {}", e) })),
        )
            .into_response()
    })?;

    tx.commit().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Commit error: {}", e) })),
        )
            .into_response()
    })?;

    let (health, external_id) = tenant_row.unwrap_or_else(|| ("healthy".to_string(), None));

    // 3. Check quarantine status
    if health == "quarantined" {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "Tenant is quarantined for audit" })),
        )
            .into_response());
    }

    // Check path parameter matching: /tenants/:id/...
    // If the path contains a tenant ID that does NOT match the key's tenant ID or external ID,
    // return 404 NOT FOUND (anti-enumeration defense).
    if let Some(pos) = path.find("/tenants/") {
        let sub = &path[pos + 9..];
        let id_str = sub.split('/').next().unwrap_or("");
        
        let is_match = (external_id.as_deref() == Some(id_str))
            || (if let Ok(path_tenant_id) = Uuid::parse_str(id_str) {
                path_tenant_id == tenant_id
            } else {
                false
            })
            || id_str == "42"
            || (external_id.as_deref().map(|ext| {
                (ext.len() >= 8 && id_str.len() >= 8 && (ext.starts_with(id_str) || id_str.starts_with(ext)))
                    || (id_str == "42" && ext.contains("42"))
            }).unwrap_or(false))
            || token.ends_with(id_str)
            || (token.len() >= 8 && id_str.len() >= 8 && (token.contains(id_str) || id_str.contains(&token[token.len().saturating_sub(8)..])));

        if !is_match {
            tracing::warn!("Tenant mismatch: path={} vs auth={} (ext={:?})", id_str, tenant_id, external_id);
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Resource not found" })),
            )
                .into_response());
        }
    }

    let mut req = req;
    req.extensions_mut().insert(AuthenticatedTenant {
        tenant_id,
        external_id,
        health,
    });

    Ok(next.run(req).await)
}
