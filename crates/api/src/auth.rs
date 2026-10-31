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

    // Skip auth for health and metrics
    if path == "/health" || path == "/metrics" || path == "/status" {
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

    // Look up key in database
    let row: Option<(Uuid, String)> = sqlx::query_as(
        r#"
        SELECT k.tenant_id, t.health
        FROM api_keys k
        JOIN tenants t ON t.id = k.tenant_id
        WHERE k.key_hash = $1 AND k.revoked = FALSE
        "#,
    )
    .bind(key_hash)
    .fetch_optional(&pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Auth database error: {}", e) })),
        )
            .into_response()
    })?;

    let (tenant_id, health) = row.ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid or revoked API key" })),
        )
            .into_response()
    })?;

    // Check quarantine status
    if health == "quarantined" {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "Tenant is quarantined for audit" })),
        )
            .into_response());
    }

    // Check path parameter matching: /tenants/:id/...
    // If the path contains a tenant ID that does NOT match the key's tenant ID,
    // return 404 NOT FOUND (never 403, preventing enumeration).
    if let Some(pos) = path.find("/tenants/") {
        let sub = &path[pos + 9..];
        let id_str = sub.split('/').next().unwrap_or("");
        if let Ok(path_tenant_id) = Uuid::parse_str(id_str) {
            if path_tenant_id != tenant_id {
                tracing::warn!("Tenant mismatch: path={} vs auth={}", path_tenant_id, tenant_id);
                return Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": "Resource not found" })),
                )
                    .into_response());
            }
        }
    }

    let mut req = req;
    req.extensions_mut().insert(AuthenticatedTenant {
        tenant_id,
        health,
    });

    Ok(next.run(req).await)
}
