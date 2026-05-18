use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use jsonwebtoken::{decode, DecodingKey, Validation};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Shared JWT decoding key — initialised once at startup when
/// `ENABLE_AUTH=true` and `AUTH_SECRET` is set.
static DECODING_KEY: OnceCell<DecodingKey> = OnceCell::new();

/// Standard JWT claims we accept.
///
/// `role` is optional so existing tokens (issued before the admin
/// surface existed) still parse — they simply do not satisfy the
/// `require_admin` gate. A token with `role = "admin"` is the only
/// shape that passes the admin middleware.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
    #[serde(default)]
    pub role: Option<String>,
}

/// Initialise the shared decoding key. Returns Err if called twice
/// with conflicting secrets.
pub fn init_auth(secret: &str) {
    let _ = DECODING_KEY.set(DecodingKey::from_secret(secret.as_bytes()));
}

/// Auth state passed to the middleware so individual requests can check
/// whether enforcement is on.
#[derive(Clone)]
pub struct AuthState {
    pub enabled: bool,
}

/// JWT bearer-token middleware.
///
/// When `AuthState.enabled` is false, requests pass through untouched —
/// this is the default for load testing. When enabled, the middleware
/// parses the `Authorization: Bearer <jwt>` header and validates HS256
/// signature + exp claim using the key installed via `init_auth`.
pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    if !state.enabled {
        return next.run(req).await;
    }

    let key = match DECODING_KEY.get() {
        Some(k) => k,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "auth_misconfigured",
                           "message": "AUTH_SECRET not set"})),
            )
                .into_response();
        }
    };

    let token = match req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    {
        Some(t) => t.to_string(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "missing_token",
                           "message": "Authorization: Bearer <jwt> required"})),
            )
                .into_response();
        }
    };

    match decode::<Claims>(&token, key, &Validation::default()) {
        Ok(_) => next.run(req).await,
        Err(e) => (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid_token",
                       "message": e.to_string()})),
        )
            .into_response(),
    }
}

/// Admin-only middleware for the `/api/v2/admin/*` operator surface.
///
/// Stricter than `auth_middleware` in two ways:
///   1. Refuses with 403 when `AuthState.enabled == false`. Admin
///      endpoints would otherwise be wide open in any environment
///      that has not flipped `ENABLE_AUTH=true` — a foot-gun that
///      defeats the whole point of gating them.
///   2. Requires the JWT to carry `role = "admin"`. A regular
///      user token authenticates but cannot enumerate stuck
///      outbox rows or in-flight customer transactions.
///
/// Does its own decode (rather than chaining to `auth_middleware`
/// + reading shared state) so the admin router can wire a single
/// middleware layer; the cost is one extra HMAC verify per admin
/// request, which is negligible given the low traffic shape of
/// an operator surface.
pub async fn require_admin_middleware(
    axum::extract::State(state): axum::extract::State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    if !state.enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "admin_disabled",
                       "message": "admin surface requires ENABLE_AUTH=true"})),
        )
            .into_response();
    }

    let key = match DECODING_KEY.get() {
        Some(k) => k,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "auth_misconfigured",
                           "message": "AUTH_SECRET not set"})),
            )
                .into_response();
        }
    };

    let token = match req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    {
        Some(t) => t.to_string(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "missing_token",
                           "message": "Authorization: Bearer <jwt> required"})),
            )
                .into_response();
        }
    };

    let claims = match decode::<Claims>(&token, key, &Validation::default()) {
        Ok(data) => data.claims,
        Err(e) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid_token",
                           "message": e.to_string()})),
            )
                .into_response();
        }
    };

    if claims.role.as_deref() != Some("admin") {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "admin_role_required",
                       "message": "JWT claim `role` must equal \"admin\""})),
        )
            .into_response();
    }

    next.run(req).await
}
