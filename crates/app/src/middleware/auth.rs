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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
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
