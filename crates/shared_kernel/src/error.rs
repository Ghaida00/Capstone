use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

/// Application-wide error type.
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Redis pool error: {0}")]
    RedisPool(#[from] deadpool_redis::PoolError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    /// HTTP 409 — resource already exists (duplicate
    /// `account_number` or `email` on `POST /api/v2/accounts`,
    /// or any future endpoint that creates a uniquely-keyed
    /// resource). Distinct from `BadRequest` so callers can
    /// distinguish "your input is malformed" from "your input
    /// is valid but the resource already exists".
    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Rate limited")]
    RateLimited,

    #[error("Circuit breaker open")]
    CircuitBreakerOpen,

    #[error("Service overloaded")]
    ServiceOverloaded,

    /// A-2: Redis Sentinel resolution failure (all sentinels
    /// unreachable / malformed reply / timeout). Consistent with
    /// every other shared_kernel infra error being an `AppError`
    /// variant — was `anyhow::Result` in two private helpers,
    /// migrated for consistency.
    #[error("Sentinel error: {0}")]
    SentinelError(String),

    /// R-7: a specific upstream dependency's per-dependency
    /// breaker is open. Distinct from `CircuitBreakerOpen` (the
    /// coarse HTTP-edge breaker) — this names WHICH dependency
    /// (`db` | `redis` | `rabbitmq`) so one unhealthy dependency
    /// fails fast without the global edge breaker rejecting every
    /// route. Maps to 503 + `Retry-After`.
    #[error("Dependency unavailable: {name}")]
    DependencyDown { name: &'static str },

    #[error("Internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match &self {
            AppError::Database(e) => {
                tracing::error!(error = %e, "Database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database_error",
                    "An internal database error occurred".to_string(),
                )
            }
            AppError::Redis(e) => {
                tracing::error!(error = %e, "Redis error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "cache_error",
                    "An internal cache error occurred".to_string(),
                )
            }
            AppError::RedisPool(e) => {
                tracing::error!(error = %e, "Redis pool error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "cache_error",
                    "Cache pool exhausted".to_string(),
                )
            }
            AppError::Serialization(e) => {
                tracing::error!(error = %e, "Serialization error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "serialization_error",
                    "Data serialization error".to_string(),
                )
            }
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg.clone()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "bad_request", msg.clone()),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, "conflict", msg.clone()),
            AppError::RateLimited => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "Too many requests, please try again later".to_string(),
            ),
            AppError::CircuitBreakerOpen => (
                StatusCode::SERVICE_UNAVAILABLE,
                "circuit_breaker_open",
                "Service temporarily unavailable, please try again later".to_string(),
            ),
            AppError::ServiceOverloaded => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_overloaded",
                "Service is currently overloaded, please try again later".to_string(),
            ),
            AppError::DependencyDown { name } => {
                tracing::warn!(dependency = name, "dependency breaker open — failing fast");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "dependency_down",
                    format!("Dependency '{name}' is temporarily unavailable, please retry"),
                )
            }
            AppError::SentinelError(msg) => {
                tracing::error!(error = %msg, "Sentinel error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "sentinel_error",
                    "Redis Sentinel resolution failed".to_string(),
                )
            }
            AppError::Internal(msg) => {
                tracing::error!(error = %msg, "Internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "An internal error occurred".to_string(),
                )
            }
        };

        let body = json!({
            "error": error_type,
            "message": message,
        });

        let mut resp = (status, Json(body)).into_response();
        // R-7: tell the caller it is worth retrying a downed
        // dependency shortly (the breaker half-opens on a timer).
        if matches!(self, AppError::DependencyDown { .. }) {
            resp.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("3"),
            );
        }
        resp
    }
}

pub type AppResult<T> = Result<T, AppError>;
