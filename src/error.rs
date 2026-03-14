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

    #[error("Rate limited")]
    RateLimited,

    #[error("Circuit breaker open")]
    CircuitBreakerOpen,

    #[error("Service overloaded")]
    ServiceOverloaded,

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

        (status, Json(body)).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
