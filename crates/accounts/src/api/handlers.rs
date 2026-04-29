//! HTTP handlers for `/api/v2/accounts/*`.
//!
//! Each handler is trivially short by design: extract → port
//! call → map result. Any conditional work belongs behind the
//! port, not here.
//!
//! Error mapping to HTTP goes through `From<AccountError> for
//! AppError` (defined below) so this module plugs into the
//! existing `AppResult<Json<...>>` style without introducing a
//! second error framework.

use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use shared_kernel::error::{AppError, AppResult};
use shared_kernel::responses::ApiResponse;

use super::super::infrastructure::AccountsDeps;
use super::super::ports::{AccountError, AccountId, Balance};

// ─── Validation constants (match the legacy handler) ────────

/// Matches the DB VARCHAR(50) on `users.account_number`. We
/// reject anything longer at the HTTP boundary so a user-sent
/// blob of 10 MB never reaches the DB.
const MAX_ACCOUNT_LEN: usize = 50;

// ─── Response DTO ──────────────────────────────────────────

/// Wire shape returned by the v2 balance endpoint. The structure
/// is preserved verbatim from the pre-cull v1 response so cached
/// entries and downstream clients see no diff across the cull.
///
/// `Deserialize` is required so the Redis cache helper can
/// decode a previously-cached value via
/// `cache.get::<BalanceResponse>(...)`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BalanceResponse {
    pub account_number: String,
    pub balance: String,
    pub currency: String,
    pub status: String,
}

impl From<Balance> for BalanceResponse {
    fn from(b: Balance) -> Self {
        Self {
            account_number: b.account_id.0,
            balance: b.amount_str,
            currency: b.currency,
            status: b.status.as_str().to_owned(),
        }
    }
}

// ─── Error mapping ─────────────────────────────────────────

impl From<AccountError> for AppError {
    fn from(err: AccountError) -> AppError {
        match err {
            AccountError::NotFound(msg) => AppError::NotFound(format!("Account {} not found", msg)),
            AccountError::Validation(msg) => AppError::BadRequest(msg),
            AccountError::Infra(msg) => AppError::Internal(msg),
        }
    }
}

// ─── Handler ───────────────────────────────────────────────

/// GET /api/v2/accounts/:account_number/balance
///
/// Reads through the `accounts` module's service trait so the
/// business logic stays swappable without touching the HTTP
/// layer. Same JSON response shape and Redis cache key as the
/// pre-cull v1 endpoint, so cached entries written before the
/// v1 cull continue to serve until natural TTL expiry.
pub(crate) async fn get_balance(
    State(deps): State<AccountsDeps>,
    Path(account_number): Path<String>,
) -> AppResult<Json<ApiResponse<BalanceResponse>>> {
    // HTTP-shape validation (length, charset). Domain-level
    // validation ("empty id") lives in the application layer so
    // it is testable without a web framework.
    validate_account_number(&account_number)?;

    // Reuse the existing Redis cache key so both v1 and v2 see
    // each other's cached entries — smooth cutover when the
    // legacy endpoint is retired.
    let cache_key = format!(
        "{}:balance:{}",
        shared_kernel::cache::redis::CACHE_KEY_VERSION,
        account_number
    );

    if let Some(cached) = deps.cache.get::<BalanceResponse>(&cache_key).await? {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }
    metrics::counter!("cache_misses_total").increment(1);

    let id = AccountId(account_number);
    let balance = deps.service.get_balance(&id).await?;
    let response: BalanceResponse = balance.into();

    // 30 s TTL mirrors the legacy handler. Cache-set failures
    // are swallowed because a missing cache is not a client-
    // facing error.
    let _ = deps.cache.set(&cache_key, &response, 30).await;

    Ok(Json(ApiResponse::success(response)))
}

fn validate_account_number(s: &str) -> AppResult<()> {
    if s.is_empty() {
        return Err(AppError::BadRequest(
            "account_number must not be empty".into(),
        ));
    }
    if s.len() > MAX_ACCOUNT_LEN {
        return Err(AppError::BadRequest(format!(
            "account_number must be at most {} characters",
            MAX_ACCOUNT_LEN
        )));
    }
    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(AppError::BadRequest(
            "account_number contains invalid characters (alphanumeric, hyphens, underscores, dots only)".into(),
        ));
    }
    Ok(())
}
