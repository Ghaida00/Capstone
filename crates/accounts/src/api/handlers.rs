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
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use shared_kernel::error::{AppError, AppResult};
use shared_kernel::responses::ApiResponse;

use super::super::application::BALANCE_CACHE_TTL_SECS;
use super::super::infrastructure::AccountsDeps;
use super::super::ports::{AccountCreated, AccountError, AccountId, Balance, CreateAccountInput};

// ─── Validation constants (match the legacy handler) ────────

/// Matches the DB VARCHAR(50) on `users.account_number`. We
/// reject anything longer at the HTTP boundary so a user-sent
/// blob of 10 MB never reaches the DB.
const MAX_ACCOUNT_LEN: usize = 50;

// ─── Response DTO ──────────────────────────────────────────

/// Wire shape returned by `GET /api/v2/accounts/:id/balance`.
///
/// `Deserialize` lets the Redis cache helper decode a stored
/// value via `cache.get::<BalanceResponse>(...)`. `Clone` lets
/// the moka in-process cache return owned copies on hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Wire shape for `POST /api/v2/accounts` request body.
#[derive(Debug, Deserialize)]
pub(crate) struct CreateAccountRequest {
    /// Optional caller-supplied account number. If absent the
    /// service auto-generates an `ACC_XXXXXXX` number.
    pub account_number: Option<String>,
    pub full_name: String,
    pub email: Option<String>,
    /// Opening balance as a decimal string, e.g. `"500000.00"`.
    /// Defaults to `"0.00"` when absent.
    pub initial_balance: Option<String>,
}

/// Wire shape returned by `POST /api/v2/accounts`.
#[derive(Debug, Serialize)]
pub(crate) struct CreateAccountResponse {
    pub account_number: String,
    pub full_name: String,
    pub email: Option<String>,
    pub balance: String,
    pub currency: String,
    pub status: String,
}

impl From<AccountCreated> for CreateAccountResponse {
    fn from(a: AccountCreated) -> Self {
        Self {
            account_number: a.account_number,
            full_name: a.full_name,
            email: a.email,
            balance: a.balance,
            currency: a.currency,
            status: a.status,
        }
    }
}

// ─── Error mapping ─────────────────────────────────────────

impl From<AccountError> for AppError {
    fn from(err: AccountError) -> AppError {
        match err {
            AccountError::NotFound(msg) => AppError::NotFound(format!("Account {} not found", msg)),
            AccountError::Validation(msg) => AppError::BadRequest(msg),
            AccountError::AlreadyExists(msg) => AppError::Conflict(msg),
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

    // L1: in-process moka, zero TCP on hit.
    if let Some(cached) = deps.moka_balance.get(&account_number).await {
        metrics::counter!("cache_hits_total", "tier" => "moka").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }

    let cache_key = format!(
        "{}:balance:{}",
        shared_kernel::cache::redis::CACHE_KEY_VERSION,
        account_number
    );

    // L2: Redis. On hit, populate L1 so the next request on this
    // replica skips the Redis hop. On error, log + degrade to a
    // DB read instead of returning 500: a transient Redis outage
    // must not take down the balance endpoint when the DB is
    // healthy.
    match deps.cache.get::<BalanceResponse>(&cache_key).await {
        Ok(Some(cached)) => {
            metrics::counter!("cache_hits_total", "tier" => "redis").increment(1);
            deps.moka_balance
                .insert(account_number.clone(), cached.clone())
                .await;
            return Ok(Json(ApiResponse::success(cached)));
        }
        Ok(None) => {
            metrics::counter!("cache_misses_total").increment(1);
        }
        Err(e) => {
            tracing::warn!(error = %e, "Redis cache.get failed; falling through to DB");
            metrics::counter!("cache_errors_total", "op" => "get").increment(1);
        }
    }

    let id = AccountId(account_number.clone());
    let balance = deps.service.get_balance(&id).await?;
    let response: BalanceResponse = balance.into();

    if let Err(e) = deps
        .cache
        .set(&cache_key, &response, BALANCE_CACHE_TTL_SECS)
        .await
    {
        tracing::warn!(error = %e, "Redis cache.set failed; serving DB result");
        metrics::counter!("cache_errors_total", "op" => "set").increment(1);
    }
    deps.moka_balance
        .insert(account_number, response.clone())
        .await;

    Ok(Json(ApiResponse::success(response)))
}

/// POST /api/v2/accounts
///
/// Register a new bank account. HTTP 201 Created on success.
/// HTTP 409 Conflict when `account_number` or `email` is
/// already taken. HTTP 400 on validation errors.
///
/// The handler is intentionally thin — all business logic
/// (account number generation, balance validation, duplicate
/// detection) lives in the `application` layer so it is
/// testable without a web framework.
pub(crate) async fn create_account(
    State(deps): State<AccountsDeps>,
    Json(req): Json<CreateAccountRequest>,
) -> AppResult<(StatusCode, Json<ApiResponse<CreateAccountResponse>>)> {
    let input = CreateAccountInput {
        account_number: req.account_number,
        full_name: req.full_name,
        email: req.email,
        initial_balance: req.initial_balance,
    };

    let created = deps.service.create_account(input).await?;
    metrics::counter!("accounts_created_total").increment(1);

    Ok((
        StatusCode::CREATED,
        Json(ApiResponse::success(created.into())),
    ))
}

// ─── Shared validation ──────────────────────────────────────

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
