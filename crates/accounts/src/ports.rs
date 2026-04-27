//! Public contract of the `accounts` module.
//!
//! Other modules (and tests) may import ONLY from this file.
//! Everything below is deliberately infrastructure-free: no
//! `sqlx`, no `redis`, no `axum` types leak through these
//! signatures. The `infrastructure/` layer converts between the
//! port DTOs and whatever the database row / cache entry shape
//! happens to be.
//!
//! See `docs/architecture/module-anatomy.md` §2 (ports.rs
//! section) for the rules this file obeys.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ─── DTOs ────────────────────────────────────────────────────

/// Opaque account identifier as exposed to other modules / the
/// API boundary. Wraps the human-readable account number so we
/// can change the underlying representation later (UUID, hash,
/// etc.) without breaking every call site.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountId(pub String);

impl AccountId {
    /// Expose the underlying string when the infrastructure
    /// layer needs to issue SQL. Kept terse — there is no
    /// inner validation because construction via the api DTO
    /// is already validated.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The lifecycle state of an account. Matches the DB CHECK
/// constraint on `users.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccountStatus {
    Active,
    Inactive,
    Blocked,
}

impl AccountStatus {
    /// Wire-level representation used by the DB and by legacy
    /// JSON responses. Kept lower-case to match the existing
    /// `users.status` values.
    pub fn as_str(&self) -> &'static str {
        match self {
            AccountStatus::Active => "active",
            AccountStatus::Inactive => "inactive",
            AccountStatus::Blocked => "blocked",
        }
    }
}

/// The value returned by a balance lookup.
///
/// `amount_str` is pre-serialised to string form because the
/// underlying column is `DECIMAL(18,2)` and JSON numbers do not
/// round-trip arbitrary precision; callers that need maths
/// should parse this themselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub account_id: AccountId,
    pub amount_str: String,
    pub currency: String,
    pub status: AccountStatus,
}

// ─── Errors ──────────────────────────────────────────────────

/// Every service call returns this. Infrastructure-level
/// failures (pool exhaustion, Redis timeouts, …) are flattened
/// into `Infra(String)` so other modules do not need to import
/// sqlx / redis types.
///
/// When mapping to HTTP the api layer uses `From<AccountError>
/// for AppError` — see `api::handlers`.
#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("account not found: {0}")]
    NotFound(String),

    #[error("invalid account identifier: {0}")]
    Validation(String),

    #[error("infrastructure: {0}")]
    Infra(String),
}

// ─── Service trait ──────────────────────────────────────────

/// The public surface of this module.
///
/// Other modules inject `Arc<dyn AccountService>` (aliased as
/// [`DynAccountService`] for ergonomics) and never import any
/// concrete type from this crate's `infrastructure/` directory.
#[async_trait]
pub trait AccountService: Send + Sync + 'static {
    /// Read the current balance for an account. Returns
    /// [`AccountError::NotFound`] if no active account with that
    /// id exists.
    async fn get_balance(&self, id: &AccountId) -> Result<Balance, AccountError>;
}

/// Convenience alias — prefer this in constructor signatures.
pub type DynAccountService = Arc<dyn AccountService>;
