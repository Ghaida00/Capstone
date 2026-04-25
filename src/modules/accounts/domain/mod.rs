//! Pure domain layer for `accounts`.
//!
//! No `sqlx`, no `redis`, no `axum` imports — this directory is
//! readable front-to-back as "what accounts means as a business
//! concept" without ever seeing a SQL query.
//!
//! The module is small so the `mod.rs` holds everything directly
//! (entity + repository trait). Split into `entity.rs`,
//! `repository.rs`, etc. once the file crosses ~200 lines.

use async_trait::async_trait;

use super::ports::{AccountId, AccountStatus, Balance};

// ─── Domain entity ───────────────────────────────────────────

/// The domain's own view of an account, richer than the DTO in
/// `ports.rs` (carries internal-only fields like `full_name`
/// and `email` that are not exposed through the current ports).
///
/// Intentionally private inside the module. Leaking this to
/// another module is a review blocker; convert to the
/// `Balance` port DTO at the infrastructure boundary.
///
/// `full_name` and `email` are populated by the repository but
/// not yet read by any use case — they're kept on the entity
/// so the forthcoming `create_account` / `send_statement` use
/// cases (Phase 2+) do not need to widen the row projection
/// or the repo trait. `#[allow(dead_code)]` is deliberate and
/// scoped to these two fields only.
#[derive(Debug, Clone)]
pub(crate) struct Account {
    pub id: AccountId,
    #[allow(dead_code)]
    pub full_name: String,
    #[allow(dead_code)]
    pub email: Option<String>,
    pub amount_str: String,
    pub currency: String,
    pub status: AccountStatus,
}

impl Account {
    /// Convert to the port-facing DTO. The conversion is
    /// lossy on purpose — `full_name` / `email` are not
    /// exposed to other modules today. Widening the DTO is a
    /// deliberate API evolution, not an accident.
    pub(crate) fn to_balance(&self) -> Balance {
        Balance {
            account_id: self.id.clone(),
            amount_str: self.amount_str.clone(),
            currency: self.currency.clone(),
            status: self.status,
        }
    }
}

// ─── Domain-local errors ────────────────────────────────────

/// The domain's error enum. The port's `AccountError` is a
/// superset — every variant here maps cleanly onto one there,
/// with the infrastructure layer supplying `Infra(String)`.
///
/// `#[allow(dead_code)]` because Phase 1 only exercises the
/// happy-path read; the variants are referenced by the
/// `From<DomainError> for AccountError` impl in the
/// application layer, which future use cases will hit.
#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum DomainError {
    #[error("account not found: {0}")]
    NotFound(String),

    #[error("invalid account identifier: {0}")]
    Validation(String),
}

// ─── Repository abstraction ─────────────────────────────────

/// The port the infrastructure must satisfy. Declared inside
/// `domain/` and implemented in `infrastructure/` — classic
/// dependency inversion keeps `domain/` free of `sqlx`.
///
/// The return type is `Result<Option<Account>, String>` where
/// the `String` carries an infrastructure-level error message.
/// We keep it opaque here (no sqlx::Error leak) because the
/// application layer maps it into the port's
/// `AccountError::Infra(...)` variant.
#[async_trait]
pub(crate) trait AccountRepository: Send + Sync + 'static {
    async fn find_active_by_id(
        &self,
        id: &AccountId,
    ) -> Result<Option<Account>, String>;
}
