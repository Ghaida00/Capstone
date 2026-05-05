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

/// The domain's own view of an account.
///
/// Intentionally private inside the module. Leaking this to
/// another module is a review blocker; convert to the
/// `Balance` port DTO at the infrastructure boundary.
#[derive(Debug, Clone)]
pub(crate) struct Account {
    pub id: AccountId,
    pub amount_str: String,
    pub currency: String,
    pub status: AccountStatus,
}

impl Account {
    pub(crate) fn to_balance(&self) -> Balance {
        Balance {
            account_id: self.id.clone(),
            amount_str: self.amount_str.clone(),
            currency: self.currency.clone(),
            status: self.status,
        }
    }
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
    async fn find_active_by_id(&self, id: &AccountId) -> Result<Option<Account>, String>;
}
