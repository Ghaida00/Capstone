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

/// Minimal projection used by `insert_account` returning clause.
#[derive(Debug, Clone)]
pub(crate) struct NewAccount {
    pub account_number: String,
    pub full_name: String,
    pub email: Option<String>,
    pub balance_str: String,
}

// ─── Repository abstraction ─────────────────────────────────

/// The port the infrastructure must satisfy. Declared inside
/// `domain/` and implemented in `infrastructure/` — classic
/// dependency inversion keeps `domain/` free of `sqlx`.
///
/// A-1: returns `Result<_, RepoError>` so the application layer
/// can pattern-match retryable / non-retryable / observable
/// failure classes instead of parsing a stringified message.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RepoError {
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("{0}")]
    Other(String),
}

#[async_trait]
pub(crate) trait AccountRepository: Send + Sync + 'static {
    async fn find_active_by_id(&self, id: &AccountId) -> Result<Option<Account>, RepoError>;

    /// INSERT a new row into `users`. Returns the created row on
    /// success. The caller is responsible for validating
    /// `account_number` uniqueness at the domain level; the repo
    /// maps UNIQUE violations to `RepoError::Sqlx` with the
    /// underlying `sqlx::Error::Database` variant — the
    /// application layer then converts that to
    /// `AccountError::AlreadyExists`.
    async fn insert_account(&self, account: NewAccount) -> Result<NewAccount, RepoError>;
}
