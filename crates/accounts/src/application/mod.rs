//! Use-case orchestration for `accounts`.
//!
//! Each struct here is one use case. Dependencies come in via
//! the constructor — no globals, no `OnceCell` lookups.
//!
//! For Phase 1 the only use case is `GetBalance`. When `credit`,
//! `debit`, `create_account`, and `set_status` follow, give each
//! its own file (`credit.rs`, `debit.rs`, …) and keep this
//! `mod.rs` as the re-export index.

use std::sync::Arc;

use async_trait::async_trait;

use super::domain::{AccountRepository, DomainError};
use super::ports::{AccountError, AccountId, AccountService, Balance};

/// Implementation of [`AccountService`] that delegates all reads
/// through an injected [`AccountRepository`]. Holds no state of
/// its own beyond the repo handle; a single instance is shared
/// across every HTTP request via `Arc<dyn AccountService>`.
pub(crate) struct GetBalanceService {
    repo: Arc<dyn AccountRepository>,
}

impl GetBalanceService {
    pub(crate) fn new(repo: Arc<dyn AccountRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl AccountService for GetBalanceService {
    async fn get_balance(&self, id: &AccountId) -> Result<Balance, AccountError> {
        // Surface-level validation that the DOMAIN cares about.
        // HTTP-shape validation (length, charset) happens in the
        // api layer so this stays I/O-free and testable without
        // a web framework.
        if id.as_str().is_empty() {
            return Err(AccountError::Validation(
                "account id must not be empty".into(),
            ));
        }

        match self.repo.find_active_by_id(id).await {
            Ok(Some(account)) => Ok(account.to_balance()),
            Ok(None) => Err(AccountError::NotFound(id.as_str().to_owned())),
            Err(msg) => Err(AccountError::Infra(msg)),
        }
    }
}

// `DomainError` → `AccountError` mapping. Kept here rather than
// in `ports.rs` because the domain error type is module-private
// and must never appear in a port signature.
impl From<DomainError> for AccountError {
    fn from(err: DomainError) -> Self {
        match err {
            DomainError::NotFound(msg) => AccountError::NotFound(msg),
            DomainError::Validation(msg) => AccountError::Validation(msg),
        }
    }
}
