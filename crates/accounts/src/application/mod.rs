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

use shared_kernel::cache::redis::RedisCache;

use super::domain::{AccountRepository, DomainError};
use super::ports::{AccountError, AccountId, AccountService, Balance};

/// TTL for the existence/balance cache. Transactions only check that
/// the `from_account` exists and is active — staleness on the balance
/// itself is irrelevant because the consumer re-checks balance under
/// row-level UPDATE before debiting.
const ACCOUNT_CACHE_TTL_SECS: u64 = 60;

/// Implementation of [`AccountService`] backed by repo + Redis cache.
///
/// Previously every transaction-create issued a synchronous DB SELECT
/// against `users` to verify the from-account existed. Under load that
/// roundtrip dominated p95 (~10–25 ms even on a cache-warm box). We now
/// read-through Redis with a 60 s TTL — verification stays correct
/// (consumer re-validates balance with `UPDATE … WHERE balance >= $1`)
/// while the hot path drops to a single Redis GET on warm keys.
pub(crate) struct GetBalanceService {
    repo: Arc<dyn AccountRepository>,
    cache: RedisCache,
}

impl GetBalanceService {
    pub(crate) fn new(repo: Arc<dyn AccountRepository>, cache: RedisCache) -> Self {
        Self { repo, cache }
    }
}

#[async_trait]
impl AccountService for GetBalanceService {
    async fn get_balance(&self, id: &AccountId) -> Result<Balance, AccountError> {
        if id.as_str().is_empty() {
            return Err(AccountError::Validation(
                "account id must not be empty".into(),
            ));
        }

        let cache_key = format!(
            "{}:acc:{}",
            shared_kernel::cache::redis::CACHE_KEY_VERSION,
            id.as_str()
        );
        if let Ok(Some(cached)) = self.cache.get::<Balance>(&cache_key).await {
            return Ok(cached);
        }

        match self.repo.find_active_by_id(id).await {
            Ok(Some(account)) => {
                let bal = account.to_balance();
                let _ = self.cache.set(&cache_key, &bal, ACCOUNT_CACHE_TTL_SECS).await;
                Ok(bal)
            }
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
