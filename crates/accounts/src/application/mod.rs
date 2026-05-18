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

use super::domain::AccountRepository;
use super::ports::{AccountError, AccountId, AccountService, Balance};

/// TTL for both balance-cache write sites — the `:acc:` key written
/// here and the `:balance:` key written by [`super::api::handlers::
/// get_balance`]. Bounded short on purpose: cached entries carry the
/// `status` field, and `cache_invalidator` only fires on
/// `transactions.committed` events. Status flips that do NOT commit
/// a transaction (admin freeze, fraud lock, KYC hold) are bounded by
/// this TTL alone, so an oversized value would make `GET /balance`
/// advertise `status: Active` for the full window after the underlying
/// row was blocked. The debit path stays correct independently — the
/// consumer's `UPDATE … WHERE status = 'active'` rejects stale writes
/// at the row — so this constant only governs the read endpoint's
/// staleness.
pub(crate) const BALANCE_CACHE_TTL_SECS: u64 = 10;

/// Implementation of [`AccountService`] backed by repo + Redis cache.
///
/// `get_balance` reads through the `{v}:acc:{id}` Redis key on the
/// hot path, falling back to a single repo SELECT on miss and
/// writing the freshly loaded `Balance` back with
/// `BALANCE_CACHE_TTL_SECS`. Debit-path correctness does not depend
/// on this cache — the consumer's `UPDATE … WHERE balance >= $1
/// AND status = 'active'` validates against the row at debit time.
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
                let _ = self
                    .cache
                    .set(&cache_key, &bal, BALANCE_CACHE_TTL_SECS)
                    .await;
                Ok(bal)
            }
            Ok(None) => Err(AccountError::NotFound(id.as_str().to_owned())),
            // A-1: RepoError → AccountError::Infra via Display
            Err(e) => Err(AccountError::Infra(e.to_string())),
        }
    }
}

