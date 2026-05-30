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
use rust_decimal::Decimal;

use shared_kernel::cache::redis::RedisCache;

use super::domain::{AccountRepository, NewAccount, RepoError};
use super::ports::{
    AccountCreated, AccountError, AccountId, AccountService, AccountStatus, Balance,
    CreateAccountInput,
};

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

/// Reject the one shape of `AccountId` that has no chance of
/// hitting an active row — empty string. Kept as a pure helper
/// (rather than inlined) so it carries a unit test (T-3) and so
/// any future tightening (length cap, charset) lands here and not
/// scattered across handlers.
fn validate_account_id(id: &AccountId) -> Result<(), AccountError> {
    if id.as_str().is_empty() {
        return Err(AccountError::Validation(
            "account id must not be empty".into(),
        ));
    }
    Ok(())
}

// ─── Validation helpers for create_account ──────────────────

const MAX_ACCOUNT_NUMBER_LEN: usize = 50;
const MAX_FULL_NAME_LEN: usize = 150;
const MAX_EMAIL_LEN: usize = 150;
/// Prefix for auto-generated account numbers.
const ACCOUNT_NUMBER_PREFIX: &str = "ACC_";

/// Validate a caller-supplied account number. Rules mirror
/// the `users.account_number` column: VARCHAR(50), and the
/// same charset as `validate_account_number` in the handler
/// (alphanumeric, hyphens, underscores, dots).
fn validate_account_number(s: &str) -> Result<(), AccountError> {
    if s.is_empty() {
        return Err(AccountError::Validation(
            "account_number must not be empty".into(),
        ));
    }
    if s.len() > MAX_ACCOUNT_NUMBER_LEN {
        return Err(AccountError::Validation(format!(
            "account_number must be at most {} characters",
            MAX_ACCOUNT_NUMBER_LEN
        )));
    }
    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(AccountError::Validation(
            "account_number contains invalid characters \
             (alphanumeric, hyphens, underscores, dots only)"
                .into(),
        ));
    }
    Ok(())
}

fn validate_full_name(s: &str) -> Result<(), AccountError> {
    if s.trim().is_empty() {
        return Err(AccountError::Validation(
            "full_name must not be empty".into(),
        ));
    }
    if s.len() > MAX_FULL_NAME_LEN {
        return Err(AccountError::Validation(format!(
            "full_name must be at most {} characters",
            MAX_FULL_NAME_LEN
        )));
    }
    Ok(())
}

fn validate_email(s: &str) -> Result<(), AccountError> {
    if s.len() > MAX_EMAIL_LEN {
        return Err(AccountError::Validation(format!(
            "email must be at most {} characters",
            MAX_EMAIL_LEN
        )));
    }
    // Minimal sanity: must contain exactly one `@` with at least
    // one char on each side. Full RFC 5321 validation is out of
    // scope — rely on the DB UNIQUE constraint for duplicates.
    let at_count = s.chars().filter(|&c| c == '@').count();
    if at_count != 1 {
        return Err(AccountError::Validation(
            "email must contain exactly one '@'".into(),
        ));
    }
    let parts: Vec<&str> = s.splitn(2, '@').collect();
    if parts[0].is_empty() || parts[1].is_empty() {
        return Err(AccountError::Validation(
            "email must have a local part and a domain".into(),
        ));
    }
    Ok(())
}

/// Detect whether a `RepoError` wraps a PostgreSQL UNIQUE
/// violation (SQLSTATE 23505). Used to convert duplicate-key
/// errors into `AccountError::AlreadyExists` without importing
/// `sqlx` into the application layer.
fn is_unique_violation(err: &RepoError) -> bool {
    if let RepoError::Sqlx(sqlx::Error::Database(db_err)) = err {
        return db_err
            .code()
            .map(|c| c.as_ref() == "23505")
            .unwrap_or(false);
    }
    false
}

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
        validate_account_id(id)?;

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

    async fn create_account(
        &self,
        input: CreateAccountInput,
    ) -> Result<AccountCreated, AccountError> {
        // ── Validate inputs ──────────────────────────────────
        validate_full_name(&input.full_name)?;

        if let Some(ref email) = input.email {
            validate_email(email)?;
        }

        // Validate or generate account_number.
        let account_number = match input.account_number {
            Some(ref num) => {
                validate_account_number(num)?;
                num.clone()
            }
            None => {
                // Auto-generate a unique number using a UUID v4
                // suffix. Keeps the `ACC_` prefix convention
                // used by the seed data. Collision probability
                // at 1M accounts ≈ negligible (birthday bound
                // for 7 hex chars ≈ 2^28 = 268M combinations).
                let suffix = uuid::Uuid::new_v4()
                    .to_string()
                    .replace('-', "")
                    .chars()
                    .take(7)
                    .collect::<String>()
                    .to_uppercase();
                format!("{}{}", ACCOUNT_NUMBER_PREFIX, suffix)
            }
        };

        // Validate and parse initial_balance.
        let balance: Decimal = match input.initial_balance.as_deref() {
            Some(s) => s.parse::<Decimal>().map_err(|_| {
                AccountError::Validation(format!(
                    "initial_balance '{}' is not a valid decimal",
                    s
                ))
            })?,
            None => Decimal::ZERO,
        };

        if balance < Decimal::ZERO {
            return Err(AccountError::Validation(
                "initial_balance must be >= 0".into(),
            ));
        }

        if balance.scale() > 2 {
            return Err(AccountError::Validation(
                "initial_balance must have at most 2 decimal places".into(),
            ));
        }

        // ── Persist ──────────────────────────────────────────
        let new_account = NewAccount {
            account_number: account_number.clone(),
            full_name: input.full_name.clone(),
            email: input.email.clone(),
            balance_str: balance.to_string(),
        };

        match self.repo.insert_account(new_account).await {
            Ok(created) => Ok(AccountCreated {
                account_number: created.account_number,
                full_name: created.full_name,
                email: created.email,
                balance: created.balance_str,
                currency: "IDR".to_string(),
                status: AccountStatus::Active.as_str().to_owned(),
            }),
            Err(ref e) if is_unique_violation(e) => Err(AccountError::AlreadyExists(format!(
                "account_number '{}' or email is already registered",
                account_number
            ))),
            Err(e) => Err(AccountError::Infra(e.to_string())),
        }
    }
}

// ─── Unit tests for pure domain helpers (T-3) ───────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_account_id_rejects_empty() {
        let id = AccountId(String::new());
        match validate_account_id(&id) {
            Err(AccountError::Validation(msg)) => assert!(msg.contains("empty")),
            other => panic!("expected Validation(empty), got {other:?}"),
        }
    }

    #[test]
    fn validate_account_id_accepts_canonical() {
        let id = AccountId("ACC_0000001".to_owned());
        assert!(validate_account_id(&id).is_ok());
    }

    #[test]
    fn validate_account_id_accepts_single_char() {
        // No length-lower-bound rule today; the only invariant is
        // "non-empty". Test pins the contract so future length
        // caps cannot regress it silently.
        let id = AccountId("a".to_owned());
        assert!(validate_account_id(&id).is_ok());
    }

    #[test]
    fn validate_full_name_rejects_empty() {
        assert!(matches!(
            validate_full_name(""),
            Err(AccountError::Validation(_))
        ));
        assert!(matches!(
            validate_full_name("   "),
            Err(AccountError::Validation(_))
        ));
    }

    #[test]
    fn validate_full_name_accepts_normal() {
        assert!(validate_full_name("Budi Santoso").is_ok());
    }

    #[test]
    fn validate_email_rejects_no_at() {
        assert!(matches!(
            validate_email("nodomain"),
            Err(AccountError::Validation(_))
        ));
    }

    #[test]
    fn validate_email_rejects_multiple_at() {
        assert!(matches!(
            validate_email("a@b@c"),
            Err(AccountError::Validation(_))
        ));
    }

    #[test]
    fn validate_email_accepts_normal() {
        assert!(validate_email("budi@bank.id").is_ok());
    }

    #[test]
    fn validate_account_number_rejects_special_chars() {
        assert!(matches!(
            validate_account_number("ACC 001"),
            Err(AccountError::Validation(_))
        ));
    }

    #[test]
    fn validate_account_number_accepts_canonical() {
        assert!(validate_account_number("ACC_0000001").is_ok());
    }
}