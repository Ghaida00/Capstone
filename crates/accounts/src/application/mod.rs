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
/// VARCHAR(150) in the DB counts characters, not bytes.
/// Use `s.chars().count()` for all non-ASCII-restricted fields. (#5)
const MAX_FULL_NAME_LEN: usize = 150;
const MAX_EMAIL_LEN: usize = 150;
/// Prefix for auto-generated account numbers.
const ACCOUNT_NUMBER_PREFIX: &str = "ACC_";
/// Maximum INSERT attempts when auto-generating an account number.
/// Retries absorb the ~0.37% collision rate at 1 M accounts
/// without ever surfacing a 409 to a caller who supplied no
/// account_number. (#2)
const MAX_AUTO_GENERATE_ATTEMPTS: u32 = 5;

/// Validate a caller-supplied account number. Rules mirror
/// the `users.account_number` column: VARCHAR(50), and the
/// same charset as `validate_account_number` in the handler
/// (alphanumeric, hyphens, underscores, dots).
/// account_number is ASCII-only (charset check above), so
/// byte length == char length and `s.len()` is correct here.
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

/// VARCHAR(150) counts characters. Use `chars().count()` so a name
/// like "Ångström" (8 chars, >8 bytes) is not wrongly rejected. (#5)
fn validate_full_name(s: &str) -> Result<(), AccountError> {
    if s.trim().is_empty() {
        return Err(AccountError::Validation(
            "full_name must not be empty".into(),
        ));
    }
    if s.chars().count() > MAX_FULL_NAME_LEN {
        return Err(AccountError::Validation(format!(
            "full_name must be at most {} characters",
            MAX_FULL_NAME_LEN
        )));
    }
    Ok(())
}

/// VARCHAR(150) counts characters. Use `chars().count()` for the
/// same reason as `validate_full_name`. (#5)
fn validate_email(s: &str) -> Result<(), AccountError> {
    if s.chars().count() > MAX_EMAIL_LEN {
        return Err(AccountError::Validation(format!(
            "email must be at most {} characters",
            MAX_EMAIL_LEN
        )));
    }
    // Minimal sanity: must contain exactly one `@` with at least
    // one char on each side. Full RFC 5321 validation is out of
    // scope.
    //
    // NOTE (#1): email uniqueness is enforced per-shard only
    // (the UNIQUE(email) constraint lives on each PostgreSQL
    // instance). Two accounts with the same email on different
    // shards will both succeed. This is documented behaviour for
    // the capstone scope; a global uniqueness mechanism (e.g. a
    // Redis SET or a dedicated directory table) would be required
    // for a production deployment.
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

/// Generate one candidate auto-account-number.
fn generate_account_number() -> String {
    let suffix = uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .chars()
        .take(7)
        .collect::<String>()
        .to_uppercase();
    format!("{}{}", ACCOUNT_NUMBER_PREFIX, suffix)
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

        // Validate and parse initial_balance early so we pass a
        // typed `Decimal` into `NewAccount` — no String round-trip
        // needed. (#3+#4)
        let balance: Decimal = match input.initial_balance.as_deref() {
            Some(s) => s.parse::<Decimal>().map_err(|_| {
                AccountError::Validation(format!("initial_balance '{}' is not a valid decimal", s))
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

        // ── Resolve account_number ───────────────────────────
        match input.account_number {
            // Caller supplied a number — validate and try once.
            // A 409 here is the caller's fault (they asked for a
            // specific number that already exists). (#2)
            Some(ref num) => {
                validate_account_number(num)?;
                let new_account = NewAccount {
                    account_number: num.clone(),
                    full_name: input.full_name.clone(),
                    email: input.email.clone(),
                    balance,
                };
                match self.repo.insert_account(new_account).await {
                    Ok(created) => Ok(AccountCreated {
                        account_number: created.account_number,
                        full_name: created.full_name,
                        email: created.email,
                        balance: created.balance.to_string(),
                        currency: "IDR".to_string(),
                        status: AccountStatus::Active.as_str().to_owned(),
                    }),
                    Err(ref e) if is_unique_violation(e) => Err(AccountError::AlreadyExists(
                        format!("account_number '{}' is already registered", num),
                    )),
                    Err(e) => Err(AccountError::Infra(e.to_string())),
                }
            }

            // Caller did not supply a number — auto-generate and
            // retry up to MAX_AUTO_GENERATE_ATTEMPTS times on
            // collision so the caller never sees a spurious 409.
            // (#2: ~0.37% collision at 1M accounts, bounded retry
            // makes the failure probability ~(0.0037)^5 ≈ 7×10⁻¹²)
            None => {
                let mut last_err =
                    AccountError::Infra("exhausted auto-generate attempts (internal error)".into());
                for _ in 0..MAX_AUTO_GENERATE_ATTEMPTS {
                    let account_number = generate_account_number();
                    let new_account = NewAccount {
                        account_number: account_number.clone(),
                        full_name: input.full_name.clone(),
                        email: input.email.clone(),
                        balance,
                    };
                    match self.repo.insert_account(new_account).await {
                        Ok(created) => {
                            return Ok(AccountCreated {
                                account_number: created.account_number,
                                full_name: created.full_name,
                                email: created.email,
                                balance: created.balance.to_string(),
                                currency: "IDR".to_string(),
                                status: AccountStatus::Active.as_str().to_owned(),
                            });
                        }
                        Err(ref e) if is_unique_violation(e) => {
                            // Collision — generate a new number and retry.
                            last_err = AccountError::Infra(format!(
                                "auto-generated account_number '{}' collided, retrying",
                                account_number
                            ));
                            continue;
                        }
                        Err(e) => return Err(AccountError::Infra(e.to_string())),
                    }
                }
                Err(last_err)
            }
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

    // #5 fix: chars().count() — non-ASCII names within 150 chars
    // must be accepted even when their byte length exceeds 150.
    #[test]
    fn validate_full_name_accepts_non_ascii_within_char_limit() {
        // "é" = 2 bytes, 1 char. 100 repetitions = 100 chars, 200
        // bytes. The old s.len() > 150 would reject this; the fixed
        // s.chars().count() > 150 accepts it.
        let name = "é".repeat(100);
        assert!(validate_full_name(&name).is_ok());
    }

    #[test]
    fn validate_full_name_rejects_over_char_limit() {
        let name = "a".repeat(151);
        assert!(matches!(
            validate_full_name(&name),
            Err(AccountError::Validation(_))
        ));
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
