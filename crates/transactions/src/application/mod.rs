//! Use-case orchestration for `transactions`.
//!
//! One struct per use case. All dependencies are injected as
//! trait objects from `domain/` or as `Arc<dyn ...>` ports from
//! sibling modules. No I/O imports here.

use std::hash::Hasher;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

use accounts::ports::{AccountError, AccountId, DynAccountService};
use shared_kernel::db::shard::ShardRouter;

use super::domain::{
    DomainError, IdempotencyAwareWriter, PublishedTransaction, ReserveOutcome, Transaction,
    TransactionPublisher, TransactionRepository,
};
use super::ports::{
    CreateTransactionInput, ListFilter, TransactionAccepted, TransactionError, TransactionId,
    TransactionService, TransactionStatusView, TransactionView,
};

// ─── Validation constants (parity with legacy handler) ──────

const MAX_ACCOUNT_LEN: usize = 50;
const MAX_REFERENCE_ID_LEN: usize = 100;

fn validate_account(s: &str, field: &str) -> Result<(), TransactionError> {
    if s.is_empty() {
        return Err(TransactionError::Validation(format!(
            "{} must not be empty",
            field
        )));
    }
    if s.len() > MAX_ACCOUNT_LEN {
        return Err(TransactionError::Validation(format!(
            "{} must be at most {} characters",
            field, MAX_ACCOUNT_LEN
        )));
    }
    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(TransactionError::Validation(format!(
            "{} contains invalid characters",
            field
        )));
    }
    Ok(())
}

fn validate_reference_id(s: &str) -> Result<(), TransactionError> {
    if s.len() > MAX_REFERENCE_ID_LEN {
        return Err(TransactionError::Validation(format!(
            "reference_id must be at most {} characters",
            MAX_REFERENCE_ID_LEN
        )));
    }
    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(TransactionError::Validation(
            "reference_id contains invalid characters".into(),
        ));
    }
    Ok(())
}

// ─── Entity → port DTO conversion ───────────────────────────

fn tx_to_view(t: Transaction) -> TransactionView {
    TransactionView {
        id: t.id,
        from_account: t.from_account,
        to_account: t.to_account,
        amount: t.amount.to_string(),
        currency: t.currency,
        status: t.status,
        reference_id: t.reference_id,
        description: t.description,
        created_at: t.created_at,
        updated_at: t.updated_at,
        processed_at: t.processed_at,
    }
}

// ─── The single service, four use-case methods ──────────────
//
// Combining the four methods into one impl keeps the file flat.
// Splitting into per-method structs is a refactor for when the
// dependency graph diverges across methods (e.g. only `create`
// needs the publisher; `list` does not). Today every method's
// dependency set is a subset of the service struct's, so the
// blob impl is simpler.

pub(crate) struct TransactionsService {
    repo: Arc<dyn TransactionRepository>,
    idempotency: Arc<dyn IdempotencyAwareWriter>,
    publisher: Arc<dyn TransactionPublisher>,
    /// The cross-module port. We hold it as the trait alias so
    /// that swapping the `accounts` impl (or stubbing it in
    /// tests) requires zero edits here. This is the seam the
    /// modular-monolith story is built on — see
    /// docs/architecture/phase2-transactions-walkthrough.md §6.2.
    accounts: DynAccountService,
    /// When false (load-test default), `create` skips the
    /// `accounts.get_balance` round-trip — the consumer
    /// re-validates balance under `UPDATE … WHERE balance >= $1`
    /// before debiting, so the only thing this saves is the
    /// fail-fast 400 for unknown senders. Toggled via
    /// `TX_VERIFY_FROM_ACCOUNT` at startup.
    verify_from_account: bool,
}

impl TransactionsService {
    pub(crate) fn new(
        repo: Arc<dyn TransactionRepository>,
        idempotency: Arc<dyn IdempotencyAwareWriter>,
        publisher: Arc<dyn TransactionPublisher>,
        accounts: DynAccountService,
        verify_from_account: bool,
    ) -> Self {
        Self {
            repo,
            idempotency,
            publisher,
            accounts,
            verify_from_account,
        }
    }
}

#[async_trait]
impl TransactionService for TransactionsService {
    async fn create(
        &self,
        input: CreateTransactionInput,
    ) -> Result<TransactionAccepted, TransactionError> {
        // Validation. Parsing now happens once at the API layer
        // — `Decimal` arrives already-typed via serde, so the
        // service only needs to check the domain invariant
        // (positive). The previous implementation re-parsed via
        // `Decimal::from_str` after the handler had already done
        // a `to_string`, which is pure round-trip cost on every
        // create.
        if input.amount <= Decimal::ZERO {
            return Err(TransactionError::Validation(
                "amount must be positive".into(),
            ));
        }
        validate_account(&input.from_account, "from_account")?;
        validate_account(&input.to_account, "to_account")?;
        if input.from_account == input.to_account {
            return Err(TransactionError::Validation(
                "from_account and to_account must differ".into(),
            ));
        }
        if let Some(rid) = input.reference_id.as_deref() {
            validate_reference_id(rid)?;
        }
        if input.currency.trim().is_empty() {
            return Err(TransactionError::Validation(
                "currency must not be empty".into(),
            ));
        }

        // Cross-module sender existence check — gated behind
        // `TX_VERIFY_FROM_ACCOUNT` (default off). Even on a warm
        // cache this is a Redis GET on the hot path, and the
        // consumer already re-validates balance under
        // `UPDATE … WHERE balance >= $1` before debiting, so
        // dropping the synchronous probe is safe at the cost of
        // surfacing "unknown sender" as a `failed` row downstream
        // instead of a 400 here. Re-enable for any environment
        // that wants the fail-fast behaviour.
        if self.verify_from_account {
            match self
                .accounts
                .get_balance(&AccountId(input.from_account.clone()))
                .await
            {
                Ok(_) => {}
                Err(AccountError::NotFound(_)) => {
                    return Err(TransactionError::Validation(format!(
                        "from_account {} does not exist or is not active",
                        input.from_account
                    )));
                }
                Err(AccountError::Validation(m)) => {
                    return Err(TransactionError::Validation(m));
                }
                Err(AccountError::Infra(m)) => {
                    return Err(TransactionError::Infra(m));
                }
            }
        }

        let reference_id = input
            .reference_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let shard = ShardRouter::shard_for(&input.from_account);
        let idempotency_key = format!("txn:{}:{}", shard, reference_id);

        // Canonical wire-form of the amount, computed once. Used
        // for both the request_hash bytes and the queue payload
        // — the consumer's wire schema still expects a JSON
        // string, so this is the single conversion point.
        let amount_str = input.amount.to_string();

        // Stable hash for duplicate detection. The previous
        // implementation built a `serde_json::Value`, allocated a
        // BTreeMap of fields, then `.to_string()`'d it — a few
        // µs of pure overhead per create that showed up under
        // load. fnv-1a over the canonical field bytes (with a
        // 0xff separator so adjacent fields can't be confused)
        // is allocation-free and good enough for collision-class
        // dedupe inside an idempotency_key namespace that is
        // already disambiguated by `reference_id`.
        //
        // Hash format change is incompatible with rows produced
        // by the old code path; the load-test fixture wipes the
        // table between runs, but a real cutover would need a
        // migration of existing `idempotency_keys.request_hash`.
        let request_hash = {
            let mut h = fnv::FnvHasher::default();
            for part in [
                input.from_account.as_bytes(),
                input.to_account.as_bytes(),
                amount_str.as_bytes(),
                input.currency.as_bytes(),
                reference_id.as_bytes(),
                input.description.as_deref().unwrap_or("").as_bytes(),
            ] {
                h.write(part);
                h.write_u8(0xff);
            }
            format!("{:016x}", h.finish())
        };

        let accepted = TransactionAccepted {
            reference_id: reference_id.clone(),
            status: "accepted".into(),
            message: format!("Transaction queued for processing (shard {})", shard),
        };
        let payload = serde_json::to_value(&accepted)
            .map_err(|e| TransactionError::Infra(format!("payload serialise: {e}")))?;

        // Idempotency dance: reserve OR replay OR conflict.
        match self
            .idempotency
            .reserve(shard, &idempotency_key, &request_hash, &payload)
            .await
            .map_err(TransactionError::Infra)?
        {
            ReserveOutcome::Replay(stored) => {
                // Decode the stored accepted payload if it parses,
                // otherwise return the new one (matches legacy
                // fallback behaviour).
                let replayed: TransactionAccepted =
                    serde_json::from_value(stored).unwrap_or_else(|_| accepted.clone());
                return Ok(replayed);
            }
            ReserveOutcome::HashConflict => {
                return Err(TransactionError::IdempotencyConflict(
                    "idempotency key reused with a different payload".into(),
                ));
            }
            ReserveOutcome::Reserved => { /* fall through to publish */ }
        }

        // Publish to the queue. Failures here are infrastructure
        // — the caller has already committed an idempotency row,
        // and the consumer is responsible for re-driving via the
        // status API.
        let request_id = input.request_id.clone().unwrap_or_default();
        self.publisher
            .publish_created(PublishedTransaction {
                from_account: input.from_account.clone(),
                to_account: input.to_account.clone(),
                amount_str,
                currency: input.currency.clone(),
                reference_id: reference_id.clone(),
                description: input.description.clone(),
                request_id,
                shard,
                idempotency_key: idempotency_key.clone(),
                request_hash: request_hash.clone(),
            })
            .await
            .map_err(TransactionError::Infra)?;

        Ok(accepted)
    }

    async fn get_by_id(&self, id: TransactionId) -> Result<TransactionView, TransactionError> {
        match self.repo.find_by_id(id).await {
            Ok(Some(tx)) => Ok(tx_to_view(tx)),
            Ok(None) => Err(TransactionError::NotFound(id.as_uuid().to_string())),
            Err(msg) => Err(TransactionError::Infra(msg)),
        }
    }

    async fn list(&self, filter: ListFilter) -> Result<Vec<TransactionView>, TransactionError> {
        let rows = self
            .repo
            .list(&filter)
            .await
            .map_err(TransactionError::Infra)?;
        Ok(rows.into_iter().map(tx_to_view).collect())
    }

    async fn get_status_by_reference(
        &self,
        reference_id: &str,
    ) -> Result<TransactionStatusView, TransactionError> {
        if reference_id.is_empty() {
            return Err(TransactionError::Validation(
                "reference_id must not be empty".into(),
            ));
        }
        validate_reference_id(reference_id)?;

        match self.repo.find_status_by_reference(reference_id).await {
            Ok(Some(s)) => Ok(TransactionStatusView {
                reference_id: s.reference_id,
                status: s.status,
                processed_at: s.processed_at,
            }),
            Ok(None) => Err(TransactionError::NotFound(reference_id.to_owned())),
            Err(msg) => Err(TransactionError::Infra(msg)),
        }
    }
}

impl From<DomainError> for TransactionError {
    fn from(err: DomainError) -> Self {
        match err {
            DomainError::NotFound(m) => TransactionError::NotFound(m),
            DomainError::Validation(m) => TransactionError::Validation(m),
        }
    }
}

// silence unused-import warning when chrono::Utc is only used
// transitively via the trait return types.
#[allow(dead_code)]
fn _chrono_anchor() -> Utc {
    Utc
}
