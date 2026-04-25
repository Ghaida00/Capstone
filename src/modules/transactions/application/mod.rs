//! Use-case orchestration for `transactions`.
//!
//! One struct per use case. All dependencies are injected as
//! trait objects from `domain/` or as `Arc<dyn ...>` ports from
//! sibling modules. No I/O imports here.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use uuid::Uuid;

use crate::db::shard::ShardRouter;
use crate::modules::accounts::ports::{
    AccountError, AccountId, DynAccountService,
};

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
}

impl TransactionsService {
    pub(crate) fn new(
        repo: Arc<dyn TransactionRepository>,
        idempotency: Arc<dyn IdempotencyAwareWriter>,
        publisher: Arc<dyn TransactionPublisher>,
        accounts: DynAccountService,
    ) -> Self {
        Self {
            repo,
            idempotency,
            publisher,
            accounts,
        }
    }
}

#[async_trait]
impl TransactionService for TransactionsService {
    async fn create(
        &self,
        input: CreateTransactionInput,
    ) -> Result<TransactionAccepted, TransactionError> {
        // Parity with the legacy validator. Decimal parsing is
        // here (not in the api layer) because amount semantics
        // are domain-level: positive, parseable, currency must
        // be non-empty.
        let amount = Decimal::from_str(&input.amount_str)
            .map_err(|_| TransactionError::Validation("amount must be a decimal".into()))?;
        if amount <= Decimal::ZERO {
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

        // ── Cross-module dependency: verify `from_account` exists
        // ── and is active in the `accounts` module's tables.
        //
        // This is the line the Phase 2 walkthrough §6.2 promised
        // — the modular-monolith dep injection finally exercised
        // at runtime. Note we go through the public port trait
        // (`AccountService::get_balance`); we never read the
        // `users` table directly, even though we technically
        // could from a sqlx-aware module.
        //
        // BEHAVIOURAL DIVERGENCE FROM v1: the legacy
        // `/api/v1/transactions` does not validate the sender
        // exists — it just queues the message and the consumer
        // discovers the missing account when the debit UPDATE
        // matches zero rows. v2 fails fast with a 400 instead.
        // Acceptable divergence: it's a stricter, clearer error.
        match self.accounts.get_balance(&AccountId(input.from_account.clone())).await {
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

        let reference_id = input
            .reference_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let shard = ShardRouter::shard_for(&input.from_account);
        let idempotency_key = format!("txn:{}:{}", shard, reference_id);

        // Stable hash for duplicate detection — must match the
        // legacy producer's hash so the same key routes to the
        // same row regardless of v1/v2 path.
        let request_hash = json!({
            "from_account": input.from_account,
            "to_account":   input.to_account,
            "amount":       input.amount_str,
            "currency":     input.currency,
            "reference_id": reference_id,
            "description":  input.description,
        })
        .to_string();

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
                amount_str: input.amount_str.clone(),
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

    async fn get_by_id(
        &self,
        id: TransactionId,
    ) -> Result<TransactionView, TransactionError> {
        match self.repo.find_by_id(id).await {
            Ok(Some(tx)) => Ok(tx_to_view(tx)),
            Ok(None) => Err(TransactionError::NotFound(id.as_uuid().to_string())),
            Err(msg) => Err(TransactionError::Infra(msg)),
        }
    }

    async fn list(
        &self,
        filter: ListFilter,
    ) -> Result<Vec<TransactionView>, TransactionError> {
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
