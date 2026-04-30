//! Use-case orchestration for `transactions`.
//!
//! One struct per use case. All dependencies are injected as
//! trait objects from `domain/` or as `Arc<dyn ...>` ports from
//! sibling modules. No I/O imports here.

use std::sync::Arc;

use sha2::{Digest, Sha256};

use async_trait::async_trait;
use uuid::Uuid;

use accounts::ports::{AccountError, AccountId, DynAccountService};
use shared_kernel::db::shard::ShardRouter;

use super::domain::{
    IdempotencyAwareWriter, PublishedTransaction, ReserveOutcome, Transaction,
    TransactionPublisher, TransactionRepository,
};
use super::ports::{
    CreateTransactionInput, ListFilter, TransactionAccepted, TransactionError, TransactionId,
    TransactionService, TransactionStatusView, TransactionView,
};

// ─── Validation constants (parity with legacy handler) ──────

const MAX_ACCOUNT_LEN: usize = 50;
const MAX_REFERENCE_ID_LEN: usize = 100;
/// `description` is `TEXT` in DB. Cap at app layer to prevent
/// per-message DoS / cost amplification.
const MAX_DESCRIPTION_LEN: usize = 500;
/// DB column is `VARCHAR(3) NOT NULL DEFAULT 'IDR'`. Reject anything
/// else early — the consumer would otherwise abort the whole batch
/// when the INSERT trips the column-length error.
const CURRENCY_LEN: usize = 3;
/// DB column is `DECIMAL(18, 2)`. Reject scales > 2 (would silently
/// round, losing/creating money) and values that would overflow.
const MAX_AMOUNT_SCALE: u32 = 2;

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
    // First/last char must be alphanumeric to forbid pathological
    // shapes like `"..."`, `"---"`, `"_"`, `".acc"`. The middle
    // characters allow `- _ .` for common account-number formats.
    let first_ok = s
        .chars()
        .next()
        .map(|c| c.is_ascii_alphanumeric())
        .unwrap_or(false);
    let last_ok = s
        .chars()
        .next_back()
        .map(|c| c.is_ascii_alphanumeric())
        .unwrap_or(false);
    if !first_ok || !last_ok {
        return Err(TransactionError::Validation(format!(
            "{} must start and end with an alphanumeric character",
            field
        )));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(TransactionError::Validation(format!(
            "{} contains invalid characters",
            field
        )));
    }
    Ok(())
}

/// Validate the caller-supplied (or UUID-fallback) `reference_id`.
///
/// Rules:
///   * Non-empty.
///   * At most `MAX_REFERENCE_ID_LEN` bytes. ASCII-only means
///     bytes == chars, so this maps one-to-one to the DB column
///     (`VARCHAR(100)`).
///   * Charset: ASCII alphanumeric, `-`, `_`, `.`. Same charset as
///     `validate_account` so the two fields cohabit the idempotency
///     key (`txn:{shard}:{reference_id}`) and the DB UNIQUE on
///     `(reference_id, from_account)` without unicode-normalisation
///     surprises (NFC vs NFD producing distinct keys for the same
///     logical id).
fn validate_reference_id(s: &str) -> Result<(), TransactionError> {
    if s.is_empty() {
        return Err(TransactionError::Validation(
            "reference_id must not be empty".into(),
        ));
    }
    if s.len() > MAX_REFERENCE_ID_LEN {
        return Err(TransactionError::Validation(format!(
            "reference_id must be at most {} characters",
            MAX_REFERENCE_ID_LEN
        )));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(TransactionError::Validation(
            "reference_id must be ASCII alphanumeric, '-', '_' or '.'".into(),
        ));
    }
    Ok(())
}

/// `Decimal::scale()` returns the number of digits to the right of
/// the decimal point. The DB column is `DECIMAL(18, 2)`, so any
/// scale > 2 would silently round on INSERT/UPDATE — creating or
/// destroying money rounding-class amounts. Reject up-front.
fn validate_amount(amount: rust_decimal::Decimal) -> Result<(), TransactionError> {
    if amount <= rust_decimal::Decimal::ZERO {
        return Err(TransactionError::Validation(
            "amount must be positive".into(),
        ));
    }
    if amount.scale() > MAX_AMOUNT_SCALE {
        return Err(TransactionError::Validation(format!(
            "amount must have at most {} decimal places",
            MAX_AMOUNT_SCALE
        )));
    }
    // DECIMAL(18, 2) max = 9_999_999_999_999_999.99 (16 digits before
    // the decimal point, 2 after). Reject amounts that would overflow
    // before the consumer hits a DB CHECK violation that aborts the
    // whole batch.
    let max_amount = rust_decimal::Decimal::new(9_999_999_999_999_999_99i64, 2);
    if amount > max_amount {
        return Err(TransactionError::Validation(
            "amount exceeds maximum DECIMAL(18, 2) value".into(),
        ));
    }
    Ok(())
}

fn validate_currency(s: &str) -> Result<(), TransactionError> {
    if s.len() != CURRENCY_LEN {
        return Err(TransactionError::Validation(format!(
            "currency must be exactly {} characters",
            CURRENCY_LEN
        )));
    }
    if !s.chars().all(|c| c.is_ascii_uppercase()) {
        return Err(TransactionError::Validation(
            "currency must be uppercase ASCII (e.g. IDR, USD)".into(),
        ));
    }
    Ok(())
}

fn validate_description(s: &str) -> Result<(), TransactionError> {
    if s.len() > MAX_DESCRIPTION_LEN {
        return Err(TransactionError::Validation(format!(
            "description must be at most {} characters",
            MAX_DESCRIPTION_LEN
        )));
    }
    // Newlines and control chars break CSV exports / log lines /
    // simple UI rendering downstream. Restrict to printable ASCII +
    // common Latin-1 punctuation; clients with multi-byte needs
    // can update this rule deliberately.
    if s.chars()
        .any(|c| c.is_control() && c != '\t')
    {
        return Err(TransactionError::Validation(
            "description must not contain control characters".into(),
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
    /// Held so shard derivation goes through the instance method
    /// `shard_for_account`, anchored to this router's actual
    /// `shards.len()`. Earlier code used the static
    /// `ShardRouter::shard_for` which read a process-global atomic
    /// — fine in a single-router process, but a footgun the moment
    /// a sibling router publishes a different shard count.
    shards: ShardRouter,
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
        shards: ShardRouter,
        verify_from_account: bool,
    ) -> Self {
        Self {
            repo,
            idempotency,
            publisher,
            accounts,
            shards,
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
        // service only needs to check domain invariants here.
        validate_amount(input.amount)?;
        validate_account(&input.from_account, "from_account")?;
        validate_account(&input.to_account, "to_account")?;
        // Case-insensitive identity check — `"acc1"` vs `"ACC1"`
        // would otherwise pass and the consumer would debit one
        // row and credit a non-existent one. The case-fold also
        // closes a hash-bypass: the idempotency key embeds
        // `from_account`, so distinct casings would otherwise
        // produce distinct keys for the same logical account.
        if input
            .from_account
            .eq_ignore_ascii_case(&input.to_account)
        {
            return Err(TransactionError::Validation(
                "from_account and to_account must differ".into(),
            ));
        }
        if let Some(rid) = input.reference_id.as_deref() {
            validate_reference_id(rid)?;
        }
        validate_currency(&input.currency)?;
        if let Some(desc) = input.description.as_deref() {
            validate_description(desc)?;
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
        let shard = self.shards.shard_for_account(&input.from_account);
        let idempotency_key = format!("txn:{}:{}", shard, reference_id);

        // Canonical wire-form of the amount, computed once. Used
        // for both the request_hash bytes and the queue payload
        // — the consumer's wire schema still expects a JSON
        // string, so this is the single conversion point.
        let amount_str = input.amount.to_string();

        // SHA-256 over the canonical field bytes with a 0xff
        // separator so adjacent fields cannot be confused. The
        // 64-bit fnv-1a previously here was vulnerable to
        // birthday-class collisions (~2^32 keys per namespace) —
        // a hash collision here lets a different request replay
        // the original "accepted" response, masking a payload-
        // tamper attack. SHA-256 closes that bypass.
        //
        // Hash format change is incompatible with rows produced
        // by the old code path; the load-test fixture wipes the
        // table between runs, but a real cutover would need a
        // migration of existing `idempotency_keys.request_hash`.
        let request_hash = {
            let mut h = Sha256::new();
            for part in [
                input.from_account.as_bytes(),
                input.to_account.as_bytes(),
                amount_str.as_bytes(),
                input.currency.as_bytes(),
                reference_id.as_bytes(),
                input.description.as_deref().unwrap_or("").as_bytes(),
            ] {
                h.update(part);
                h.update([0xff]);
            }
            format!("{:x}", h.finalize())
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

        // Publish to the queue. On failure roll back the
        // idempotency reservation so the caller can retry — and so
        // a stuck `processing` row + cached "accepted" payload
        // does not lie to subsequent retries about a transaction
        // that never reached the consumer.
        let request_id = input.request_id.clone().unwrap_or_default();
        let publish_result = self
            .publisher
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
            .await;

        if let Err(publish_err) = publish_result {
            // Best-effort cleanup. If the cleanup itself fails the
            // row will sit in `processing` until natural expiry —
            // bad, but no worse than the no-cleanup status quo.
            // Surface both errors in the log for triage.
            if let Err(release_err) = self
                .idempotency
                .release(shard, &idempotency_key)
                .await
            {
                tracing::error!(
                    publish_err = %publish_err,
                    release_err = %release_err,
                    idempotency_key = %idempotency_key,
                    "publish failed AND idempotency release failed — row stuck until expiry"
                );
                metrics::counter!("idempotency_release_failures_total").increment(1);
            } else {
                tracing::warn!(
                    error = %publish_err,
                    idempotency_key = %idempotency_key,
                    "publish failed, idempotency reservation rolled back"
                );
                metrics::counter!("idempotency_releases_total").increment(1);
            }
            return Err(TransactionError::Infra(publish_err));
        }

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

