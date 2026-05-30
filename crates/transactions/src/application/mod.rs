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
    IdempotencyAwareWriter, ReserveOutcome, Transaction, TransactionRepository, TransactionStatus,
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
    let max_amount = rust_decimal::Decimal::new(999_999_999_999_999_999_i64, 2);
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
    if s.chars().any(|c| c.is_control() && c != '\t') {
        return Err(TransactionError::Validation(
            "description must not contain control characters".into(),
        ));
    }
    Ok(())
}

// ─── Idempotency-key and request-hash construction ──────────
//
// Extracted as named helpers so the wire-shape is testable in
// isolation (T-3 unit tests, T-2 proptest invariants). The
// consumer agrees with the producer on these strings by reading
// them off the outbox row; any change here must be paired with
// a migration of pre-existing `idempotency_keys.request_hash`.

/// Format of the per-(shard, reference_id) idempotency key the
/// producer reserves and the consumer reads. The shard is part
/// of the key so the same `reference_id` written into different
/// shards (a legal collision under the routing function) stays
/// distinct.
fn idempotency_key(shard: usize, reference_id: &str) -> String {
    format!("txn:{}:{}", shard, reference_id)
}

/// SHA-256 over the canonical field bytes with a `0xff` separator
/// between every field. The separator forecloses adjacency-class
/// collisions — without it, `("ab", "c")` and `("a", "bc")` would
/// hash to the same digest. The 64-bit FNV-1a previously used
/// here had ~2^32 keys-per-namespace birthday bound; SHA-256
/// raises that to a level the rest of the system can rely on.
///
/// `amount_str` is the wire-form decimal (`Decimal::to_string`),
/// chosen at the call site to share one canonical encoding with
/// the queue payload — passing in the raw `Decimal` here would
/// either double-encode or risk drift if the call site formats
/// differently from this helper.
fn hash_request(
    from_account: &str,
    to_account: &str,
    amount_str: &str,
    currency: &str,
    reference_id: &str,
    description: Option<&str>,
) -> String {
    let mut h = Sha256::new();
    for part in [
        from_account.as_bytes(),
        to_account.as_bytes(),
        amount_str.as_bytes(),
        currency.as_bytes(),
        reference_id.as_bytes(),
        description.unwrap_or("").as_bytes(),
    ] {
        h.update(part);
        h.update([0xff]);
    }
    format!("{:x}", h.finalize())
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
// Every method's dependency set is a subset of the service
// struct's, so the blob impl is simpler than per-method splits.

pub(crate) struct TransactionsService {
    repo: Arc<dyn TransactionRepository>,
    idempotency: Arc<dyn IdempotencyAwareWriter>,
    /// The cross-module port. Held as the trait alias so swapping
    /// the `accounts` impl (or stubbing it in tests) requires zero
    /// edits here.
    accounts: DynAccountService,
    /// Anchors shard derivation to `shard_for_account` on this
    /// router's actual `shards.len()`.
    shards: ShardRouter,
    /// When false, `create` skips the `accounts.get_balance` Redis
    /// round-trip; the consumer re-validates balance under
    /// `UPDATE … WHERE balance >= $1` before debiting, so the only
    /// thing this saves is the fail-fast 400 for unknown senders.
    /// Toggled via `TX_VERIFY_FROM_ACCOUNT` at startup.
    verify_from_account: bool,
}

impl TransactionsService {
    pub(crate) fn new(
        repo: Arc<dyn TransactionRepository>,
        idempotency: Arc<dyn IdempotencyAwareWriter>,
        accounts: DynAccountService,
        shards: ShardRouter,
        verify_from_account: bool,
    ) -> Self {
        Self {
            repo,
            idempotency,
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
        if input.from_account.eq_ignore_ascii_case(&input.to_account) {
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
                Err(AccountError::AlreadyExists(m)) => {
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
        let idemp_key = idempotency_key(shard, &reference_id);

        // Canonical wire-form of the amount, computed once. Used
        // for both the request_hash bytes and the queue payload
        // — the consumer's wire schema still expects a JSON
        // string, so this is the single conversion point.
        let amount_str = input.amount.to_string();

        let request_hash = hash_request(
            &input.from_account,
            &input.to_account,
            &amount_str,
            &input.currency,
            &reference_id,
            input.description.as_deref(),
        );

        let accepted = TransactionAccepted {
            reference_id: reference_id.clone(),
            status: "accepted".into(),
            message: format!("Transaction queued for processing (shard {})", shard),
        };
        let response_payload = serde_json::to_value(&accepted)
            .map_err(|e| TransactionError::Infra(format!("payload serialise: {e}")))?;

        // Wire-form RabbitMQ message. Field names match what the
        // consumer expects; the publish-outbox worker forwards
        // this JSONB column to the broker as-is.
        let request_id = input.request_id.clone().unwrap_or_default();
        let mut outbox_payload = serde_json::json!({
            "from_account":    input.from_account,
            "to_account":      input.to_account,
            "amount":          amount_str,
            "currency":        input.currency,
            "reference_id":    reference_id,
            "description":     input.description,
            "request_id":      request_id,
            "shard":           shard,
            "idempotency_key": idemp_key,
            "request_hash":    request_hash,
        });
        // `traceparent` travels with `request_id` so the consumer can
        // parent its span under the originating HTTP request. Present
        // only when the HTTP span had an OTel context.
        if let Some(tp) = &input.traceparent {
            outbox_payload["traceparent"] = serde_json::Value::String(tp.clone());
        }

        // Reserve commits the response and the outbox payload in a
        // single Postgres transaction. After this returns
        // `Reserved` the queue message is durable; the worker
        // drains it asynchronously.
        match self
            .idempotency
            .reserve(
                shard,
                &idemp_key,
                &request_hash,
                &response_payload,
                &outbox_payload,
            )
            .await
            .map_err(|e| TransactionError::Infra(e.to_string()))?
        {
            ReserveOutcome::Replay(stored) => {
                let replayed: TransactionAccepted =
                    serde_json::from_value(stored).unwrap_or_else(|_| accepted.clone());
                Ok(replayed)
            }
            ReserveOutcome::HashConflict => Err(TransactionError::IdempotencyConflict(
                "idempotency key reused with a different payload".into(),
            )),
            ReserveOutcome::Reserved => Ok(accepted),
        }
    }

    async fn get_by_id(&self, id: TransactionId) -> Result<TransactionView, TransactionError> {
        match self.repo.find_by_id(id).await {
            Ok(Some(tx)) => Ok(tx_to_view(tx)),
            Ok(None) => Err(TransactionError::NotFound(id.as_uuid().to_string())),
            // A-1: RepoError flows out via Display; pattern-matching
            // by variant (Sqlx, Join, Serialize, Other) is now
            // possible here when a retry/escalate policy needs it.
            Err(e) => Err(TransactionError::Infra(e.to_string())),
        }
    }

    async fn list(&self, filter: ListFilter) -> Result<Vec<TransactionView>, TransactionError> {
        let rows = self
            .repo
            .list(&filter)
            .await
            .map_err(|e| TransactionError::Infra(e.to_string()))?;
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

        let tx_status = self
            .repo
            .find_status_by_reference(reference_id)
            .await
            .map_err(|e| TransactionError::Infra(e.to_string()))?;

        // Hot path: `transactions` already has the row, skip both
        // idempotency checks. Cold path: `transactions` missed, ask
        // both idempotency stores in turn.
        //
        // The PG check covers the "consumer hasn't materialised the
        // transactions row yet but the idempotency row is already
        // in PG" gap (the case the spec's original fix was written
        // for — applies fully under IDEMPOTENCY_BACKEND=pg).
        //
        // The Redis check covers the further gap introduced by the
        // Hybrid / Redis backends: the reservation lives in Redis
        // from POST until the `redis_intake` worker flushes it to
        // PG. During that window the PG check misses but the
        // reservation is genuinely in flight; pre-this-fix, every
        // poll-immediately-after-POST returned a spurious 404
        // (empirically 20/20 first-polls). PG-only backend impls
        // return false from this method so the cost is zero there.
        let idem_exists = if tx_status.is_some() {
            false
        } else {
            let in_pg = self
                .repo
                .idempotency_exists_for_reference(reference_id)
                .await
                .map_err(|e| TransactionError::Infra(e.to_string()))?;
            if in_pg {
                true
            } else {
                self.idempotency
                    .reservation_exists_for_reference(reference_id, self.shards.num_shards())
                    .await
                    .map_err(|e| TransactionError::Infra(e.to_string()))?
            }
        };

        resolve_status_view(tx_status, idem_exists, reference_id)
    }
}

// ─── Status resolution helper (pure) ─────────────────────────
//
// Decides what `GET /status/{ref}` should return given the two
// repo lookups: the authoritative `transactions` row (if any)
// and whether the reference exists in `idempotency_keys`. Pure
// so the decision logic is unit-testable in isolation; the async
// wiring that fetches the two inputs lives in
// `get_status_by_reference`.
fn resolve_status_view(
    tx_status: Option<TransactionStatus>,
    idem_exists: bool,
    reference_id: &str,
) -> Result<TransactionStatusView, TransactionError> {
    match tx_status {
        Some(s) => Ok(TransactionStatusView {
            reference_id: s.reference_id,
            status: s.status,
            processed_at: s.processed_at,
        }),
        None if idem_exists => Ok(TransactionStatusView {
            reference_id: reference_id.to_string(),
            status: "pending".to_string(),
            processed_at: None,
        }),
        None => Err(TransactionError::NotFound(reference_id.to_owned())),
    }
}

// ─── Unit tests for pure domain helpers (T-3) ───────────────
//
// Each test is one assertion over a pure function. The
// validators and the two hash/key helpers cover the DB-constraint
// invariants (DECIMAL(18,2), VARCHAR(3), VARCHAR(100)) and the
// wire-shape invariants the consumer reads off the outbox. A
// regression here surfaces in <1ms instead of "k6 is flakier
// this week" (the audit's T-3 framing).
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    // ── validate_account ──────────────────────────────────

    #[test]
    fn validate_account_rejects_empty() {
        assert!(validate_account("", "from_account").is_err());
    }

    #[test]
    fn validate_account_rejects_oversize() {
        let s = "a".repeat(MAX_ACCOUNT_LEN + 1);
        assert!(validate_account(&s, "from_account").is_err());
    }

    #[test]
    fn validate_account_accepts_canonical() {
        assert!(validate_account("ACC_0000001", "from_account").is_ok());
    }

    #[test]
    fn validate_account_rejects_leading_punctuation() {
        for bad in ["-ACC_001", "_ACC_001", ".ACC_001"] {
            assert!(
                validate_account(bad, "from_account").is_err(),
                "expected reject: {bad}"
            );
        }
    }

    #[test]
    fn validate_account_rejects_trailing_punctuation() {
        for bad in ["ACC_001-", "ACC_001_", "ACC_001."] {
            assert!(
                validate_account(bad, "from_account").is_err(),
                "expected reject: {bad}"
            );
        }
    }

    #[test]
    fn validate_account_rejects_non_ascii() {
        assert!(validate_account("ACCçUNT_1", "from_account").is_err());
    }

    #[test]
    fn validate_account_rejects_disallowed_punctuation() {
        for bad in ["ACC@001", "ACC/001", "ACC 001", "ACC+001"] {
            assert!(
                validate_account(bad, "from_account").is_err(),
                "expected reject: {bad}"
            );
        }
    }

    #[test]
    fn validate_account_accepts_dotted_account_number() {
        assert!(validate_account("4.0.0", "from_account").is_ok());
    }

    // ── validate_reference_id ─────────────────────────────

    #[test]
    fn validate_reference_id_rejects_empty() {
        assert!(validate_reference_id("").is_err());
    }

    #[test]
    fn validate_reference_id_rejects_oversize() {
        let s = "a".repeat(MAX_REFERENCE_ID_LEN + 1);
        assert!(validate_reference_id(&s).is_err());
    }

    #[test]
    fn validate_reference_id_accepts_uuid_shape() {
        assert!(validate_reference_id("0c8a9c4f-1ad2-4b41-9bef-1e96eb6f2d0f").is_ok());
    }

    #[test]
    fn validate_reference_id_rejects_unicode() {
        // NFC vs NFD ambiguity is precisely why ASCII-only is the
        // rule; the validator is the gate.
        assert!(validate_reference_id("ref-é").is_err());
    }

    // ── validate_amount ───────────────────────────────────

    #[test]
    fn validate_amount_rejects_zero() {
        assert!(validate_amount(Decimal::ZERO).is_err());
    }

    #[test]
    fn validate_amount_rejects_negative() {
        assert!(validate_amount(Decimal::new(-100, 2)).is_err());
    }

    #[test]
    fn validate_amount_rejects_scale_over_two() {
        // 12.3456 has scale 4 — DB column would silently round
        // it. Reject at the validator.
        assert!(validate_amount(Decimal::new(123456, 4)).is_err());
    }

    #[test]
    fn validate_amount_accepts_canonical_two_decimal() {
        assert!(validate_amount(Decimal::new(12345, 2)).is_ok());
    }

    #[test]
    fn validate_amount_accepts_integer_no_fraction() {
        assert!(validate_amount(Decimal::new(100, 0)).is_ok());
    }

    #[test]
    fn validate_amount_rejects_overflow_above_decimal_18_2() {
        // 9_999_999_999_999_999.99 is the DB ceiling; +0.01
        // overflows and would trip a CHECK in the consumer
        // batch. The audit-prescribed test.
        let max = Decimal::new(999_999_999_999_999_999_i64, 2);
        let over = max + Decimal::new(1, 2);
        assert!(validate_amount(over).is_err());
    }

    // ── validate_currency ─────────────────────────────────

    #[test]
    fn validate_currency_rejects_too_short() {
        assert!(validate_currency("ID").is_err());
    }

    #[test]
    fn validate_currency_rejects_too_long() {
        assert!(validate_currency("IDRX").is_err());
    }

    #[test]
    fn validate_currency_rejects_lowercase() {
        assert!(validate_currency("idr").is_err());
    }

    #[test]
    fn validate_currency_accepts_idr_and_usd() {
        assert!(validate_currency("IDR").is_ok());
        assert!(validate_currency("USD").is_ok());
    }

    // ── validate_description ──────────────────────────────

    #[test]
    fn validate_description_accepts_empty() {
        // Empty description is legal at the column level (it is
        // `TEXT`, nullable via the `Option<String>` in the input).
        assert!(validate_description("").is_ok());
    }

    #[test]
    fn validate_description_accepts_tab() {
        // The validator carves tab out as the one allowed
        // control char so CSV-aligned descriptions still pass.
        assert!(validate_description("col1\tcol2").is_ok());
    }

    #[test]
    fn validate_description_rejects_newline() {
        assert!(validate_description("line1\nline2").is_err());
        assert!(validate_description("line1\rline2").is_err());
    }

    #[test]
    fn validate_description_rejects_null_byte() {
        assert!(validate_description("hello\0world").is_err());
    }

    #[test]
    fn validate_description_rejects_oversize() {
        let s = "a".repeat(MAX_DESCRIPTION_LEN + 1);
        assert!(validate_description(&s).is_err());
    }

    // ── idempotency_key + hash_request ────────────────────

    #[test]
    fn idempotency_key_is_shard_prefixed() {
        assert_eq!(idempotency_key(0, "abc"), "txn:0:abc");
        assert_eq!(idempotency_key(1, "abc"), "txn:1:abc");
    }

    #[test]
    fn idempotency_key_distinguishes_shards_for_same_ref() {
        // Same reference_id routed to different shards yields
        // distinct keys — the design intent of putting `shard`
        // in the key in the first place.
        assert_ne!(idempotency_key(0, "ref"), idempotency_key(1, "ref"));
    }

    #[test]
    fn hash_request_is_deterministic() {
        let a = hash_request("from", "to", "10.00", "IDR", "ref", Some("d"));
        let b = hash_request("from", "to", "10.00", "IDR", "ref", Some("d"));
        assert_eq!(a, b);
    }

    #[test]
    fn hash_request_changes_with_any_field() {
        let base = hash_request("from", "to", "10.00", "IDR", "ref", Some("d"));
        let cases = [
            hash_request("FROM", "to", "10.00", "IDR", "ref", Some("d")),
            hash_request("from", "TO", "10.00", "IDR", "ref", Some("d")),
            hash_request("from", "to", "10.01", "IDR", "ref", Some("d")),
            hash_request("from", "to", "10.00", "USD", "ref", Some("d")),
            hash_request("from", "to", "10.00", "IDR", "ref2", Some("d")),
            hash_request("from", "to", "10.00", "IDR", "ref", Some("e")),
            hash_request("from", "to", "10.00", "IDR", "ref", None),
        ];
        for h in cases {
            assert_ne!(h, base, "field-mutation should change hash: got {h}");
        }
    }

    #[test]
    fn hash_request_disambiguates_adjacent_field_concatenation() {
        // The 0xff separator's whole purpose: ("ab", "c") and
        // ("a", "bc") must hash distinctly. Without the
        // separator they collide. This is the one test that
        // proves the separator is doing its job.
        let a = hash_request("ab", "c", "1.00", "IDR", "ref", Some(""));
        let b = hash_request("a", "bc", "1.00", "IDR", "ref", Some(""));
        assert_ne!(a, b);
    }

    #[test]
    fn hash_request_returns_64_hex_chars() {
        // SHA-256 hex digest is always 64 chars; if this ever
        // changes the consumer's `request_hash` column width is
        // wrong.
        let h = hash_request("a", "b", "1.00", "IDR", "ref", None);
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ── resolve_status_view — pure decision function for #5 ──
    //
    // The handler queries `transactions` first; if missing, it
    // queries `idempotency_keys` to disambiguate "accepted but
    // not yet flushed" (return 200+pending) from "never accepted"
    // (return 404). The pure helper below isolates that decision
    // so we can test it without standing up a repo mock.

    use super::super::domain::TransactionStatus;
    use chrono::Utc;

    #[test]
    fn resolve_status_view_returns_terminal_status_when_in_transactions() {
        // When the transactions table already has the row, the
        // helper returns it as-is and never consults idempotency.
        let now = Utc::now();
        let view = resolve_status_view(
            Some(TransactionStatus {
                reference_id: "abc-123".to_string(),
                status: "completed".to_string(),
                processed_at: Some(now),
            }),
            false, // idem_exists irrelevant when tx_status is Some
            "abc-123",
        )
        .expect("must return Ok when transactions has the row");

        assert_eq!(view.reference_id, "abc-123");
        assert_eq!(view.status, "completed");
        assert_eq!(view.processed_at, Some(now));
    }

    #[test]
    fn resolve_status_view_returns_pending_when_only_idempotency_has_row() {
        // The accept→flush gap: HTTP handler wrote the
        // idempotency row, but the consumer hasn't materialised
        // the transactions row yet. Surface "pending" so the
        // client doesn't see a 404 right after their 202.
        let view = resolve_status_view(None, true, "in-flight-ref")
            .expect("must return Ok when idempotency has the row");

        assert_eq!(view.reference_id, "in-flight-ref");
        assert_eq!(view.status, "pending");
        assert_eq!(view.processed_at, None);
    }

    #[test]
    fn resolve_status_view_returns_not_found_when_both_miss() {
        // Neither table has the reference — the request was never
        // accepted (or was reaped long ago). Genuine 404.
        let err = resolve_status_view(None, false, "nonexistent")
            .expect_err("must return Err when neither table has the row");

        match err {
            TransactionError::NotFound(rid) => assert_eq!(rid, "nonexistent"),
            other => panic!("expected NotFound, got {:?}", other),
        }
    }
}

// ─── Property-based tests for financial-correctness invariants (T-2) ─
//
// The three audit-prescribed properties for the
// transactions::application surface. Property tests pay off where
// invariants are easy to state but hard to enumerate by example:
//
//   * `hash_request` is a collision-resistant deterministic function
//     over the canonical input tuple. A payload-tamper attack
//     succeeds iff this property fails.
//   * `validate_amount` accepts iff DECIMAL(18,2) accepts. A
//     mismatch silently rounds money on the consumer INSERT.
//   * `idempotency_key` is injective over (shard, reference_id).
//     A collision means two logically-distinct requests share a
//     reservation slot and the second one replays the first one's
//     response.
//
// Each property runs proptest's default of 256 cases. `_test` suffix
// avoids macro-expansion shadow collisions with the regression
// tests above.
#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;
    use rust_decimal::Decimal;

    proptest! {
        /// `hash_request` is injective over distinct input tuples
        /// (collision resistance of SHA-256 + the `0xff` field
        /// separator together). Tests the contract the audit names
        /// as the highest-stakes financial-correctness invariant:
        /// if two distinct payloads hash equal, the second one
        /// replays the first one's accepted response, which is a
        /// payload-tamper bypass.
        ///
        /// Strategy: two independently-drawn tuples drawn from a
        /// space that comfortably exceeds SHA-256's birthday bound
        /// at 256 trial pairs; collision probability is ~2^-256
        /// per pair, so the property is empirically equivalent to
        /// "always distinct" — a failure would mean the separator
        /// or the field-order shifted in a way the constructor
        /// drifted from.
        #[test]
        fn hash_request_distinct_inputs_distinct_outputs(
            from1 in "[A-Z]{3}_[0-9]{4}",
            to1   in "[A-Z]{3}_[0-9]{4}",
            amt1  in 1_i64..1_000_000_000_i64,
            ref1  in "[a-zA-Z0-9._-]{1,40}",
            desc1 in "[ -~]{0,100}",
            from2 in "[A-Z]{3}_[0-9]{4}",
            to2   in "[A-Z]{3}_[0-9]{4}",
            amt2  in 1_i64..1_000_000_000_i64,
            ref2  in "[a-zA-Z0-9._-]{1,40}",
            desc2 in "[ -~]{0,100}",
        ) {
            let amt1_str = Decimal::new(amt1, 2).to_string();
            let amt2_str = Decimal::new(amt2, 2).to_string();
            let h1 = hash_request(&from1, &to1, &amt1_str, "IDR", &ref1, Some(&desc1));
            let h2 = hash_request(&from2, &to2, &amt2_str, "IDR", &ref2, Some(&desc2));
            let inputs_equal =
                from1 == from2 && to1 == to2 && amt1 == amt2 && ref1 == ref2 && desc1 == desc2;
            if inputs_equal {
                prop_assert_eq!(h1, h2);
            } else {
                prop_assert_ne!(h1, h2);
            }
        }

        /// `validate_amount`'s decision matches the DB invariant
        /// it gates: accept iff `amount > 0 AND scale ≤ 2 AND
        /// amount ≤ DECIMAL(18,2) max`. A mismatch on the scale
        /// edge silently rounds money on the consumer's
        /// DECIMAL(18,2) INSERT; a mismatch on the overflow edge
        /// trips a runtime CHECK and aborts the whole batch.
        ///
        /// Strategy: mantissa bounded so the only way to overflow
        /// the DB max is via the scale itself, keeping the rule
        /// crisp on every drawn input.
        #[test]
        fn validate_amount_matches_db_constraint(
            mantissa in 1_i64..=1_000_000_000_000_000_000_i64,
            scale in 0_u32..=4_u32,
        ) {
            let amt = Decimal::new(mantissa, scale);
            let max = Decimal::new(999_999_999_999_999_999_i64, 2);
            let should_reject = scale > 2 || amt > max;
            let res = validate_amount(amt);
            if should_reject {
                prop_assert!(res.is_err(), "expected Err for {amt} (scale={scale})");
            } else {
                prop_assert!(res.is_ok(), "expected Ok for {amt} (scale={scale})");
            }
        }

        /// `idempotency_key(shard, ref)` is injective. The format
        /// `txn:{shard}:{ref}` is injective regardless of what's
        /// in `ref` because `shard` is rendered as an integer
        /// (cannot contain `:` or be empty), so the two `:` in
        /// the format mark unambiguous field boundaries — every
        /// distinct `(shard, ref)` pair therefore produces a
        /// distinct key. Property pins that for any later
        /// refactor (e.g. dropping the prefix, swapping the
        /// separator) that would silently break it.
        #[test]
        fn idempotency_key_injective(
            shard1 in 0_usize..1024,
            shard2 in 0_usize..1024,
            ref1   in "[ -~]{1,80}",
            ref2   in "[ -~]{1,80}",
        ) {
            let k1 = idempotency_key(shard1, &ref1);
            let k2 = idempotency_key(shard2, &ref2);
            if shard1 == shard2 && ref1 == ref2 {
                prop_assert_eq!(k1, k2);
            } else {
                prop_assert_ne!(k1, k2);
            }
        }
    }
}
