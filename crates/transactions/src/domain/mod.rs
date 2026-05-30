//! Pure domain layer for `transactions`.
//!
//! No `sqlx`, no `redis`, no `axum`, no `amqprs` — read this
//! tree front-to-back to understand the business concept.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use super::ports::{ListFilter, TransactionId};

// ─── Entities ────────────────────────────────────────────────
//
// No `DomainError` here on purpose: every error path leaves the
// module through `TransactionError` at the port boundary
// (crates/transactions/src/ports.rs). Add a domain-local error
// type only when a real business-rule violation needs to surface
// independently of infrastructure (none today).

#[derive(Debug, Clone)]
pub(crate) struct Transaction {
    pub id: Uuid,
    pub from_account: String,
    pub to_account: String,
    pub amount: Decimal,
    pub currency: String,
    pub status: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionStatus {
    pub reference_id: String,
    pub status: String,
    pub processed_at: Option<DateTime<Utc>>,
}

// ─── Repository error type (A-1) ────────────────────────────
//
// Typed error at the port boundary so callers can pattern-match
// retryable / non-retryable / observable failure classes instead
// of parsing strings. `RepoError::Sqlx` flows through
// `shared_kernel::db::failover::is_transient` unchanged — the
// application layer (or any future retry-wrapping caller) can
// decide on retry policy by inspecting the variant.

#[derive(Debug, thiserror::Error)]
pub(crate) enum RepoError {
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("join: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    /// Redis errors don't get a typed variant here because adding
    /// `redis::RedisError` to a domain-layer enum would force a
    /// direct `redis` dep on this crate (Redis is an infra detail
    /// the domain doesn't know about). The redis-flavoured
    /// implementor sites format their own context message into
    /// this variant — preserving the typed-error-at-the-port
    /// win for the SQL paths without leaking a layering violation
    /// to keep one boundary tidy.
    #[error("{0}")]
    Other(String),
}

// ─── Repository trait (declared in domain, impl'd in infra) ─

#[async_trait]
pub(crate) trait TransactionRepository: Send + Sync + 'static {
    /// Cross-shard fan-out by id. Returns the first shard that
    /// has the row.
    async fn find_by_id(&self, id: TransactionId) -> Result<Option<Transaction>, RepoError>;

    /// Cross-shard list with keyset cursor + per-shard limit.
    /// Caller is responsible for re-sorting / truncating across
    /// shards if needed (current impl does both).
    async fn list(&self, filter: &ListFilter) -> Result<Vec<Transaction>, RepoError>;

    /// Cross-shard fan-out by reference_id.
    async fn find_status_by_reference(
        &self,
        reference_id: &str,
    ) -> Result<Option<TransactionStatus>, RepoError>;

    /// Cross-shard existence check by reference_id against
    /// `idempotency_keys`. Returns `true` as soon as any shard
    /// reports a hit. Used by `get_status_by_reference` to
    /// disambiguate the accept→flush window: if `transactions`
    /// has no row but `idempotency_keys` does, the request was
    /// accepted and is still in flight (200 + pending), not
    /// genuinely missing (404).
    async fn idempotency_exists_for_reference(
        &self,
        reference_id: &str,
    ) -> Result<bool, RepoError>;
}

// ─── Idempotency-aware writer trait ─────────────────────────

/// Encapsulates the create-time idempotency dance: reserve the
/// key, decide replay vs. revive, return whether the caller
/// should proceed or just replay an earlier accepted response.
/// `outbox_payload` is committed in the same Postgres transaction
/// as the reservation, so a successful `Reserved` outcome means
/// the queue message is durable in the outbox table even if the
/// app pod dies before the publish-outbox worker drains it.
#[async_trait]
pub(crate) trait IdempotencyAwareWriter: Send + Sync + 'static {
    async fn reserve(
        &self,
        shard: usize,
        idempotency_key: &str,
        request_hash: &str,
        accepted_payload: &serde_json::Value,
        outbox_payload: &serde_json::Value,
    ) -> Result<ReserveOutcome, RepoError>;

    /// Cross-shard existence check by reference_id against the
    /// Redis idempotency namespace. Returns `true` if any shard's
    /// `v1:idemp:txn:{shard}:{reference_id}` key is present.
    ///
    /// Closes the second accept→commit race that the spec's
    /// original PG-only fix left open. Under the Hybrid (or pure
    /// Redis) backend the reservation lives in Redis from the
    /// moment `reserve()` returns until the `redis_intake` worker
    /// flushes it to PG. During that window
    /// `TransactionRepository::idempotency_exists_for_reference`
    /// (which only checks PG) returns false even though the request
    /// is genuinely in flight; this method covers the gap.
    ///
    /// PG-only impls return `Ok(false)` — they never write to the
    /// Redis namespace, so checking it would always miss.
    async fn reservation_exists_for_reference(
        &self,
        reference_id: &str,
        num_shards: usize,
    ) -> Result<bool, RepoError>;
}

#[derive(Debug)]

pub(crate) enum ReserveOutcome {
    /// First-mover — durable reservation + outbox row committed.
    Reserved,
    /// Same key + same payload — replay the stored accepted response.
    Replay(serde_json::Value),
    /// Same key, different payload.
    HashConflict,
}
