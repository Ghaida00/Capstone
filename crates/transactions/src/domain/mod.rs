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
