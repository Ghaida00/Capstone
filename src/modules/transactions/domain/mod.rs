//! Pure domain layer for `transactions`.
//!
//! No `sqlx`, no `redis`, no `axum`, no `amqprs` — read this
//! tree front-to-back to understand the business concept.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

use super::ports::{ListFilter, TransactionId};

// ─── Entities ────────────────────────────────────────────────

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

// ─── Domain errors ──────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum DomainError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("validation: {0}")]
    Validation(String),
}

// ─── Repository trait (declared in domain, impl'd in infra) ─

#[async_trait]
pub(crate) trait TransactionRepository: Send + Sync + 'static {
    /// Cross-shard fan-out by id. Returns the first shard that
    /// has the row.
    async fn find_by_id(&self, id: TransactionId) -> Result<Option<Transaction>, String>;

    /// Cross-shard list with keyset cursor + per-shard limit.
    /// Caller is responsible for re-sorting / truncating across
    /// shards if needed (current impl does both).
    async fn list(&self, filter: &ListFilter) -> Result<Vec<Transaction>, String>;

    /// Cross-shard fan-out by reference_id.
    async fn find_status_by_reference(
        &self,
        reference_id: &str,
    ) -> Result<Option<TransactionStatus>, String>;
}

// ─── Idempotency-aware writer trait ─────────────────────────

/// Encapsulates the create-time idempotency dance: reserve the
/// key, decide replay vs. revive, return whether the caller
/// should proceed to publish to the queue or just return the
/// existing response. Implementation lives in `infrastructure/`
/// because it spans Postgres + Redis.
#[async_trait]
pub(crate) trait IdempotencyAwareWriter: Send + Sync + 'static {
    async fn reserve(
        &self,
        shard: usize,
        idempotency_key: &str,
        request_hash: &str,
        accepted_payload: &serde_json::Value,
    ) -> Result<ReserveOutcome, String>;
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum ReserveOutcome {
    /// Caller is the first to reserve — proceed to publish.
    Reserved,
    /// Existing matching reservation — replay accepted response.
    Replay(serde_json::Value),
    /// Different request used the same key.
    HashConflict,
}

// ─── Outbound publisher trait (the queue port) ──────────────

/// What the create use case needs from the message bus.
/// Implementation in `infrastructure/publisher.rs` adapts the
/// concrete `crate::queue::QueueProducer`.
///
/// The payload is owned (`String`s, not `&str`s) so the trait
/// method has no lifetime parameters — async-trait gets along
/// with object-safe traits more cleanly that way. The few
/// string clones per publish are negligible against the
/// network round-trip cost of the AMQP publish itself.
#[async_trait]
pub(crate) trait TransactionPublisher: Send + Sync + 'static {
    async fn publish_created(&self, payload: PublishedTransaction) -> Result<(), String>;
}

#[derive(Debug, Serialize)]
pub(crate) struct PublishedTransaction {
    pub from_account: String,
    pub to_account: String,
    pub amount_str: String,
    pub currency: String,
    pub reference_id: String,
    pub description: Option<String>,
    pub request_id: String,
    pub shard: usize,
    pub idempotency_key: String,
    pub request_hash: String,
}
