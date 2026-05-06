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
// `DomainError` lived here through Phase 4 but never had a
// constructor — every error path goes through `TransactionError`
// at the port boundary. It was removed to keep the surface
// honest; reintroduce when a real domain-layer rule needs to
// signal failure independently of infrastructure.

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
    ) -> Result<ReserveOutcome, String>;
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
