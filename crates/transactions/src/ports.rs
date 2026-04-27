//! Public contract of the `transactions` module.
//!
//! Other modules / the bootstrap may import ONLY from this file.
//! All DTOs are infrastructure-free: no `sqlx`, no `axum`, no
//! `amqprs`. Conversions happen at the `infrastructure/` and
//! `api/` boundaries.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── DTOs ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId(pub Uuid);

impl TransactionId {
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// Input to the create use case. Strings (not typed AccountIds)
/// because Phase 2 keeps wire-level parity with the legacy
/// endpoint, which used plain strings.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateTransactionInput {
    pub from_account: String,
    pub to_account: String,
    /// Decimal serialised as string at the wire boundary.
    pub amount_str: String,
    pub currency: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
    /// Carried through for tracing — not used by domain logic.
    pub request_id: Option<String>,
}

/// What the create use case returns. Mirrors the legacy
/// `TransactionCreatedResponse` shape so the v2 endpoint emits
/// byte-identical JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionAccepted {
    pub reference_id: String,
    pub status: String,
    pub message: String,
}

/// Read-side DTO. Mirrors legacy `TransactionResponse`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionView {
    pub id: Uuid,
    pub from_account: String,
    pub to_account: String,
    pub amount: String,
    pub currency: String,
    pub status: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

/// Status-only projection used by `get_status_by_reference`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStatusView {
    pub reference_id: String,
    pub status: String,
    pub processed_at: Option<DateTime<Utc>>,
}

/// Filter for the list use case. Cursor-based pagination
/// matches the legacy endpoint exactly.
#[derive(Debug, Clone)]
pub struct ListFilter {
    pub limit: u32,
    pub before: Option<DateTime<Utc>>,
}

// ─── Errors ──────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("validation: {0}")]
    Validation(String),

    /// Idempotency key already used with a different request payload.
    #[error("idempotency conflict: {0}")]
    IdempotencyConflict(String),

    #[error("infrastructure: {0}")]
    Infra(String),
}

// ─── Service trait ──────────────────────────────────────────

#[async_trait]
pub trait TransactionService: Send + Sync + 'static {
    async fn create(
        &self,
        input: CreateTransactionInput,
    ) -> Result<TransactionAccepted, TransactionError>;

    async fn get_by_id(&self, id: TransactionId) -> Result<TransactionView, TransactionError>;

    async fn list(&self, filter: ListFilter) -> Result<Vec<TransactionView>, TransactionError>;

    async fn get_status_by_reference(
        &self,
        reference_id: &str,
    ) -> Result<TransactionStatusView, TransactionError>;
}

pub type DynTransactionService = Arc<dyn TransactionService>;
