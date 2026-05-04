//! Public contract of the `notifications` module.
//!
//! Phase 3 ships the **read** half of the surface
//! ([`NotificationLog`]); the **write** half (`NotificationDispatcher`
//! that other modules can call to trigger an alert directly,
//! bypassing the bus) is deliberately deferred until a real caller
//! shows up. See `src/modules/notifications/README.md` §3 for the
//! planned shape.
//!
//! Other modules and the api layer may import ONLY from this
//! file — never from `notifications::infrastructure`,
//! `notifications::application`, or `notifications::domain`.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── DTOs ────────────────────────────────────────────────────

/// What kind of activity produced this notification.
///
/// The string values are the lower-cased enum names. They are
/// stable wire-level identifiers — rename a variant and you
/// break HTTP clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationKind {
    /// A `transactions.committed` event was observed.
    TransactionCommitted,
}

/// One entry in the in-memory notification log.
///
/// Phase 3 does NOT yet persist these to a database (see the
/// `notification_log` row in `notifications/README.md` for the
/// planned schema); the ring buffer in `infrastructure/` is the
/// only store. Restarts lose history, which is fine for a
/// proof-of-shape — a real `notification_log` table is a
/// follow-up that swaps the in-memory store for a sqlx-backed
/// repository without touching this DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationEntry {
    pub id: Uuid,
    pub kind: NotificationKind,
    /// Free-form recipient identifier. For `TransactionCommitted`
    /// this is the `to_account` because the receiving side is
    /// the one that gets the "money landed" alert.
    pub recipient: String,
    /// Human-readable summary suitable for direct display in a
    /// list view; richer payload available via [`Self::payload`].
    pub summary: String,
    /// Original event payload, kept for audit / detail views.
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

// ─── Errors ──────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum NotificationError {
    #[error("validation: {0}")]
    Validation(String),

    #[error("infrastructure: {0}")]
    Infra(String),
}

// ─── Service trait ──────────────────────────────────────────

/// Read-only surface for the api layer and any future caller
/// that wants to peek at the notification log.
#[async_trait]
pub trait NotificationLog: Send + Sync + 'static {
    /// Return the most recent `limit` entries, newest-first.
    /// Implementations must clamp `limit` to a sane upper bound.
    async fn recent(&self, limit: usize) -> Result<Vec<NotificationEntry>, NotificationError>;
}

pub type DynNotificationLog = Arc<dyn NotificationLog>;
