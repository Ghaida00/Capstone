//! Pure domain types for `notifications`.
//!
//! No I/O crates here — no `sqlx`, no `redis`, no `axum`,
//! no `tokio::sync::broadcast`. The kernel's `Event` envelope
//! enters the module at the `infrastructure/` boundary; what
//! arrives in this file is already deserialised into module-owned
//! shapes.
//!
//! See `docs/architecture/module-anatomy.md` §3 for why the
//! repository trait declared here lives in `domain/` rather than
//! in `infrastructure/`: domain says what it needs, infrastructure
//! implements it. Classic dependency inversion.

use async_trait::async_trait;
use serde::Deserialize;

use super::ports::{NotificationEntry, NotificationError};

// ─── Domain payloads (deserialise targets for kernel events) ─

/// Notifications' private view of the `transactions.committed`
/// event. The publisher (queue consumer today) owns its own
/// struct; this one is the consumer's.
///
/// Field names MUST match the publisher's serialised JSON so the
/// `Event::decode_payload::<TransactionCommittedDomainEvent>()`
/// call at the boundary succeeds.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TransactionCommittedDomainEvent {
    pub to_account: String,
    pub from_account: String,
    pub amount: String,
    pub currency: String,
    pub reference_id: Option<String>,
}

// ─── Repository trait (declared here, impl'd in infrastructure) ─

/// The store that backs the read-side notification log.
///
/// The Phase 3 implementation is an in-memory ring buffer; a
/// later phase will add a sqlx-backed `notification_log` table.
/// Both implementations satisfy this trait, so the application
/// layer code does not need to know which is in play.
#[async_trait]
pub(crate) trait NotificationStore: Send + Sync + 'static {
    /// Append a new entry. Older entries may be evicted to honour
    /// the implementation's capacity bound.
    async fn append(&self, entry: NotificationEntry) -> Result<(), NotificationError>;

    /// Return the most recent `limit` entries, newest-first.
    async fn recent(&self, limit: usize) -> Result<Vec<NotificationEntry>, NotificationError>;
}
