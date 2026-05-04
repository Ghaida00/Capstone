//! Use-case orchestration for `notifications`.
//!
//! Two responsibilities live here:
//!
//! 1. [`EventDispatcher`] — the long-running task that drains the
//!    shared-kernel event bus, maps each event to a
//!    [`NotificationEntry`], and appends it to the store.
//! 2. [`NotificationLogService`] — the read-side service the api
//!    layer calls when answering `GET /api/v2/notifications/recent`.
//!
//! No I/O crates here — both delegate through trait objects from
//! `domain/` (`NotificationStore`) and `shared_kernel::events`
//! (`EventSubscriber`).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::events::{Event, EventSubscriber};

use super::domain::{NotificationStore, TransactionCommittedDomainEvent};
use super::ports::{NotificationEntry, NotificationError, NotificationKind, NotificationLog};

/// Hard cap on the page size returned by the api layer; protects
/// against accidental "give me everything" queries.
const MAX_RECENT: usize = 200;

// ─── Read-side service ─────────────────────────────────────

pub(crate) struct NotificationLogService {
    store: Arc<dyn NotificationStore>,
}

impl NotificationLogService {
    pub(crate) fn new(store: Arc<dyn NotificationStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl NotificationLog for NotificationLogService {
    async fn recent(&self, limit: usize) -> Result<Vec<NotificationEntry>, NotificationError> {
        let clamped = limit.clamp(1, MAX_RECENT);
        self.store.recent(clamped).await
    }
}

// ─── Event-dispatch loop ───────────────────────────────────

/// Long-running task that bridges `shared_kernel::events` to the
/// notification store.
///
/// The constructor returns the dispatcher; `spawn` consumes it
/// and produces a `JoinHandle`. The `cancel` token participates
/// in the same graceful-shutdown protocol as the rest of the
/// app — when triggered, the loop stops draining the bus and
/// exits.
pub(crate) struct EventDispatcher {
    subscriber: Arc<dyn EventSubscriber>,
    store: Arc<dyn NotificationStore>,
}

impl EventDispatcher {
    pub(crate) fn new(
        subscriber: Arc<dyn EventSubscriber>,
        store: Arc<dyn NotificationStore>,
    ) -> Self {
        Self { subscriber, store }
    }

    /// Spawn the dispatch loop. Returns the `JoinHandle` so the
    /// bootstrap can await graceful exit.
    ///
    /// A small bounded set of recently-seen `event.id`s deduplicates
    /// the bus — re-published events (consumer redelivery, replay,
    /// etc.) would otherwise produce duplicate notification entries
    /// for the same transaction. The set is capped via FIFO eviction
    /// so memory is bounded regardless of throughput.
    pub(crate) fn spawn(self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        let mut rx = self.subscriber.subscribe();
        let store = self.store;
        // Bound chosen to cover the practical redelivery window —
        // a few seconds of throughput at peak. Larger means more
        // memory, smaller means duplicate entries leak past the
        // dedupe under sustained re-publish.
        const DEDUPE_WINDOW: usize = 4096;
        let mut seen: std::collections::VecDeque<Uuid> =
            std::collections::VecDeque::with_capacity(DEDUPE_WINDOW);
        let mut seen_set: std::collections::HashSet<Uuid> =
            std::collections::HashSet::with_capacity(DEDUPE_WINDOW);

        tokio::spawn(async move {
            tracing::info!("notifications: event dispatcher started");
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Ok(event) => {
                                if seen_set.contains(&event.id) {
                                    metrics::counter!("notifications_dedup_hits_total")
                                        .increment(1);
                                    continue;
                                }
                                seen_set.insert(event.id);
                                seen.push_back(event.id);
                                if seen.len() > DEDUPE_WINDOW {
                                    if let Some(old) = seen.pop_front() {
                                        seen_set.remove(&old);
                                    }
                                }

                                if let Some(entry) = map_event_to_entry(&event) {
                                    if let Err(e) = store.append(entry).await {
                                        tracing::error!(
                                            error = %e,
                                            event_name = %event.name,
                                            "notifications: failed to append entry"
                                        );
                                    }
                                }
                            }
                            Err(RecvError::Lagged(skipped)) => {
                                // Lossy bus — slow subscriber dropped messages.
                                // Increment a metric so this is visible in
                                // Grafana rather than swallowed silently.
                                metrics::counter!("notifications_events_lagged_total")
                                    .increment(skipped);
                                tracing::warn!(
                                    skipped,
                                    "notifications: dispatcher lagged; messages dropped"
                                );
                            }
                            Err(RecvError::Closed) => {
                                tracing::info!(
                                    "notifications: event bus closed; dispatcher exiting"
                                );
                                break;
                            }
                        }
                    }
                    _ = cancel.cancelled() => {
                        tracing::info!(
                            "notifications: cancellation received; dispatcher exiting"
                        );
                        break;
                    }
                }
            }
        })
    }
}

/// Translate an opaque kernel event into a module-owned
/// notification entry. Returns `None` for events the module is
/// not interested in — that filter is the only place that knows
/// which event names matter, deliberately central so adding a
/// new kind is one match arm + one DTO.
fn map_event_to_entry(event: &Event) -> Option<NotificationEntry> {
    match event.name.as_str() {
        shared_kernel::events::EVENT_TRANSACTIONS_COMMITTED => {
            let payload: TransactionCommittedDomainEvent = match event.decode_payload() {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        event_id = %event.id,
                        "notifications: malformed transactions.committed payload"
                    );
                    metrics::counter!("notifications_payload_decode_errors_total").increment(1);
                    return None;
                }
            };
            let summary = match payload.reference_id.as_deref() {
                Some(rid) => format!(
                    "Received {} {} from {} (ref {})",
                    payload.amount, payload.currency, payload.from_account, rid
                ),
                None => format!(
                    "Received {} {} from {}",
                    payload.amount, payload.currency, payload.from_account
                ),
            };
            Some(NotificationEntry {
                id: Uuid::new_v4(),
                kind: NotificationKind::TransactionCommitted,
                recipient: payload.to_account.clone(),
                summary,
                payload: event.payload.clone(),
                created_at: Utc::now(),
            })
        }
        _ => None,
    }
}
