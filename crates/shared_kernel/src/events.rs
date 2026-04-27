//! Cross-module event bus.
//!
//! ## Why this exists
//!
//! Phase 3 of the modular-monolith migration requires
//! `notifications` to react to things that happen elsewhere
//! (`transactions::infrastructure` commits a row, `accounts`
//! flips a status) **without** any module importing another
//! module's types. The kernel-owned [`Event`] envelope makes that
//! possible: the publisher serialises a payload struct of its
//! choosing, the consumer deserialises with its own struct
//! definition, and the two never share a Rust type.
//!
//! ## Today's transport: in-process broadcast
//!
//! [`InProcessEventBus`] is a thin wrapper over
//! `tokio::sync::broadcast`. That fits a single-binary modular
//! monolith perfectly:
//!
//! - Every subscriber receives every event.
//! - The bus is lossy under back-pressure (slow subscribers see
//!   `RecvError::Lagged`); each subscriber decides how to react.
//! - Zero infrastructure cost — no RabbitMQ exchange, no extra
//!   queue, no extra connection pool. Phase 5 (microservice
//!   extraction) is when we swap this for an AMQP-backed impl
//!   that implements the same two traits below.
//!
//! ## Why two traits, not one
//!
//! Publishers do not need the `subscribe` capability and
//! subscribers do not need `publish`. Splitting the trait keeps
//! the dependency graph honest: the consumer holds an
//! `Arc<dyn EventPublisher>`, the notifications module holds an
//! `Arc<dyn EventSubscriber>`. Neither can accidentally do the
//! other's job.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

/// Default channel capacity for the in-process broadcast bus.
///
/// Sized for "many bursts of a few hundred events" rather than
/// "sustained millions" — at sustained rates above this we want
/// real AMQP, not a tokio channel.
const DEFAULT_BUS_CAPACITY: usize = 1024;

/// Neutral event envelope.
///
/// The `payload` is opaque JSON. Publishers fill it via
/// [`Event::new`] from any `Serialize` type they own; subscribers
/// pull it back via [`Event::decode_payload`] using a type they
/// own. The two structs do not need to live in the same crate or
/// module — that is the whole point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Globally unique id, useful for log correlation and
    /// dedup at the consumer side.
    pub id: Uuid,
    /// Dotted name in the `<context>.<verb>` shape, e.g.
    /// `transactions.committed`. Subscribers filter on this.
    pub name: String,
    /// Wall-clock time the publisher created the envelope.
    pub occurred_at: DateTime<Utc>,
    /// Module-defined payload, opaque to the bus.
    pub payload: serde_json::Value,
}

impl Event {
    /// Build a new event from any serialisable payload.
    pub fn new<T: Serialize>(name: impl Into<String>, payload: &T) -> Result<Self, String> {
        let payload =
            serde_json::to_value(payload).map_err(|e| format!("event payload serialise: {e}"))?;
        Ok(Self {
            id: Uuid::new_v4(),
            name: name.into(),
            occurred_at: Utc::now(),
            payload,
        })
    }

    /// Deserialise the payload into a subscriber-owned type.
    pub fn decode_payload<T: for<'de> Deserialize<'de>>(&self) -> Result<T, String> {
        serde_json::from_value(self.payload.clone())
            .map_err(|e| format!("event payload decode: {e}"))
    }
}

/// What a publisher needs.
///
/// Held as `Arc<dyn EventPublisher>` by anything that emits —
/// the queue consumer, future application-layer code, etc.
pub trait EventPublisher: Send + Sync + 'static {
    /// Best-effort publish.
    ///
    /// Returns `Ok(receiver_count)` even when zero subscribers
    /// are listening: the bus's contract is "fire and forget at
    /// least once delivered to every currently-subscribed
    /// listener" — there is no durability guarantee. If you need
    /// at-least-once across restarts, route through AMQP
    /// directly, not through this bus.
    fn publish(&self, event: Event) -> Result<usize, String>;
}

/// What a subscriber needs.
///
/// Held as `Arc<dyn EventSubscriber>` by modules that consume —
/// `notifications` today, `analytics` tomorrow.
pub trait EventSubscriber: Send + Sync + 'static {
    /// Hand out a fresh receiver. Each call gets an independent
    /// queue position, so two subscribers do not steal events
    /// from one another.
    fn subscribe(&self) -> broadcast::Receiver<Event>;
}

/// In-process implementation backed by `tokio::sync::broadcast`.
///
/// One sender, many receivers. Cloning is cheap (`Arc` inside
/// `broadcast::Sender`) so the bootstrap can hand out clones
/// freely.
#[derive(Clone)]
pub struct InProcessEventBus {
    sender: broadcast::Sender<Event>,
}

impl InProcessEventBus {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUS_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }
}

impl Default for InProcessEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventPublisher for InProcessEventBus {
    fn publish(&self, event: Event) -> Result<usize, String> {
        // `broadcast::Sender::send` returns Err only when there
        // are zero active receivers. That is not an error for
        // our semantics — fire-and-forget — so we collapse it
        // to `Ok(0)`.
        Ok(self.sender.send(event).unwrap_or(0))
    }
}

impl EventSubscriber for InProcessEventBus {
    fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }
}
