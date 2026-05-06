//! Adapters for the `notifications` module.
//!
//! Today there is exactly one adapter — the in-memory ring
//! buffer that backs the `NotificationStore` trait. Once the
//! `notification_log` table lands, an additional sqlx-backed
//! repository will live next to this file and will be selected
//! at `init` time based on configuration.
//!
//! Bootstrap-facing surface:
//!
//! - [`NotificationsDeps`] — what the api router needs.
//! - [`init`] — assemble the deps and spawn the dispatcher.

pub(crate) mod store;

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use shared_kernel::events::EventSubscriber;

use super::application::{EventDispatcher, NotificationLogService};
use super::domain::NotificationStore;
use super::ports::DynNotificationLog;

use store::InMemoryNotificationStore;

/// Wiring bundle the bootstrap hands to `super::api::router`.
///
/// `Clone` so the bootstrap can clone for nested routers.
#[derive(Clone)]
pub struct NotificationsDeps {
    pub log: DynNotificationLog,
}

/// Construct concrete deps and spawn the event-dispatch loop.
///
/// Called once at startup. The returned `JoinHandle` lets the
/// caller (App / tests) await graceful exit; it is currently
/// just dropped by the bootstrap because the dispatcher
/// honours the `cancel` token and exits before the runtime
/// shuts down.
pub fn init(
    subscriber: Arc<dyn EventSubscriber>,
    cancel: CancellationToken,
) -> (NotificationsDeps, JoinHandle<()>) {
    let store: Arc<dyn NotificationStore> =
        Arc::new(InMemoryNotificationStore::new(/* capacity */ 512));

    let log: DynNotificationLog = Arc::new(NotificationLogService::new(store.clone()));

    let dispatcher = EventDispatcher::new(subscriber, store);
    let handle = dispatcher.spawn(cancel);

    (NotificationsDeps { log }, handle)
}
