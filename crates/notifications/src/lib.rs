//! `notifications` module (C) — out-of-band alerts, event-driven.
//!
//! Has no inbound module dependencies. No other module depends on
//! `notifications` either; a change in any other module never
//! forces this one to rebuild, and vice versa.
//!
//! Phase 3 implementation: subscribes to
//! `shared_kernel::events::EventSubscriber`, currently filters for
//! `transactions.committed` events, and exposes the resulting
//! ring-buffered log via `GET /api/v2/notifications/recent`.
//!
//! See `src/modules/notifications/README.md` for the planned
//! shape (subscriptions, persistent log, dispatch channels) and
//! `docs/architecture/phase3-notifications-walkthrough.md` for
//! a file-by-file tour.

pub mod ports;

pub(crate) mod api;
pub(crate) mod application;
pub(crate) mod domain;
pub(crate) mod infrastructure;

pub use api::router;
pub use infrastructure::init;
