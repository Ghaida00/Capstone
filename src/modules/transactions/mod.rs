//! `transactions` module (A) — money movement.
//!
//! Phase 2 parallel implementation. Mounted under
//! `/api/v2/transactions/*` alongside the legacy
//! `/api/v1/transactions/*` handlers, both pointing at the same
//! shards / Redis / RabbitMQ.
//!
//! See `docs/architecture/phase2-transactions-walkthrough.md`
//! for the file-by-file tour. The module mirrors the shape of
//! `accounts/` so the navigation is identical.
//!
//! Dependency position: this module DEPENDS on
//! `accounts::ports::DynAccountService`, injected at startup
//! and used in `application::TransactionsService::create` to
//! verify the sender exists before reserving the idempotency
//! row. This is the modular-monolith DI seam in active use —
//! grep `rg 'use crate::modules::accounts' src/modules/transactions`
//! to verify the only crossing is through `ports`.

pub mod ports;

pub(crate) mod domain;
pub(crate) mod application;
pub(crate) mod infrastructure;
pub(crate) mod api;

pub use api::router;
pub use infrastructure::init;
