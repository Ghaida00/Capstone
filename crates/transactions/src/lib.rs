//! `transactions` module (A) — money movement.
//!
//! Mounted under `/api/v2/transactions/*` and the canonical
//! surface for the write path. The legacy `/api/v1/transactions/*`
//! handlers were deleted in the post-Phase-4 v1 cull (Step B);
//! the consumer they shared the queue with also lives here now
//! at `infrastructure/consumer.rs` (Step A).
//!
//! See `docs/architecture/phase2-transactions-walkthrough.md`
//! for the file-by-file tour. The module mirrors the shape of
//! `accounts/` so the navigation is identical.
//!
//! Dependency position: this crate DEPENDS on
//! `accounts::ports::DynAccountService`, injected at startup
//! and used in `application::TransactionsService::create` to
//! verify the sender exists before reserving the idempotency
//! row. This is the modular-monolith DI seam in active use —
//! after the Phase 4 split it is the COMPILER, not a grep, that
//! enforces "transactions only sees `accounts::ports`": this
//! crate's `Cargo.toml` lists `accounts` as a dependency, but
//! nothing in the `accounts` module surface outside `ports.rs`
//! is `pub` (the rest is `pub(crate)`).

pub mod ports;

pub(crate) mod api;
pub(crate) mod application;
pub(crate) mod domain;
pub(crate) mod infrastructure;

pub use api::router;
pub use infrastructure::{
    init, spawn_cache_invalidator, spawn_cross_shard_processor, spawn_idempotency_cleanup,
    spawn_publish_outbox, start_consumer,
};
