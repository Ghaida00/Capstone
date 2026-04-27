//! Legacy DB-shape DTOs the v1 handlers still consume.
//!
//! Phase 4 moved every other file in this directory
//! (`failover.rs`, `pool.rs`, `shard.rs`, `shard_tests.rs`) into
//! `shared_kernel::db`. The Step-A consumer rewire (Phase-2
//! follow-up) removed the consumer's dependency on
//! `CreateTransactionRequest`, so the only remaining caller is
//! `crates/app/src/api/handlers.rs`. The whole file exits the
//! `app` crate when the v1 cull (Step B) lands. See
//! `docs/architecture/cutover-readiness.md` and
//! `docs/architecture/v1-caller-inventory.md`.

pub mod models;
