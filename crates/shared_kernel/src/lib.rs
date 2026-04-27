//! `shared_kernel` — cross-cutting infrastructure available to every
//! business module crate.
//!
//! ## What lives here
//!
//! - [`events`] — neutral cross-module event bus (`Event` envelope,
//!   `EventPublisher` / `EventSubscriber` traits, `InProcessEventBus`).
//! - [`db`] — sharded Postgres router, read/write pools, retry helpers
//!   for transient failures.
//! - [`cache`] — Sentinel-aware Redis facade.
//! - [`queue`] — RabbitMQ producer (consumer still lives in the `app`
//!   crate until Phase 2 follow-up rewires it into `transactions`).
//! - [`error`] — application-wide `AppError` + `IntoResponse` impl.
//! - [`responses`] — standard JSON response wrapper used by every
//!   module's HTTP layer.
//!
//! ## Dependency rule
//!
//! `shared_kernel` may depend on the standard library + external
//! crates only. It MUST NOT `use` anything from `accounts`,
//! `transactions`, `notifications`, or `app`. Phase 4 made that rule
//! a compile error: those crates do not appear in this crate's
//! `Cargo.toml`, so the import would not even resolve.

pub mod cache;
pub mod db;
pub mod error;
pub mod events;
pub mod queue;
pub mod responses;
