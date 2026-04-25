//! `accounts` module (B) — user accounts and balances.
//!
//! Phase 1 parallel implementation. Lives alongside the original
//! `src/api/handlers::get_balance` endpoint without deleting it;
//! the new module is mounted at `/api/v2/accounts/*`, the old path
//! stays at `/api/v1/users/{account_number}/balance`.
//!
//! See `docs/architecture/phase1-accounts-walkthrough.md` for a
//! newbie/intern-friendly explanation of every file in this tree
//! and why it exists.
//!
//! Dependency position: leaf of the module graph — no other
//! module is imported from here. See
//! `docs/architecture/dependency-rules.md`.

pub mod ports;

pub(crate) mod domain;
pub(crate) mod application;
pub(crate) mod infrastructure;
pub(crate) mod api;

// Re-exports the bootstrap uses. These are the ONLY things
// outside-of-ports that may leave the module.
pub use api::router;
pub use infrastructure::init;
