//! `accounts` module (B) — user accounts and balances.
//!
//! Mounted at `/api/v2/accounts/*` and the canonical surface for
//! account reads. The original `/api/v1/users/{account_number}/balance`
//! endpoint was deleted in the post-Phase-4 v1 cull (Step B);
//! `users` → `accounts` was a resource rename, not just a version
//! bump.
//!
//! See the crate [`README.md`](../../README.md) for the per-module
//! checklist (tables owned / ports exposed / ports consumed / events
//! / operational notes).
//!
//! Dependency position: leaf of the module graph — no other
//! module is imported from here. The rule is enforced at the
//! workspace level: this crate's `Cargo.toml` declares no
//! intra-workspace dependencies, so a regression that imports
//! another module would not compile.

pub mod ports;

pub(crate) mod api;
pub(crate) mod application;
pub(crate) mod domain;
pub(crate) mod infrastructure;

// Re-exports the bootstrap uses. These are the ONLY things
// outside-of-ports that may leave the module.
pub use api::router;
pub use infrastructure::init;
