//! Modular-monolith bounded contexts.
//!
//! Phase 1 — parallel implementation. `accounts` is the first
//! real module; it lives alongside the legacy code under
//! `src/api/`, `src/db/` and is mounted by the bootstrap at
//! `/api/v2/accounts/*`.
//!
//! `_template/`, `transactions/`, and `notifications/` exist as
//! skeletons but are NOT declared here yet — they are pulled in
//! once Phase 2 / Phase 3 of the migration starts.
//!
//! See:
//!   - docs/architecture/modular-monolith.md    — design
//!   - docs/architecture/module-anatomy.md      — per-module shape
//!   - docs/architecture/migration-plan.md      — phased rollout
//!   - docs/architecture/phase1-accounts-walkthrough.md
//!       — file-by-file tour of this module (read me first!)

pub mod accounts;
pub mod transactions;
