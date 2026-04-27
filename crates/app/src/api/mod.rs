//! Legacy v1 HTTP handlers + the standalone `/health` and
//! `/metrics` routes.
//!
//! Phase 4 moved `responses.rs` into `shared_kernel::responses`
//! so module crates can share the standard envelope. The handlers
//! here remain only as long as v1 + `/health` live in the
//! composition root — see
//! `docs/architecture/cutover-readiness.md`.

pub mod handlers;
