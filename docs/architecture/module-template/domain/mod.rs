//! Pure domain layer for this module.
//!
//! Rules (enforced by review; see
//! docs/architecture/dependency-rules.md §2):
//!
//!   * No `sqlx`, `redis`, `axum`, `reqwest`, or any I/O crate.
//!   * No `tokio::fs`, no `tokio::net`.
//!   * May import pure types from `shared_kernel` (e.g. ids,
//!     errors, monetary types) but NOT `shared_kernel::db` etc.
//!   * May NOT import from another module's `ports` — application
//!     layer is where cross-module orchestration happens.
//!
//! Contents of this directory should be readable front-to-back as
//! "what this module does" without ever seeing a SQL query.

// pub mod entity;
// pub mod value_object;
// pub mod error;
// pub mod repository; // trait only — implemented in infrastructure
