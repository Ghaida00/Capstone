//! Module template — copy this directory to start a new module.
//!
//! Only two items are public by design:
//!   * `ports`  — the trait-and-DTO contract other modules depend on.
//!   * `api`    — the axum sub-router + the `infrastructure::init`
//!                wiring constructor, re-exported for bootstrap use.
//!
//! Everything else is `pub(crate)` or private. Widening visibility
//! beyond that is a review blocker; the port-adapter shape is
//! documented in `docs/adr/0003-port-adapter-shape.md`.
//!
//! This file is deliberately commented out so the template does not
//! contribute compile work until the module is real. When you copy
//! this directory, uncomment the declarations below.

#![allow(dead_code)]

// pub mod ports;
//
// pub(crate) mod domain;
// pub(crate) mod application;
// pub(crate) mod infrastructure;
// pub(crate) mod api;
//
// // Re-exports for the bootstrap. These are the ONLY things other
// // crates / modules (outside of direct port consumers) may touch.
// pub use api::router;
// pub use infrastructure::init;
