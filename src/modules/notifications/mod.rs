//! `notifications` module (C) — out-of-band alerts, event-driven.
//!
//! Has no inbound module dependencies. No other module depends on
//! `notifications` either; a change in any other module never
//! forces this one to rebuild, and vice versa.
//! See `src/modules/notifications/README.md`.
//!
//! Shape (once populated) follows the template at
//! `src/modules/_template/`.

#![allow(dead_code)]

// pub mod ports;
//
// pub(crate) mod domain;
// pub(crate) mod application;
// pub(crate) mod infrastructure;
// pub(crate) mod api;
//
// pub use api::router;
// pub use infrastructure::init;
