//! Adapters — where the module meets the outside world.
//!
//! This is the ONLY layer where the following imports are allowed:
//!   * `sqlx` (queries against the tables this module owns).
//!   * `redis` / the shared redis facade (for caching or
//!     distributed locks owned by this module).
//!   * `reqwest` / HTTP clients (for outbound calls to
//!     third-party services).
//!   * `shared_kernel::{db, cache, events}` (the shared
//!     infrastructure facades).
//!
//! Everything in this directory is `pub(crate)` or tighter, EXCEPT
//! the `init` function re-exported at the module root, which is
//! the bootstrap's entry point for wiring this module up.

// pub(crate) mod repository;
// pub(crate) mod events; // optional: event producers/consumers
// mod init;
// pub use init::init;
