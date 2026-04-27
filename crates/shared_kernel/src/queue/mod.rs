//! Message-queue infrastructure.
//!
//! Phase 4 carves out the `producer` (used by `transactions`) into
//! the kernel so module crates can publish without depending on the
//! `app` binary. The `consumer` still lives in the `app` crate until
//! the Phase 2 follow-up rewire moves it into
//! `transactions::infrastructure::consumer`.

pub mod producer;
