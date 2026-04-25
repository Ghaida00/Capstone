//! Use-case orchestration.
//!
//! Each public struct here is one use case. The struct holds its
//! dependencies (ports of this module AND of other modules, via
//! `Arc<dyn …>`) and exposes a single async method `execute(...)`
//! or a handful of tightly-related methods.
//!
//! Application services MUST:
//!   * Take dependencies by injection — never look them up from a
//!     global / `OnceCell`.
//!   * Return types defined in this module's `ports.rs` (or
//!     standard library types). Never leak `domain::Entity` to the
//!     caller — the api layer then becomes coupled to domain.
//!   * Not contain SQL or any I/O directly. Delegate to repo
//!     traits in `domain::repository` and their infrastructure
//!     implementations.
//!
//! This is the most testable layer in a module: every dependency
//! is a trait, so tests substitute fakes without infra ever
//! running.

// pub(crate) mod example_use_case;
// pub(crate) use example_use_case::ExampleUseCase;
