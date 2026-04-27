//! sqlx-backed repository implementation.
//!
//! Implements the trait declared in `super::super::domain::repository`.
//! This is the ONLY file in the module that writes SQL against the
//! tables this module owns.
//!
//! Rules:
//!   * Use the `shared_kernel::db::ShardRouter` facade for pool
//!     selection — never hold a `PgPool` directly.
//!   * Wrap transient errors via `shared_kernel::errors` into the
//!     module's own `DomainError::Infra(...)` variant.
//!   * Do NOT leak `sqlx::Error` out of this file. Every public
//!     fn returns `Result<T, DomainError>`.
//!   * Do NOT use the `query!` macro (compile-time checked against
//!     a live DB) unless / until we have a canonical CI database
//!     stood up for that — prefer `query_as::<_, Row>(...)` with
//!     manually-derived `FromRow`s.
//!
//! When this module is eventually lifted into its own crate
//! (migration Phase 4) the imports below will be the things that
//! have to change — `shared_kernel` becomes a crate dep instead
//! of a sibling module, everything else stays.

// use async_trait::async_trait;
// use crate::shared_kernel::db::ShardRouter;
// use super::super::domain::repository::ExampleRepository;
// use super::super::domain::error::DomainError;
//
// pub(crate) struct SqlxExampleRepository {
//     shards: std::sync::Arc<ShardRouter>,
// }
//
// impl SqlxExampleRepository {
//     pub(crate) fn new(shards: std::sync::Arc<ShardRouter>) -> Self {
//         Self { shards }
//     }
// }
//
// #[async_trait]
// impl ExampleRepository for SqlxExampleRepository {
//     // impl methods here...
// }
