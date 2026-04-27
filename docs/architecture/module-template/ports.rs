//! Public contract of this module. Other modules may import ONLY
//! from this file. Everything here must be:
//!
//!   * object-safe (usable as `Arc<dyn Trait>`),
//!   * `Send + Sync + 'static`, so it survives async boundaries,
//!   * free of infrastructure types (no sqlx rows, no axum extracts,
//!     no redis connection handles leak through these signatures).
//!
//! Convert at the `infrastructure/` boundary if the internal repo
//! returns richer types.
//!
//! See docs/architecture/module-anatomy.md §2 (ports.rs section)
//! for the full shape rules.

// use std::sync::Arc;
// use async_trait::async_trait;
//
// #[derive(Debug, Clone)]
// pub struct ExampleId(pub String);
//
// #[derive(Debug, Clone)]
// pub struct ExampleDto {
//     pub id: ExampleId,
//     pub whatever: String,
// }
//
// #[derive(Debug, thiserror::Error)]
// pub enum ExampleError {
//     #[error("not found: {0}")]
//     NotFound(String),
//     #[error("conflict: {0}")]
//     Conflict(String),
//     #[error("infrastructure: {0}")]
//     Infra(String),
// }
//
// /// The public surface for this module. Other modules inject
// /// `Arc<dyn ExampleService>` (aliased as `DynExampleService`
// /// for ergonomics).
// #[async_trait]
// pub trait ExampleService: Send + Sync + 'static {
//     async fn get(&self, id: &ExampleId) -> Result<ExampleDto, ExampleError>;
// }
//
// pub type DynExampleService = Arc<dyn ExampleService>;
