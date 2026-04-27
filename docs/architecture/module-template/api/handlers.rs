//! HTTP handlers for this module's endpoints.
//!
//! One handler per route. Target length: shorter than its
//! docstring. If a handler grows branchy, the branching belongs
//! in an `application/` use case, not here.
//!
//! Handlers MUST:
//!   * Extract request data (path, query, json body).
//!   * Call exactly ONE application use case (or, rarely, a port
//!     method directly for simple reads).
//!   * Map the result to HTTP via this module's DTO types.
//!   * Never hold a DB pool, a redis conn, or any infrastructure
//!     handle directly — everything comes through injected ports.

// use axum::{extract::State, Json};
// use super::dto::*;
// use crate::shared_kernel::errors::ApiError;
//
// pub(crate) async fn create_something(
//     State(deps): State<ModuleDeps>,
//     Json(req): Json<CreateSomethingRequest>,
// ) -> Result<Json<CreateSomethingResponse>, ApiError> {
//     let out = deps.use_case.execute(req.into()).await?;
//     Ok(Json(out.into()))
// }
