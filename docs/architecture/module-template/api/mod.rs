//! HTTP gateway for this module.
//!
//! Exports a single public constructor, `router(deps) -> Router`,
//! that the top-level bootstrap mounts under the module's prefix
//! (typically `/<module_name>`).
//!
//! Nothing else in this directory is public. Handler fns are
//! `pub(crate)` at most; DTOs are `pub(crate)`; internal helpers
//! are private.
//!
//! Handlers are kept deliberately trivial — they translate HTTP
//! into port calls and back, and that's it. No business logic
//! lives here; any conditional behaviour belongs in
//! `application/`.

// pub(crate) mod dto;
// pub(crate) mod handlers;
//
// use axum::Router;
//
// pub fn router(/* deps: ModuleDeps */) -> Router {
//     Router::new()
//         // .route("/something", post(handlers::create_something))
// }
