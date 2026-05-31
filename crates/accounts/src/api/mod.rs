//! HTTP gateway for the `accounts` module.
//!
//! Exposes exactly one public function — [`router`] — which the
//! top-level bootstrap mounts under `/api/v2/accounts`. Everything
//! else is `pub(crate)` or tighter.
//!
//! Business logic never lives in this file or its siblings.
//! Handlers translate HTTP ↔ port calls; anything richer belongs
//! in `application/`.

pub(crate) mod handlers;

use axum::routing::{get, post};
use axum::Router;

use super::infrastructure::AccountsDeps;

/// Build the module's sub-router. Bootstrap mounts this under
/// the `/api/v2/accounts` prefix.
pub fn router(deps: AccountsDeps) -> Router {
    Router::new()
        .route("/", post(handlers::create_account))
        .route("/{account_number}/balance", get(handlers::get_balance))
        .with_state(deps)
}
