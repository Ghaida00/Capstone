//! HTTP gateway for the `transactions` module.
//!
//! Mounted by the bootstrap under `/api/v2/transactions`.

pub(crate) mod handlers;

use axum::routing::{get, post};
use axum::Router;

use super::infrastructure::TransactionsDeps;

pub fn router(deps: TransactionsDeps) -> Router {
    Router::new()
        .route("/", post(handlers::create))
        .route("/", get(handlers::list))
        .route("/{id}", get(handlers::get_by_id))
        .route("/status/{reference_id}", get(handlers::get_status))
        .with_state(deps)
}
