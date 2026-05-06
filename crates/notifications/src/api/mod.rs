//! HTTP gateway for the `notifications` module.
//!
//! Mounted by the bootstrap under `/api/v2/notifications`.
//! Exposes exactly one read-side endpoint for now —
//! `GET /api/v2/notifications/recent`. Subscribe-/unsubscribe-
//! style endpoints will land alongside the
//! `notification_subscriptions` table whenever it is designed.

pub(crate) mod handlers;

use axum::routing::get;
use axum::Router;

use super::infrastructure::NotificationsDeps;

pub fn router(deps: NotificationsDeps) -> Router {
    Router::new()
        .route("/recent", get(handlers::recent))
        .with_state(deps)
}
