use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use tokio::sync::Semaphore;

/// Backpressure / load shedding middleware using Semaphore.
/// Instead of instantly rejecting, waits up to 500ms for a slot.
/// This smooths out traffic spikes and reduces 503 error rate.
#[derive(Clone)]
pub struct BackpressureController {
    semaphore: Arc<Semaphore>,
    max_concurrent: usize,
}

impl BackpressureController {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_concurrent,
        }
    }

    /// Get current in-flight count for metrics.
    pub fn current_in_flight(&self) -> usize {
        self.max_concurrent - self.semaphore.available_permits()
    }
}

/// Axum middleware function for backpressure with queuing.
pub async fn backpressure_middleware(
    axum::extract::State(controller): axum::extract::State<BackpressureController>,
    req: Request,
    next: Next,
) -> Response {
    // Try to acquire a permit with 500ms timeout (queue instead of instant reject)
    let permit = match tokio::time::timeout(
        Duration::from_millis(500),
        controller.semaphore.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        _ => {
            metrics::counter!("backpressure_shed_total").increment(1);
            tracing::debug!(
                in_flight = controller.current_in_flight(),
                max = controller.max_concurrent,
                "Backpressure: load shedding after 500ms queue"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "service_overloaded",
                    "message": "Service is currently overloaded, please try again later"
                })),
            )
                .into_response();
        }
    };

    let response = next.run(req).await;

    // Permit is dropped here, releasing the slot
    drop(permit);

    response
}
