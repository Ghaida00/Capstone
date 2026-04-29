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
/// Waits up to `wait_ms` for a slot, then 503s. Previous default
/// was 500 ms — too long for a <50 ms p95 budget; the new
/// default (50 ms) plus the smaller `MAX_CONCURRENT_REQUESTS`
/// cap means a saturated app sheds quickly instead of inflating
/// every request's tail with queue time.
#[derive(Clone)]
pub struct BackpressureController {
    semaphore: Arc<Semaphore>,
    max_concurrent: usize,
    wait: Duration,
}

impl BackpressureController {
    pub fn new(max_concurrent: usize, wait_ms: u64) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_concurrent,
            wait: Duration::from_millis(wait_ms),
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
    // Acquire a permit within `wait`; if 0, do an instant
    // try_acquire and shed on miss — that path skips the timer
    // setup overhead entirely for the no-queue regime.
    let permit = if controller.wait.is_zero() {
        match controller.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                metrics::counter!("backpressure_shed_total").increment(1);
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "error": "service_overloaded",
                        "message": "Service is currently overloaded, please try again later"
                    })),
                )
                    .into_response();
            }
        }
    } else {
        match tokio::time::timeout(
            controller.wait,
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
                    "Backpressure: load shedding after queue wait"
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
        }
    };

    // Fix #30: Gauge is updated eagerly here so the metrics handler no
    // longer needs a reference to the controller via AppState.
    metrics::gauge!("backpressure_in_flight").set(controller.current_in_flight() as f64);

    let response = next.run(req).await;

    drop(permit);
    metrics::gauge!("backpressure_in_flight").set(controller.current_in_flight() as f64);

    response
}
