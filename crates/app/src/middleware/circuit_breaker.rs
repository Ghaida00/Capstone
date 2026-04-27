use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use tokio::sync::Mutex;

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    /// Normal operation — requests flow through
    Closed,
    /// Failures exceeded threshold — requests are rejected
    Open,
    /// Testing recovery — limited requests allowed through
    HalfOpen,
}

/// Internal state guarded by a single Mutex to prevent TOCTOU races (#17).
struct InnerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    half_open_request_count: u32,
    last_failure_time: Option<Instant>,
}

/// Circuit breaker for protecting downstream services.
///
/// Fix #17: All state transitions are protected by a single `Mutex`
/// instead of separate `RwLock<state>` + atomics. This eliminates the
/// TOCTOU race where two tasks could both read state as `Closed` with
/// 49 failures, both increment to 50, and both trigger transitions.
#[derive(Clone)]
pub struct CircuitBreaker {
    inner: Arc<Mutex<InnerState>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    half_open_max_requests: u32,
    // Metrics (atomic for lockless reads in metrics endpoint)
    total_rejected: Arc<AtomicU64>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout_secs: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(InnerState {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                half_open_request_count: 0,
                last_failure_time: None,
            })),
            failure_threshold,
            recovery_timeout: Duration::from_secs(recovery_timeout_secs),
            half_open_max_requests: 5,
            total_rejected: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Fix #30: Update the Prometheus gauge after any state mutation so
    /// the `/metrics` handler no longer has to reach into AppState.
    fn publish_state_gauge(state: CircuitState) {
        let v = match state {
            CircuitState::Closed => 0.0,
            CircuitState::Open => 1.0,
            CircuitState::HalfOpen => 2.0,
        };
        metrics::gauge!("circuit_breaker_state").set(v);
    }

    /// Check if a request should be allowed through.
    pub async fn allow_request(&self) -> bool {
        let mut inner = self.inner.lock().await;

        match inner.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if recovery timeout has elapsed
                if let Some(t) = inner.last_failure_time {
                    if t.elapsed() >= self.recovery_timeout {
                        // Transition to half-open
                        inner.state = CircuitState::HalfOpen;
                        inner.half_open_request_count = 0;
                        inner.success_count = 0;
                        tracing::info!("Circuit breaker: Open → HalfOpen");
                        Self::publish_state_gauge(inner.state);
                        true
                    } else {
                        self.total_rejected.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests in half-open state
                if inner.half_open_request_count < self.half_open_max_requests {
                    inner.half_open_request_count += 1;
                    true
                } else {
                    self.total_rejected.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
        }
    }

    /// Record a successful request.
    pub async fn record_success(&self) {
        let mut inner = self.inner.lock().await;

        match inner.state {
            CircuitState::HalfOpen => {
                inner.success_count += 1;
                if inner.success_count >= self.half_open_max_requests {
                    // Enough successes — close the circuit
                    inner.state = CircuitState::Closed;
                    inner.failure_count = 0;
                    inner.success_count = 0;
                    tracing::info!("Circuit breaker: HalfOpen → Closed (recovered)");
                    Self::publish_state_gauge(inner.state);
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                inner.failure_count = 0;
            }
            _ => {}
        }
    }

    /// Record a failed request.
    pub async fn record_failure(&self) {
        let mut inner = self.inner.lock().await;

        inner.failure_count += 1;
        inner.last_failure_time = Some(Instant::now());

        match inner.state {
            CircuitState::Closed => {
                if inner.failure_count >= self.failure_threshold {
                    inner.state = CircuitState::Open;
                    tracing::warn!(
                        failures = inner.failure_count,
                        threshold = self.failure_threshold,
                        "Circuit breaker: Closed → Open"
                    );
                    Self::publish_state_gauge(inner.state);
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open goes back to open
                inner.state = CircuitState::Open;
                inner.success_count = 0;
                tracing::warn!("Circuit breaker: HalfOpen → Open (failure during recovery)");
                Self::publish_state_gauge(inner.state);
            }
            _ => {}
        }
    }

    /// Get current circuit state for metrics / tests.
    #[allow(dead_code)]
    pub async fn current_state(&self) -> CircuitState {
        self.inner.lock().await.state
    }

    /// Get total rejected request count.
    #[allow(dead_code)]
    pub fn total_rejected(&self) -> u64 {
        self.total_rejected.load(Ordering::Relaxed)
    }
}

/// Axum middleware function for circuit breaker.
pub async fn circuit_breaker_middleware(
    axum::extract::State(cb): axum::extract::State<CircuitBreaker>,
    req: Request,
    next: Next,
) -> Response {
    if !cb.allow_request().await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "circuit_breaker_open",
                "message": "Service temporarily unavailable, please try again later"
            })),
        )
            .into_response();
    }

    let response = next.run(req).await;

    if response.status().is_server_error() {
        cb.record_failure().await;
    } else {
        cb.record_success().await;
    }

    response
}
