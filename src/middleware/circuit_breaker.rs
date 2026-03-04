use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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
use tokio::sync::RwLock;

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

/// Circuit breaker for protecting downstream services.
#[derive(Clone)]
pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitState>>,
    failure_count: Arc<AtomicU32>,
    success_count: Arc<AtomicU32>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
    half_open_max_requests: u32,
    half_open_request_count: Arc<AtomicU32>,
    // Metrics
    total_rejected: Arc<AtomicU64>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout_secs: u64) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: Arc::new(AtomicU32::new(0)),
            success_count: Arc::new(AtomicU32::new(0)),
            failure_threshold,
            recovery_timeout: Duration::from_secs(recovery_timeout_secs),
            last_failure_time: Arc::new(RwLock::new(None)),
            half_open_max_requests: 5,
            half_open_request_count: Arc::new(AtomicU32::new(0)),
            total_rejected: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Check if a request should be allowed through.
    pub async fn allow_request(&self) -> bool {
        let state = *self.state.read().await;

        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if recovery timeout has elapsed
                let last_failure = self.last_failure_time.read().await;
                if let Some(t) = *last_failure {
                    if t.elapsed() >= self.recovery_timeout {
                        drop(last_failure);
                        // Transition to half-open
                        *self.state.write().await = CircuitState::HalfOpen;
                        self.half_open_request_count.store(0, Ordering::SeqCst);
                        tracing::info!("Circuit breaker: Open → HalfOpen");
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
                let count = self.half_open_request_count.fetch_add(1, Ordering::SeqCst);
                if count < self.half_open_max_requests {
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
        let state = *self.state.read().await;

        match state {
            CircuitState::HalfOpen => {
                let successes = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.half_open_max_requests {
                    // Enough successes — close the circuit
                    *self.state.write().await = CircuitState::Closed;
                    self.failure_count.store(0, Ordering::SeqCst);
                    self.success_count.store(0, Ordering::SeqCst);
                    tracing::info!("Circuit breaker: HalfOpen → Closed (recovered)");
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    /// Record a failed request.
    pub async fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        *self.last_failure_time.write().await = Some(Instant::now());

        let state = *self.state.read().await;

        match state {
            CircuitState::Closed => {
                if failures >= self.failure_threshold {
                    *self.state.write().await = CircuitState::Open;
                    tracing::warn!(
                        failures = failures,
                        threshold = self.failure_threshold,
                        "Circuit breaker: Closed → Open"
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open goes back to open
                *self.state.write().await = CircuitState::Open;
                self.success_count.store(0, Ordering::SeqCst);
                tracing::warn!("Circuit breaker: HalfOpen → Open (failure during recovery)");
            }
            _ => {}
        }
    }

    /// Get current circuit state for metrics.
    pub async fn current_state(&self) -> CircuitState {
        *self.state.read().await
    }

    /// Get total rejected request count.
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
