use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
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

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;

fn state_from_u8(v: u8) -> CircuitState {
    match v {
        STATE_OPEN => CircuitState::Open,
        STATE_HALF_OPEN => CircuitState::HalfOpen,
        _ => CircuitState::Closed,
    }
}

/// Circuit breaker for protecting downstream services.
///
/// Lock-free hot path — every request used to take a `tokio::Mutex`
/// twice (allow_request + record_*). Under 500–1000 VU load that lock
/// became the dominant tail. Now `allow_request`, `record_success`, and
/// `record_failure` are pure atomic ops; transitions are CAS-protected
/// against double-flip but the request path never blocks.
#[derive(Clone)]
pub struct CircuitBreaker {
    inner: Arc<Inner>,
}

struct Inner {
    state: AtomicU8,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    half_open_request_count: AtomicU32,
    /// Millis since process start. 0 = never failed.
    last_failure_ms: AtomicU64,
    epoch: Instant,
    failure_threshold: u32,
    recovery_timeout: Duration,
    half_open_max_requests: u32,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout_secs: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: AtomicU8::new(STATE_CLOSED),
                failure_count: AtomicU32::new(0),
                success_count: AtomicU32::new(0),
                half_open_request_count: AtomicU32::new(0),
                last_failure_ms: AtomicU64::new(0),
                epoch: Instant::now(),
                failure_threshold,
                recovery_timeout: Duration::from_secs(recovery_timeout_secs),
                half_open_max_requests: 5,
            }),
        }
    }

    fn now_ms(&self) -> u64 {
        self.inner.epoch.elapsed().as_millis() as u64
    }

    fn publish_state_gauge(state: CircuitState) {
        let v = match state {
            CircuitState::Closed => 0.0,
            CircuitState::Open => 1.0,
            CircuitState::HalfOpen => 2.0,
        };
        metrics::gauge!("circuit_breaker_state").set(v);
    }

    /// Check if a request should be allowed through.
    pub fn allow_request(&self) -> bool {
        let inner = &*self.inner;
        match state_from_u8(inner.state.load(Ordering::Acquire)) {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let last = inner.last_failure_ms.load(Ordering::Relaxed);
                if last != 0
                    && self.now_ms().saturating_sub(last)
                        >= inner.recovery_timeout.as_millis() as u64
                {
                    // Try CAS to HalfOpen — only one task wins.
                    if inner
                        .state
                        .compare_exchange(
                            STATE_OPEN,
                            STATE_HALF_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        inner.half_open_request_count.store(0, Ordering::Relaxed);
                        inner.success_count.store(0, Ordering::Relaxed);
                        Self::publish_state_gauge(CircuitState::HalfOpen);
                    }
                    // Whether we won the CAS or not, the circuit is now half-open.
                    self.try_admit_half_open()
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => self.try_admit_half_open(),
        }
    }

    fn try_admit_half_open(&self) -> bool {
        let inner = &*self.inner;
        let prev = inner
            .half_open_request_count
            .fetch_add(1, Ordering::Relaxed);
        prev < inner.half_open_max_requests
    }

    /// Record a successful request.
    pub fn record_success(&self) {
        let inner = &*self.inner;
        match state_from_u8(inner.state.load(Ordering::Acquire)) {
            CircuitState::HalfOpen => {
                let prev = inner.success_count.fetch_add(1, Ordering::Relaxed);
                if prev + 1 >= inner.half_open_max_requests
                    && inner
                        .state
                        .compare_exchange(
                            STATE_HALF_OPEN,
                            STATE_CLOSED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    inner.failure_count.store(0, Ordering::Relaxed);
                    inner.success_count.store(0, Ordering::Relaxed);
                    Self::publish_state_gauge(CircuitState::Closed);
                }
            }
            CircuitState::Closed => {
                inner.failure_count.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a failed request.
    pub fn record_failure(&self) {
        let inner = &*self.inner;
        let prev = inner.failure_count.fetch_add(1, Ordering::Relaxed);
        inner
            .last_failure_ms
            .store(self.now_ms(), Ordering::Relaxed);

        match state_from_u8(inner.state.load(Ordering::Acquire)) {
            CircuitState::Closed => {
                if prev + 1 >= inner.failure_threshold
                    && inner
                        .state
                        .compare_exchange(
                            STATE_CLOSED,
                            STATE_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    Self::publish_state_gauge(CircuitState::Open);
                }
            }
            CircuitState::HalfOpen => {
                if inner
                    .state
                    .compare_exchange(
                        STATE_HALF_OPEN,
                        STATE_OPEN,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    inner.success_count.store(0, Ordering::Relaxed);
                    Self::publish_state_gauge(CircuitState::Open);
                }
            }
            _ => {}
        }
    }

}

/// Axum middleware function for circuit breaker.
pub async fn circuit_breaker_middleware(
    axum::extract::State(cb): axum::extract::State<CircuitBreaker>,
    req: Request,
    next: Next,
) -> Response {
    if !cb.allow_request() {
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
        cb.record_failure();
    } else {
        cb.record_success();
    }

    response
}
