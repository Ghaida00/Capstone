//! R-7: per-dependency circuit breakers.
//!
//! The app already has one excellent lock-free HTTP-edge breaker
//! (audit R-10, above-bar) — but it is a single global instance
//! keyed on HTTP 5xx, so a transient Redis hiccup and a DB
//! connection drop look identical and tripping rejects *every*
//! route. The standard mid-size-SaaS pattern (Nygard, *Release
//! It!* — "one breaker per dependency, not one per service") is a
//! breaker around each upstream, tripped by *that* dependency's
//! error class, failing fast with a typed error.
//!
//! This is that primitive: a dependency-agnostic, lock-free
//! breaker with the same atomic-state-machine design as the edge
//! breaker (no mutex on the hot path). Each call site (DB / Redis
//! / RabbitMQ wiring lands in follow-up commits) decides what
//! counts as a failure for *its* dependency and maps an open
//! breaker to [`crate::error::AppError::DependencyDown`] (503 +
//! `Retry-After`).

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;

/// Observable breaker state (for metrics / tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

fn state_from_u8(v: u8) -> BreakerState {
    match v {
        STATE_OPEN => BreakerState::Open,
        STATE_HALF_OPEN => BreakerState::HalfOpen,
        _ => BreakerState::Closed,
    }
}

/// Lock-free per-dependency circuit breaker.
///
/// `allow()`, `record_success()`, `record_failure()` are pure
/// atomic ops; state transitions are CAS-protected against a
/// double-flip but the call path never blocks. Cloning shares the
/// same `Arc<Inner>` so every wiring site of a given dependency
/// observes one breaker.
#[derive(Clone, Debug)]
pub struct DependencyBreaker {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// Stable dependency label (`db` | `redis` | `rabbitmq`) used
    /// for the typed error and the `dependency_breaker_state`
    /// gauge. `&'static str` so it threads into
    /// `AppError::DependencyDown` without an allocation.
    name: &'static str,
    state: AtomicU8,
    failure_count: AtomicU32,
    half_open_request_count: AtomicU32,
    /// Millis since `epoch`. 0 = never failed.
    last_failure_ms: AtomicU64,
    epoch: Instant,
    failure_threshold: u32,
    recovery_timeout: Duration,
    half_open_max_requests: u32,
}

impl DependencyBreaker {
    pub fn new(name: &'static str, failure_threshold: u32, recovery_timeout_secs: u64) -> Self {
        let b = Self {
            inner: Arc::new(Inner {
                name,
                state: AtomicU8::new(STATE_CLOSED),
                failure_count: AtomicU32::new(0),
                half_open_request_count: AtomicU32::new(0),
                last_failure_ms: AtomicU64::new(0),
                epoch: Instant::now(),
                failure_threshold,
                recovery_timeout: Duration::from_secs(recovery_timeout_secs),
                half_open_max_requests: 5,
            }),
        };
        b.publish_gauge(BreakerState::Closed);
        b
    }

    pub fn name(&self) -> &'static str {
        self.inner.name
    }

    pub fn state(&self) -> BreakerState {
        state_from_u8(self.inner.state.load(Ordering::Acquire))
    }

    fn now_ms(&self) -> u64 {
        self.inner.epoch.elapsed().as_millis() as u64
    }

    fn publish_gauge(&self, state: BreakerState) {
        let v = match state {
            BreakerState::Closed => 0.0,
            BreakerState::Open => 1.0,
            BreakerState::HalfOpen => 2.0,
        };
        metrics::gauge!("dependency_breaker_state", "dep" => self.inner.name).set(v);
    }

    /// Should a call to this dependency be attempted? `false`
    /// means fail fast (the caller returns
    /// `AppError::DependencyDown { name }`). When Open and the
    /// recovery timeout has elapsed, exactly one caller wins the
    /// CAS into HalfOpen and is allowed through as the probe.
    pub fn allow(&self) -> bool {
        let inner = &*self.inner;
        match state_from_u8(inner.state.load(Ordering::Acquire)) {
            BreakerState::Closed => true,
            BreakerState::Open => {
                let last = inner.last_failure_ms.load(Ordering::Relaxed);
                if last != 0
                    && self.now_ms().saturating_sub(last)
                        >= inner.recovery_timeout.as_millis() as u64
                    && inner
                        .state
                        .compare_exchange(
                            STATE_OPEN,
                            STATE_HALF_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    // This CAS-winning caller IS the first probe —
                    // consume slot 0 so half-open admits exactly
                    // `half_open_max_requests` total, not max + 1.
                    inner.half_open_request_count.store(1, Ordering::Release);
                    self.publish_gauge(BreakerState::HalfOpen);
                    return true;
                }
                false
            }
            BreakerState::HalfOpen => {
                // Admit a bounded number of probes; the rest fail
                // fast until one resolves the trial.
                let n = inner.half_open_request_count.fetch_add(1, Ordering::AcqRel);
                n < inner.half_open_max_requests
            }
        }
    }

    /// Record a successful call. Closes the breaker from HalfOpen
    /// (recovery confirmed) and clears the failure tally.
    pub fn record_success(&self) {
        let inner = &*self.inner;
        match state_from_u8(inner.state.load(Ordering::Acquire)) {
            BreakerState::HalfOpen => {
                if inner
                    .state
                    .compare_exchange(
                        STATE_HALF_OPEN,
                        STATE_CLOSED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    inner.failure_count.store(0, Ordering::Release);
                    self.publish_gauge(BreakerState::Closed);
                }
            }
            BreakerState::Closed => {
                inner.failure_count.store(0, Ordering::Relaxed);
            }
            BreakerState::Open => {}
        }
    }

    /// Record a failed call. Trips Closed→Open at the threshold,
    /// and HalfOpen→Open immediately (the trial failed).
    pub fn record_failure(&self) {
        let inner = &*self.inner;
        inner
            .last_failure_ms
            .store(self.now_ms().max(1), Ordering::Relaxed);
        match state_from_u8(inner.state.load(Ordering::Acquire)) {
            BreakerState::Closed => {
                let n = inner.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                if n >= inner.failure_threshold
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
                    metrics::counter!(
                        "dependency_breaker_trips_total",
                        "dep" => inner.name
                    )
                    .increment(1);
                    self.publish_gauge(BreakerState::Open);
                }
            }
            BreakerState::HalfOpen => {
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
                    metrics::counter!(
                        "dependency_breaker_trips_total",
                        "dep" => inner.name
                    )
                    .increment(1);
                    self.publish_gauge(BreakerState::Open);
                }
            }
            BreakerState::Open => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_allows_until_threshold_then_opens() {
        let b = DependencyBreaker::new("test", 3, 60);
        assert_eq!(b.state(), BreakerState::Closed);
        assert!(b.allow());
        b.record_failure();
        b.record_failure();
        assert_eq!(
            b.state(),
            BreakerState::Closed,
            "below threshold stays closed"
        );
        b.record_failure(); // 3rd → trips
        assert_eq!(b.state(), BreakerState::Open);
        assert!(!b.allow(), "open breaker fails fast");
    }

    #[test]
    fn success_resets_failure_tally() {
        let b = DependencyBreaker::new("test", 3, 60);
        b.record_failure();
        b.record_failure();
        b.record_success(); // clears the tally
        b.record_failure();
        b.record_failure();
        assert_eq!(
            b.state(),
            BreakerState::Closed,
            "tally was reset by the success, so 2 more is still < 3"
        );
    }

    #[test]
    fn open_transitions_to_half_open_after_timeout_then_closes_on_success() {
        // 0s recovery timeout so the half-open transition is
        // immediate and the test is deterministic (no sleep).
        let b = DependencyBreaker::new("test", 1, 0);
        b.record_failure(); // threshold 1 → Open
        assert_eq!(b.state(), BreakerState::Open);
        // recovery_timeout = 0 → allow() wins the CAS into HalfOpen
        assert!(b.allow(), "after timeout, one probe is admitted");
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.record_success();
        assert_eq!(
            b.state(),
            BreakerState::Closed,
            "successful probe closes it"
        );
    }

    #[test]
    fn half_open_failure_reopens() {
        let b = DependencyBreaker::new("test", 1, 0);
        b.record_failure();
        assert!(b.allow()); // → HalfOpen
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.record_failure(); // trial failed
        assert_eq!(b.state(), BreakerState::Open);
    }

    #[test]
    fn half_open_admits_bounded_probes_only() {
        let b = DependencyBreaker::new("test", 1, 0);
        b.record_failure();
        // First allow() wins the CAS into HalfOpen and counts as
        // probe 0; the next 4 are admitted (max 5), then refused.
        assert!(b.allow());
        let mut admitted = 1;
        for _ in 0..10 {
            if b.allow() {
                admitted += 1;
            }
        }
        assert_eq!(
            admitted, 5,
            "half-open admits exactly half_open_max_requests"
        );
    }
}
