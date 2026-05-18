//! R-9: operator-controlled graceful-degradation modes.
//!
//! Before this, the only degradation the system had was "reject
//! more requests" (rate-limit 429, breaker 503, backpressure 503)
//! — there was no soft mode between "fully serving" and "rejecting
//! everything". The SaaS bar expects at least one: a payments
//! system should keep answering "what is my balance" during a
//! brief write outage rather than going totally unreachable.
//!
//! `DegradationMode` is a process-global, lock-free
//! (`AtomicU8`, mirroring the circuit-breaker's atomic-state
//! style) flag flippable at runtime via the admin surface
//! (`PUT /api/v2/admin/degradation`) or seeded at startup from
//! `DEGRADATION_MODE`. [`degradation_middleware`] enforces it on
//! the write path; the `degradation_mode` gauge makes the current
//! posture visible to Prometheus/Grafana and alertable.
//!
//! Scope note: `ReadOnly` and `EssentialOnly` both block writes at
//! the HTTP edge (that is the load-bearing, customer-visible
//! behaviour the audit prescribes). The *additional* `EssentialOnly`
//! semantic — pausing non-essential background work (notifications
//! dispatch, idempotency cleanup) — is a separate hook those tasks
//! would consult; it is intentionally not wired here and called out
//! as a follow-up so this change stays bounded to the HTTP contract.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Degradation posture. Discriminants are the on-the-wire gauge
/// values (`degradation_mode`): 0 Normal, 1 ReadOnly, 2
/// EssentialOnly — monotonic in severity so an alert can fire on
/// `degradation_mode > 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationMode {
    Normal = 0,
    ReadOnly = 1,
    EssentialOnly = 2,
}

impl DegradationMode {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => DegradationMode::ReadOnly,
            2 => DegradationMode::EssentialOnly,
            _ => DegradationMode::Normal,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            DegradationMode::Normal => "normal",
            DegradationMode::ReadOnly => "read_only",
            DegradationMode::EssentialOnly => "essential_only",
        }
    }

    /// Parse the env / admin-API spelling. Returns `None` for an
    /// unrecognised value so the caller can reject it explicitly
    /// rather than silently defaulting.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "normal" => Some(DegradationMode::Normal),
            "read_only" | "readonly" => Some(DegradationMode::ReadOnly),
            "essential_only" | "essentialonly" => Some(DegradationMode::EssentialOnly),
            _ => None,
        }
    }

    /// Writes are blocked in every non-Normal mode.
    fn writes_blocked(self) -> bool {
        !matches!(self, DegradationMode::Normal)
    }
}

/// Process-global degradation flag. Cloning shares the same
/// underlying atomic (it is an `Arc`), so the admin handler and
/// every middleware instance observe the same value.
#[derive(Clone)]
pub struct DegradationFlag(Arc<AtomicU8>);

impl DegradationFlag {
    pub fn new(mode: DegradationMode) -> Self {
        let f = DegradationFlag(Arc::new(AtomicU8::new(mode as u8)));
        f.publish_gauge(mode);
        f
    }

    pub fn mode(&self) -> DegradationMode {
        DegradationMode::from_u8(self.0.load(Ordering::Relaxed))
    }

    /// Flip the posture and republish the gauge. Returns the mode
    /// set (echoed by the admin handler).
    pub fn set(&self, mode: DegradationMode) -> DegradationMode {
        self.0.store(mode as u8, Ordering::Relaxed);
        self.publish_gauge(mode);
        tracing::warn!(
            mode = mode.as_str(),
            "degradation mode changed (R-9) — write path posture updated"
        );
        mode
    }

    fn publish_gauge(&self, mode: DegradationMode) {
        metrics::gauge!("degradation_mode").set(mode as u8 as f64);
    }
}

/// Write-path enforcement. GET/HEAD/OPTIONS always pass (reads stay
/// served — that is the entire point of ReadOnly). Mutating
/// methods get a 503 + `Retry-After` when the posture is non-Normal.
/// Placed in the protection stack after `metrics` so the 503 is
/// still RED-counted.
pub async fn degradation_middleware(
    State(flag): State<DegradationFlag>,
    req: Request,
    next: Next,
) -> Response {
    let is_write = !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    let mode = flag.mode();
    if is_write && mode.writes_blocked() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("Retry-After", "5")],
            Json(json!({
                "error": "degraded_read_only",
                "message": "Service is in a degraded mode; writes are temporarily \
                            rejected. Reads remain available.",
                "mode": mode.as_str(),
            })),
        )
            .into_response();
    }
    next.run(req).await
}
