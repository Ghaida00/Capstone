//! Retry helpers for transient DB failures (connection reset during
//! pgBouncer restart, brief primary unavailability during a Patroni
//! promotion, HAProxy marking a backend DOWN while the shard primary
//! flips, etc.).
//!
//! The retry budget (attempts × backoff) is tuned in `src/config.rs`
//! to cover the typical promotion window; see
//! `docs/ha-architecture.md` §2 for the full timeline. If the window
//! exceeds the budget the error propagates as 5xx and the HTTP caller
//! retries — that is by design, not a bug.
//!
//! Non-transient errors (constraint violations, not-found, syntax) are
//! returned immediately — we only retry on errors that look like "try
//! again in a moment".
//!
//! Helpers here are intentionally free-standing — adopt them at any
//! idempotent call site where a transient DB failure should not
//! bubble as a 5xx. Wiring into non-idempotent writes is unsafe
//! without upstream dedup (see `ON CONFLICT (reference_id)` patterns
//! in `src/queue/consumer.rs`).

use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

use crate::error::AppError;
use crate::resilience::DependencyBreaker;

/// Classify whether a `sqlx::Error` is worth retrying.
///
/// Transient = network/pool/worker issue. Logic errors pass through.
pub fn is_transient(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Io(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
    )
}

/// Run `op` up to `max_attempts` times, sleeping `backoff_ms * attempt`
/// between tries on transient errors. Permanent errors return immediately.
///
/// `op_name` is only used for logging.
pub async fn retry_transient<F, Fut, T>(
    mut op: F,
    max_attempts: u32,
    backoff_ms: u64,
    op_name: &str,
) -> Result<T, sqlx::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    let mut attempt = 0u32;
    let max_attempts = max_attempts.max(1);
    loop {
        attempt += 1;
        match op().await {
            Ok(v) => {
                if attempt > 1 {
                    tracing::info!(op = op_name, attempt, "DB op succeeded after retry");
                    metrics::counter!("db_retry_success_total", "op" => op_name.to_string())
                        .increment(1);
                }
                return Ok(v);
            }
            Err(e) if is_transient(&e) && attempt < max_attempts => {
                metrics::counter!("db_retry_attempt_total", "op" => op_name.to_string())
                    .increment(1);
                tracing::warn!(
                    op = op_name,
                    attempt,
                    max_attempts,
                    error = %e,
                    "Transient DB error, retrying"
                );
                sleep(Duration::from_millis(
                    backoff_ms.saturating_mul(attempt as u64),
                ))
                .await;
            }
            Err(e) => {
                if attempt > 1 || is_transient(&e) {
                    metrics::counter!("db_retry_exhausted_total", "op" => op_name.to_string())
                        .increment(1);
                }
                return Err(e);
            }
        }
    }
}

/// R-7: breaker-protected sibling of [`retry_transient`].
///
/// Same retry semantics, but every call passes through
/// `breaker.allow()` first and every transient outcome is reported
/// to the breaker. A breaker open at call time — or a breaker that
/// opens mid-retry because of a parallel caller's transient
/// failures — fails fast with [`AppError::DependencyDown`] (503 +
/// `Retry-After`) instead of waiting the full backoff window. The
/// audit's "DbBreaker tripping on `is_transient()`" half (R-7).
///
/// Non-transient errors (constraint violations, syntax, not-found
/// returned as `Error::RowNotFound`) are propagated as
/// `AppError::Database` and do NOT count against the breaker —
/// they are the caller's responsibility, not the dependency's.
pub async fn retry_transient_with_breaker<F, Fut, T>(
    breaker: &DependencyBreaker,
    mut op: F,
    max_attempts: u32,
    backoff_ms: u64,
    op_name: &str,
) -> Result<T, AppError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    if !breaker.allow() {
        return Err(AppError::DependencyDown {
            name: breaker.name(),
        });
    }

    let mut attempt = 0u32;
    let max_attempts = max_attempts.max(1);
    loop {
        attempt += 1;
        match op().await {
            Ok(v) => {
                breaker.record_success();
                if attempt > 1 {
                    tracing::info!(op = op_name, attempt, "DB op succeeded after retry");
                    metrics::counter!("db_retry_success_total", "op" => op_name.to_string())
                        .increment(1);
                }
                return Ok(v);
            }
            Err(e) if is_transient(&e) => {
                breaker.record_failure();
                if attempt >= max_attempts {
                    metrics::counter!("db_retry_exhausted_total", "op" => op_name.to_string())
                        .increment(1);
                    return Err(AppError::Database(e));
                }
                // If the breaker just tripped (this thread's
                // failure pushed it past the threshold, or another
                // thread already did), fail fast — do not waste
                // the full backoff sleep while the dependency is
                // known-down.
                if !breaker.allow() {
                    return Err(AppError::DependencyDown {
                        name: breaker.name(),
                    });
                }
                metrics::counter!("db_retry_attempt_total", "op" => op_name.to_string())
                    .increment(1);
                tracing::warn!(
                    op = op_name,
                    attempt,
                    max_attempts,
                    error = %e,
                    "Transient DB error, retrying"
                );
                sleep(Duration::from_millis(
                    backoff_ms.saturating_mul(attempt as u64),
                ))
                .await;
            }
            Err(e) => {
                // Non-transient (logic error). Doesn't count
                // against the breaker — the dependency answered.
                if attempt > 1 {
                    metrics::counter!("db_retry_exhausted_total", "op" => op_name.to_string())
                        .increment(1);
                }
                return Err(AppError::Database(e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn breaker_open_at_entry_fails_fast() {
        let breaker = DependencyBreaker::new("db", 1, 60);
        breaker.record_failure(); // → Open
        let res: Result<(), _> = retry_transient_with_breaker(
            &breaker,
            || async { Ok::<(), sqlx::Error>(()) },
            3,
            10,
            "test",
        )
        .await;
        assert!(matches!(res, Err(AppError::DependencyDown { name: "db" })));
    }

    #[tokio::test]
    async fn transient_failures_trip_breaker_and_then_fail_fast() {
        let breaker = DependencyBreaker::new("db", 2, 60);
        // Two transient failures should trip the breaker and the
        // helper should return DependencyDown (not Database) once
        // the threshold is crossed mid-retry.
        let calls = Cell::new(0u32);
        let res: Result<(), _> = retry_transient_with_breaker(
            &breaker,
            || {
                calls.set(calls.get() + 1);
                async { Err::<(), _>(sqlx::Error::PoolTimedOut) }
            },
            5,
            0,
            "test",
        )
        .await;
        // Threshold=2 → after 2 transient failures the breaker
        // opens; the helper's mid-retry `allow()` check returns
        // DependencyDown rather than wasting more attempts.
        assert!(matches!(res, Err(AppError::DependencyDown { name: "db" })));
        assert_eq!(calls.get(), 2, "stopped at the moment the breaker tripped");
    }

    #[tokio::test]
    async fn non_transient_error_does_not_count_against_breaker() {
        let breaker = DependencyBreaker::new("db", 1, 60);
        let res: Result<(), _> = retry_transient_with_breaker(
            &breaker,
            || async { Err::<(), _>(sqlx::Error::RowNotFound) },
            3,
            10,
            "test",
        )
        .await;
        assert!(matches!(res, Err(AppError::Database(_))));
        // The breaker MUST still be Closed — a logic error is
        // not a dependency failure.
        assert_eq!(
            breaker.state(),
            crate::resilience::BreakerState::Closed,
            "non-transient errors must not trip the breaker"
        );
    }

    #[tokio::test]
    async fn success_records_success_on_breaker() {
        let breaker = DependencyBreaker::new("db", 5, 60);
        breaker.record_failure(); // 1 (< 5, still Closed)
        let res: Result<i32, _> = retry_transient_with_breaker(
            &breaker,
            || async { Ok::<i32, sqlx::Error>(42) },
            3,
            10,
            "test",
        )
        .await;
        assert_eq!(res.unwrap(), 42);
        // Success cleared the tally — 4 more failures still won't trip.
        breaker.record_failure();
        breaker.record_failure();
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), crate::resilience::BreakerState::Closed);
    }
}
