//! Retry helpers for transient DB failures (connection reset during
//! pgBouncer restart, brief primary unavailability during promotion, etc.).
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
                sleep(Duration::from_millis(backoff_ms.saturating_mul(attempt as u64))).await;
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
