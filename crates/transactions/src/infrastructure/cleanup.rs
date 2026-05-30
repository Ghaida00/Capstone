//! Periodic background sweep of expired idempotency rows.
//!
//! Runs `cleanup_expired_idempotency_keys()` on every shard's
//! writer pool on a fixed interval. Without this the table grows
//! unbounded.
//!
//! The sweep is wrapped in a transaction that issues `SET LOCAL
//! statement_timeout = '60s'` so the bulk `DELETE` is not cancelled
//! by the database-level 2 s `statement_timeout` (set per shard via
//! `ALTER DATABASE` for the hot pooled write path). `SET LOCAL`
//! resets at `COMMIT`, so it does not leak to other transactions on
//! the same pgBouncer-pooled backend connection.

use sqlx::PgPool;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use shared_kernel::db::shard::ShardRouter;

/// Tick cadence for the cleanup sweep. Default 30 s; overridable
/// via `IDEMPOTENCY_CLEANUP_INTERVAL_SECS`.
const DEFAULT_INTERVAL_SECS: u64 = 30;

/// How long to keep a row after the consumer marked it
/// `published = true`. After this window the row is eligible for
/// deletion regardless of the 24 h replay TTL — the published
/// path means the consumer already emitted the event and the
/// `transactions` row exists as the authoritative record.
///
/// Default 300 s (5 min); overridable via
/// `IDEMPOTENCY_PUBLISHED_GRACE_SECS`.
const DEFAULT_PUBLISHED_GRACE_SECS: i32 = 300;

/// Pure parsing helper for `IDEMPOTENCY_PUBLISHED_GRACE_SECS`.
/// Lives separate from the env-reading wrapper so the parse,
/// default, and clamp logic is unit-testable without touching
/// process-global env state.
fn parse_published_grace_secs(raw: Option<&str>) -> i32 {
    raw.and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(DEFAULT_PUBLISHED_GRACE_SECS)
        .clamp(60, 86_400)
}

/// Pure parsing helper for `IDEMPOTENCY_CLEANUP_INTERVAL_SECS`.
/// Mirrors `parse_published_grace_secs` but on `u64` with the
/// 5 s–1 h range that makes sense for a sweep cadence.
fn parse_cleanup_interval_secs(raw: Option<&str>) -> u64 {
    raw.and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INTERVAL_SECS)
        .clamp(5, 3_600)
}

/// Per-sweep `statement_timeout` override. Bounded so a runaway sweep
/// cannot hold a writer-pool slot indefinitely, but generous enough
/// for tens of millions of rows.
const SWEEP_STATEMENT_TIMEOUT: &str = "60s";

async fn run_sweep(pool: &PgPool, published_grace_secs: i32) -> Result<i32, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = '{}'",
        SWEEP_STATEMENT_TIMEOUT
    ))
    .execute(&mut *tx)
    .await?;
    let deleted: i32 = sqlx::query_scalar("SELECT cleanup_expired_idempotency_keys($1)")
        .bind(published_grace_secs)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(deleted)
}

pub fn spawn_idempotency_cleanup(shards: ShardRouter, cancel: CancellationToken) -> JoinHandle<()> {
    let interval_secs = parse_cleanup_interval_secs(
        std::env::var("IDEMPOTENCY_CLEANUP_INTERVAL_SECS").ok().as_deref(),
    );
    let grace_secs = parse_published_grace_secs(
        std::env::var("IDEMPOTENCY_PUBLISHED_GRACE_SECS").ok().as_deref(),
    );
    tracing::info!(
        interval_secs,
        published_grace_secs = grace_secs,
        "idempotency cleanup task starting"
    );
    let interval = Duration::from_secs(interval_secs);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    for shard_idx in 0..shards.num_shards() {
                        let pool = shards.writer(shard_idx);
                        match run_sweep(pool, grace_secs).await {
                            Ok(deleted) => {
                                if deleted > 0 {
                                    tracing::info!(
                                        shard = shard_idx,
                                        deleted,
                                        "idempotency cleanup swept rows"
                                    );
                                }
                                metrics::counter!(
                                    "idempotency_cleanup_deleted_total",
                                    "shard" => shard_idx.to_string()
                                )
                                .increment(deleted.max(0) as u64);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    shard = shard_idx,
                                    error = %e,
                                    "idempotency cleanup failed"
                                );
                            }
                        }
                    }
                }
            }
        }
        tracing::info!("idempotency cleanup task exiting");
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_published_grace_secs_returns_default_when_unset() {
        assert_eq!(parse_published_grace_secs(None), DEFAULT_PUBLISHED_GRACE_SECS);
    }

    #[test]
    fn parse_published_grace_secs_parses_valid_value() {
        assert_eq!(parse_published_grace_secs(Some("600")), 600);
        assert_eq!(parse_published_grace_secs(Some("1800")), 1800);
    }

    #[test]
    fn parse_published_grace_secs_clamps_out_of_range() {
        // Below minimum (60 s): clamp up.
        assert_eq!(parse_published_grace_secs(Some("10")), 60);
        // Above maximum (86_400 s = 1 day): clamp down.
        assert_eq!(parse_published_grace_secs(Some("999999")), 86_400);
    }

    #[test]
    fn parse_published_grace_secs_falls_back_to_default_on_garbage() {
        // Operator typo (non-numeric, empty, negative-with-suffix):
        // ignore and fall back to default rather than panic or zero.
        assert_eq!(parse_published_grace_secs(Some("not-a-number")), DEFAULT_PUBLISHED_GRACE_SECS);
        assert_eq!(parse_published_grace_secs(Some("")), DEFAULT_PUBLISHED_GRACE_SECS);
    }

    // ── parse_cleanup_interval_secs — same shape, different bounds ──

    #[test]
    fn parse_cleanup_interval_secs_returns_default_when_unset() {
        assert_eq!(parse_cleanup_interval_secs(None), DEFAULT_INTERVAL_SECS);
    }

    #[test]
    fn parse_cleanup_interval_secs_parses_valid_value() {
        assert_eq!(parse_cleanup_interval_secs(Some("60")), 60);
        assert_eq!(parse_cleanup_interval_secs(Some("5")), 5);
    }

    #[test]
    fn parse_cleanup_interval_secs_clamps_out_of_range() {
        // Below minimum (5 s): clamp up.
        assert_eq!(parse_cleanup_interval_secs(Some("1")), 5);
        // Above maximum (3600 s = 1 h): clamp down.
        assert_eq!(parse_cleanup_interval_secs(Some("99999")), 3600);
    }

    #[test]
    fn parse_cleanup_interval_secs_falls_back_to_default_on_garbage() {
        assert_eq!(parse_cleanup_interval_secs(Some("bad")), DEFAULT_INTERVAL_SECS);
        assert_eq!(parse_cleanup_interval_secs(Some("")), DEFAULT_INTERVAL_SECS);
    }
}
