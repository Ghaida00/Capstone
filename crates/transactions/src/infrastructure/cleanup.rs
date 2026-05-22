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

/// Tick cadence for the cleanup sweep. The PG function only
/// removes rows past `expires_at + 1 hour`, so a longer interval
/// delays compaction without changing what gets removed; 30 s
/// keeps the `idempotency_cleanup_deleted_total` metric prompt
/// without measurable load.
const DEFAULT_INTERVAL_SECS: u64 = 30;

/// Per-sweep `statement_timeout` override. Bounded so a runaway sweep
/// cannot hold a writer-pool slot indefinitely, but generous enough
/// for tens of millions of rows.
const SWEEP_STATEMENT_TIMEOUT: &str = "60s";

async fn run_sweep(pool: &PgPool) -> Result<i32, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = '{}'",
        SWEEP_STATEMENT_TIMEOUT
    ))
    .execute(&mut *tx)
    .await?;
    let deleted: i32 = sqlx::query_scalar("SELECT cleanup_expired_idempotency_keys()")
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(deleted)
}

pub fn spawn_idempotency_cleanup(shards: ShardRouter, cancel: CancellationToken) -> JoinHandle<()> {
    let interval = Duration::from_secs(DEFAULT_INTERVAL_SECS);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    for shard_idx in 0..shards.num_shards() {
                        let pool = shards.writer(shard_idx);
                        match run_sweep(pool).await {
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
