//! Periodic background sweep of expired idempotency rows.
//!
//! Runs `cleanup_expired_idempotency_keys()` on every shard's
//! writer pool on a fixed interval. Without this the table grows
//! unbounded — the schema declared the function but nothing ever
//! invoked it.

use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use shared_kernel::db::shard::ShardRouter;

// Tighter than the legacy 600s — the function now also sweeps
// 'processing' rows older than 60s, so we tick at 30s to keep
// the replay-without-publish window bounded for crashed reserves.
const DEFAULT_INTERVAL_SECS: u64 = 30;

pub fn spawn_idempotency_cleanup(
    shards: ShardRouter,
    cancel: CancellationToken,
) -> JoinHandle<()> {
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
                        match sqlx::query_scalar::<_, i32>(
                            "SELECT cleanup_expired_idempotency_keys()",
                        )
                        .fetch_one(pool)
                        .await
                        {
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
