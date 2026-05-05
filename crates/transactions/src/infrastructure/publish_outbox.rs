//! Background drainer for `idempotency_keys.outbox_payload`.
//!
//! One tokio task per shard. Each task polls its own shard for
//! rows where `published = false`, ships each row's
//! `outbox_payload` JSONB to RabbitMQ, and flips `published =
//! true` on broker confirm. Producer-side durability is committed
//! by `SqlxIdempotencyWriter::reserve` before the HTTP handler
//! returns 202; this worker is the only thing that turns those
//! durable rows into queue messages.

use std::time::Duration;

use sqlx::FromRow;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::db::shard::ShardRouter;
use shared_kernel::queue::producer::QueueProducer;

/// Maximum number of rows shipped per iteration. The batch runs
/// inside a single PG transaction with `FOR UPDATE SKIP LOCKED`,
/// so the value caps both the publisher's per-channel lock hold
/// and how long a row's lock keeps a sibling replica from
/// claiming it.
const BATCH_SIZE: i64 = 25;

/// Sleep between iterations when the previous batch was empty.
/// Trades CPU and database load against publish latency: every
/// not-yet-published row waits at most this long before its first
/// shipment attempt.
const IDLE_TICK: Duration = Duration::from_millis(50);

/// Sleep between iterations when the previous batch errored.
/// Longer than `IDLE_TICK` so a transient broker outage does not
/// turn into a tight retry loop.
const ERROR_TICK: Duration = Duration::from_millis(500);

#[derive(FromRow)]
struct OutboxRow {
    id: Uuid,
    outbox_payload: serde_json::Value,
}

/// Outcome of one `drain_once` iteration. Distinguishes "nothing
/// to do" (sleep `IDLE_TICK`) from "broker is unhealthy" (sleep
/// `ERROR_TICK`); previously both surfaced as `Ok(0)` and a broker
/// outage degenerated into a 50 ms hot retry loop per shard.
/// The shipped-row count is already tracked by the
/// `publish_outbox_shipped_total` counter incremented inside the
/// drain loop, so the variant carries no payload.
enum DrainOutcome {
    Empty,
    Drained,
    BrokerFailed,
}

/// Spawns one drainer task per shard. The returned handles join
/// cleanly when `cancel` is fired; aborting the parent task is
/// the supported shutdown.
pub fn spawn_publish_outbox(
    shards: ShardRouter,
    queue: QueueProducer,
    cancel: CancellationToken,
) -> Vec<JoinHandle<()>> {
    (0..shards.num_shards())
        .map(|shard_idx| {
            let shards = shards.clone();
            let queue = queue.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_shard_worker(shard_idx, shards, queue, cancel).await;
            })
        })
        .collect()
}

async fn run_shard_worker(
    shard_idx: usize,
    shards: ShardRouter,
    queue: QueueProducer,
    cancel: CancellationToken,
) {
    tracing::info!(shard = shard_idx, "publish-outbox worker starting");
    loop {
        if cancel.is_cancelled() {
            break;
        }

        match drain_once(shard_idx, &shards, &queue).await {
            Ok(DrainOutcome::Drained) => {
                // Drained a non-empty batch; immediately look for
                // more so a backlog drains as fast as the broker
                // and the per-channel publish lock allow.
            }
            Ok(DrainOutcome::Empty) => {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(IDLE_TICK) => {}
                }
            }
            Ok(DrainOutcome::BrokerFailed) => {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(ERROR_TICK) => {}
                }
            }
            Err(e) => {
                tracing::warn!(
                    shard = shard_idx,
                    error = %e,
                    "publish-outbox iteration failed"
                );
                metrics::counter!(
                    "publish_outbox_iteration_errors_total",
                    "shard" => shard_idx.to_string()
                )
                .increment(1);
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(ERROR_TICK) => {}
                }
            }
        }
    }
    tracing::info!(shard = shard_idx, "publish-outbox worker exiting");
}

/// Drains up to `BATCH_SIZE` unpublished rows from this shard.
/// Returns the number of rows shipped + marked.
///
/// Runs in a single PG transaction. `FOR UPDATE SKIP LOCKED`
/// hands disjoint batches to concurrent workers (multiple app
/// replicas) so a row is only ever published by one of them. A
/// broker failure mid-batch rolls back the entire transaction:
/// no row's `published=true` mark sticks, and the rows return to
/// the pool for the next iteration. The consumer is idempotent on
/// `idempotency_key` so a re-shipment after rollback is a Replay
/// downstream.
async fn drain_once(
    shard_idx: usize,
    shards: &ShardRouter,
    queue: &QueueProducer,
) -> Result<DrainOutcome, sqlx::Error> {
    let pool = shards.writer(shard_idx);
    let mut tx = pool.begin().await?;

    let rows: Vec<OutboxRow> = sqlx::query_as(
        r#"
        SELECT id, outbox_payload
        FROM idempotency_keys
        WHERE NOT published AND outbox_payload IS NOT NULL
        ORDER BY created_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(BATCH_SIZE)
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        // Nothing to do — abort the empty tx promptly so the
        // pooled connection returns to the pool for other work.
        tx.rollback().await?;
        return Ok(DrainOutcome::Empty);
    }

    for row in rows {
        match queue.publish(&row.outbox_payload).await {
            Ok(()) => {
                sqlx::query(
                    "UPDATE idempotency_keys
                       SET published = true,
                           published_at = NOW(),
                           updated_at = NOW()
                     WHERE id = $1",
                )
                .bind(row.id)
                .execute(&mut *tx)
                .await?;
                metrics::counter!(
                    "publish_outbox_shipped_total",
                    "shard" => shard_idx.to_string()
                )
                .increment(1);
            }
            Err(e) => {
                tracing::warn!(
                    shard = shard_idx,
                    id = %row.id,
                    error = %e,
                    "publish-outbox publish failed; rolling back batch"
                );
                metrics::counter!(
                    "publish_outbox_publish_failures_total",
                    "shard" => shard_idx.to_string()
                )
                .increment(1);
                tx.rollback().await?;
                // Surface broker failure distinctly from "queue
                // empty" so the outer loop sleeps `ERROR_TICK`
                // instead of hot-looping on `IDLE_TICK`.
                return Ok(DrainOutcome::BrokerFailed);
            }
        }
    }

    tx.commit().await?;
    Ok(DrainOutcome::Drained)
}
