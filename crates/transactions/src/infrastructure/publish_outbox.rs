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

use futures::stream::{self, StreamExt};
use sqlx::FromRow;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::db::shard::ShardRouter;
use shared_kernel::queue::producer::QueueProducer;

/// Maximum number of rows shipped per iteration. Each batch is
/// claimed in a short PG transaction (lease stamp via
/// `claimed_at`), then published outside any transaction, so the
/// value no longer caps row-lock hold time. It still caps how
/// many rows one worker holds against the lease before the next
/// re-claim window opens.
const BATCH_SIZE: i64 = 25;

/// Maximum concurrent publishes per drain iteration. Matches the
/// producer's `CHANNEL_POOL_SIZE` (2 connections × 4 channels = 8)
/// so each parallel publish gets a fresh channel rather than
/// contending on the per-channel `publish_lock`.
const PUBLISH_CONCURRENCY: usize = 8;

/// Sleep between iterations when the previous batch was empty.
/// Trades CPU and database load against publish latency: every
/// not-yet-published row waits at most this long before its first
/// shipment attempt. Each idle tick is one cheap UPDATE-RETURNING
/// that returns 0 rows, so the cost of polling more frequently is
/// dominated by sqlx round-trip overhead, not PG work.
const IDLE_TICK: Duration = Duration::from_millis(10);

/// Sleep between iterations when the previous batch errored.
/// Longer than `IDLE_TICK` so a transient broker outage does not
/// turn into a tight retry loop.
const ERROR_TICK: Duration = Duration::from_millis(500);

/// Lease window after a worker stamps `claimed_at` on a batch.
/// A row that stays unpublished beyond this is re-claimable by
/// any worker (the original holder either crashed, hung on a
/// slow broker, or failed mid-batch). Sized to comfortably
/// exceed the worst-case publish budget for a full batch
/// (`BATCH_SIZE × MAX_PUBLISH_ATTEMPTS × CONFIRM_TIMEOUT_SECS`
/// ≈ 25 × 3 × 5 = 375 s) so a healthy worker never has its
/// own lease expire under it.
const LEASE_SECS: i64 = 600;

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

/// Ships up to `BATCH_SIZE` unpublished rows from this shard.
///
/// Two phases per iteration:
///
/// 1. **Claim** (one short PG transaction): pick up to `BATCH_SIZE`
///    rows that are unpublished and either never claimed or whose
///    lease has expired, stamp `claimed_at = NOW()` on them, and
///    commit. The `FOR UPDATE SKIP LOCKED` keeps two workers from
///    grabbing the same row in the same instant; the `claimed_at`
///    stamp keeps them apart for the next `LEASE_SECS` once the
///    transaction commits and the row lock is released.
///
/// 2. **Publish + mark** (no enclosing transaction): for each
///    claimed row, `queue.publish` the payload and, on broker ack,
///    flip `published = true` in a single-row UPDATE.
///
/// The PG row lock is held only for the Phase 1 transaction —
/// milliseconds — so broker latency never holds a Postgres
/// connection or row lock open. If a worker crashes or hangs
/// mid-publish, the next worker re-claims the row after
/// `LEASE_SECS`. A re-claim of a row whose publish actually
/// reached the broker results in a duplicate publish; the
/// consumer absorbs it via `ON CONFLICT (reference_id,
/// from_account)` in `bulk_claim_slots`, so the only cost is
/// extra broker traffic and a counted skip downstream.
async fn drain_once(
    shard_idx: usize,
    shards: &ShardRouter,
    queue: &QueueProducer,
) -> Result<DrainOutcome, sqlx::Error> {
    let pool = shards.writer(shard_idx);

    // Phase 1: claim a batch under a short transaction. The
    // outer UPDATE…RETURNING wraps an inner SELECT…FOR UPDATE
    // SKIP LOCKED so the lease stamp is set atomically with the
    // claim, and the row lock is released the moment the
    // statement returns.
    let rows: Vec<OutboxRow> = sqlx::query_as(
        r#"
        UPDATE idempotency_keys
           SET claimed_at = NOW(),
               updated_at = NOW()
         WHERE id IN (
             SELECT id FROM idempotency_keys
              WHERE NOT published
                AND outbox_payload IS NOT NULL
                AND (claimed_at IS NULL
                     OR claimed_at < NOW() - make_interval(secs => $2))
              ORDER BY created_at
              LIMIT $1
              FOR UPDATE SKIP LOCKED
         )
         RETURNING id, outbox_payload
        "#,
    )
    .bind(BATCH_SIZE)
    .bind(LEASE_SECS)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(DrainOutcome::Empty);
    }

    // Publish all claimed rows in parallel, then bulk-mark the
    // confirmed ones with a single UPDATE. A failed publish leaves
    // that row's `claimed_at` stamped; re-claim resumes after
    // `LEASE_SECS`. Any failure trips `BrokerFailed` so the outer
    // loop sleeps `ERROR_TICK` instead of hot-looping on `IDLE_TICK`.
    let results: Vec<(Uuid, bool)> = stream::iter(rows)
        .map(|row| {
            let queue = queue.clone();
            async move {
                let tp = row
                    .outbox_payload
                    .get("traceparent")
                    .and_then(|v| v.as_str());
                let ok = queue.publish_traced(&row.outbox_payload, tp).await.is_ok();
                (row.id, ok)
            }
        })
        .buffer_unordered(PUBLISH_CONCURRENCY)
        .collect()
        .await;

    let succeeded: Vec<Uuid> = results
        .iter()
        .filter_map(|(id, ok)| if *ok { Some(*id) } else { None })
        .collect();

    if !succeeded.is_empty() {
        sqlx::query(
            "UPDATE idempotency_keys
                SET published = true,
                    published_at = NOW(),
                    claimed_at = NULL,
                    updated_at = NOW()
              WHERE id = ANY($1)",
        )
        .bind(&succeeded)
        .execute(pool)
        .await?;
        metrics::counter!(
            "publish_outbox_shipped_total",
            "shard" => shard_idx.to_string()
        )
        .increment(succeeded.len() as u64);
    }

    if results.iter().any(|(_, ok)| !*ok) {
        tracing::warn!(
            shard = shard_idx,
            failed = results.iter().filter(|(_, ok)| !*ok).count(),
            "publish-outbox: one or more publishes failed; \
             remaining rows retry after lease expiry"
        );
        metrics::counter!(
            "publish_outbox_publish_failures_total",
            "shard" => shard_idx.to_string()
        )
        .increment(1);
        // Surface broker failure distinctly from "queue empty" so
        // the outer loop sleeps `ERROR_TICK` instead of `IDLE_TICK`.
        return Ok(DrainOutcome::BrokerFailed);
    }

    Ok(DrainOutcome::Drained)
}
