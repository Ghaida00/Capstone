//! Redis-intake background worker — batched drain of the Tier-2
//! pending list.
//!
//! Each worker iteration claims up to `batch_size` keys from
//! `idempotency:pending:s{shard}` via pipelined `RPOPLPUSH` to
//! `idempotency:inflight:s{shard}`, then processes the batch as a
//! unit: `MGET` the reservations, bulk `INSERT ... ON CONFLICT DO
//! NOTHING RETURNING`, parallel publishes via `buffer_unordered`,
//! bulk `UPDATE`, pipelined `LREM`. ~5 round-trips per batch + N
//! parallel publishes — amortising fixed latency over N messages.
//!
//! Money-safety invariants:
//!   * Atomic claim — each `RPOPLPUSH` moves one key
//!     `pending → inflight` atomically. A crash mid-batch leaves
//!     all claimed keys in `inflight`; `drain_inflight_batched`
//!     reprocesses them on restart.
//!   * Idempotent reprocessing — `ON CONFLICT DO NOTHING` skips
//!     already-inserted rows; the `published` check skips rows a
//!     prior attempt finished.
//!   * At-least-once — a crash after broker-confirm but before the
//!     `published = true` UPDATE causes a republish on recovery;
//!     the consumer's `(reference_id, from_account)` UNIQUE
//!     absorbs the duplicate.
//!   * Lease hand-off — a publish failure clears `claimed_at` so
//!     the durable PG row is picked up by the publish_outbox
//!     backstop on its next iteration.

use std::time::Duration;

use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;
use shared_kernel::queue::producer::QueueProducer;

use super::redis_idempotency::{entry_key, inflight_key, pending_key};

/// Sleep between non-blocking `RPOPLPUSH` polls when the pending
/// list is empty. Trades CPU + Redis QPS against worst-case
/// claim-to-publish latency: every reservation waits at most
/// `IDLE_TICK + publish round-trip` before reaching the broker.
/// Mirrors the publish-outbox worker's idle cadence.
const IDLE_TICK: Duration = Duration::from_millis(10);

/// Sleep when an iteration errored (Redis or PG transient). Avoids
/// hammering a degraded dependency. Successful iterations loop
/// immediately.
const ERROR_BACKOFF: Duration = Duration::from_millis(500);

/// Maximum consecutive failures tolerated during recovery before
/// giving up and letting the main loop take over. Bounds hot-loop
/// risk on a poisoned entry without abandoning the whole tail of
/// the inflight list on a single transient error (PG hiccup, Redis
/// blip). The main loop only services `pending`, so abandoning
/// inflight here means stranded entries until the next process
/// restart.
const MAX_DRAIN_CONSECUTIVE_FAILURES: usize = 5;

/// How many publishes a batched `process_batch` runs in parallel.
/// Matches the producer's `CHANNEL_POOL_SIZE` (2 conn × 8 ch = 16)
/// so the batch's publishes saturate the channel pool without
/// queueing on per-channel `publish_lock`.
const PUBLISH_CONCURRENCY: usize = 16;

/// Spawns `num_shards × concurrency` batched redis-intake workers.
/// Each worker claims up to `batch_size` reservation keys per
/// iteration and processes them via `process_batch` — amortizing
/// fixed round-trip latency over the batch and parallelizing the
/// publishes across the producer channel pool.
pub fn spawn_redis_intake(
    shards: ShardRouter,
    cache: RedisCache,
    queue: QueueProducer,
    cancel: CancellationToken,
    concurrency: usize,
    batch_size: usize,
) -> Vec<JoinHandle<()>> {
    let concurrency = concurrency.max(1);
    let batch_size = batch_size.max(1);
    let mut handles = Vec::with_capacity(shards.num_shards() * concurrency);
    for shard_idx in 0..shards.num_shards() {
        for _ in 0..concurrency {
            let shards = shards.clone();
            let cache = cache.clone();
            let queue = queue.clone();
            let cancel = cancel.clone();
            handles.push(tokio::spawn(async move {
                run_shard_worker(shard_idx, shards, cache, queue, cancel, batch_size).await;
            }));
        }
    }
    handles
}

async fn run_shard_worker(
    shard_idx: usize,
    shards: ShardRouter,
    cache: RedisCache,
    queue: QueueProducer,
    cancel: CancellationToken,
    batch_size: usize,
) {
    tracing::info!(
        shard = shard_idx,
        batch_size,
        "redis-intake worker starting"
    );

    let pending = pending_key(shard_idx);
    let inflight = inflight_key(shard_idx);

    // Crash recovery: drain any inflight entries left from a previous
    // process incarnation BEFORE accepting new claims, so a redelivery
    // never overtakes an in-flight one.
    drain_inflight_batched(
        shard_idx, &shards, &cache, &queue, &inflight, batch_size, &cancel,
    )
    .await;

    while !cancel.is_cancelled() {
        match cache.rpoplpush_batch(&pending, &inflight, batch_size).await {
            Ok(keys) if keys.is_empty() => {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(IDLE_TICK) => {}
                }
            }
            Ok(keys) => {
                if let Err(e) = process_batch(shard_idx, &shards, &cache, &queue, keys).await {
                    tracing::warn!(
                        shard = shard_idx,
                        error = %e,
                        "redis-intake batch processing failed; keys stay in inflight"
                    );
                    metrics::counter!("idempotency_redis_intake_failures_total").increment(1);
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(ERROR_BACKOFF) => {}
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    shard = shard_idx,
                    error = %e,
                    "redis batch-claim error; backing off"
                );
                metrics::counter!("idempotency_redis_intake_errors_total").increment(1);
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(ERROR_BACKOFF) => {}
                }
            }
        }
    }
    tracing::info!(shard = shard_idx, "redis-intake worker exiting");
}

async fn drain_inflight_batched(
    shard_idx: usize,
    shards: &ShardRouter,
    cache: &RedisCache,
    queue: &QueueProducer,
    inflight: &str,
    batch_size: usize,
    cancel: &CancellationToken,
) {
    let mut consecutive_failures: usize = 0;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // Cycle: RPOPLPUSH inflight -> inflight pulls a batch through
        // the same list; process_batch's final LREM cleans up the
        // ones it handled. Anything that errored stays in inflight
        // for the next cycle.
        let claimed = match cache.rpoplpush_batch(inflight, inflight, batch_size).await {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(
                    shard = shard_idx,
                    error = %e,
                    "drain_inflight RPOPLPUSH error"
                );
                break;
            }
        };
        if claimed.is_empty() {
            break;
        }
        match process_batch(shard_idx, shards, cache, queue, claimed).await {
            Ok(()) => {
                consecutive_failures = 0;
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::warn!(
                    shard = shard_idx,
                    error = %e,
                    failures = consecutive_failures,
                    "drain_inflight: process_batch failed"
                );
                metrics::counter!("idempotency_redis_intake_failures_total").increment(1);
                if consecutive_failures >= MAX_DRAIN_CONSECUTIVE_FAILURES {
                    tracing::error!(
                        shard = shard_idx,
                        "drain_inflight: too many consecutive failures, exiting"
                    );
                    break;
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(ERROR_BACKOFF) => {}
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct StoredEntry {
    request_hash: String,
    accepted_payload: serde_json::Value,
    outbox_payload: serde_json::Value,
}

/// Process one batch of already-claimed reservation keys.
///
/// Pipeline (5 round-trips + N parallel publishes per batch):
///   1. MGET the N reservation entries from the master pool.
///   2. Bulk INSERT into `idempotency_keys` with
///      `ON CONFLICT DO NOTHING RETURNING idempotency_key` — the
///      returned set is the freshly-claimed subset; the rest are
///      pre-existing rows (crash recovery).
///   3. For the conflict subset (usually empty), bulk SELECT the
///      `published` flag; rows with `published=true` skip the
///      publish step (a previous attempt already shipped them).
///   4. Publish the remaining outbox payloads via
///      `buffer_unordered(PUBLISH_CONCURRENCY)`.
///   5. Bulk UPDATE: succeeded keys -> `published=true`,
///      `claimed_at=NULL`; failed keys -> just `claimed_at=NULL`
///      so `publish_outbox` retries from the durable PG row.
///   6. Pipelined LREM all claimed keys from the inflight list.
///
/// Money-safety: every claimed key is either published exactly
/// once (UNIQUE constraint downstream absorbs duplicate publishes
/// from the at-least-once window) OR handed off to `publish_outbox`
/// via the cleared lease. A crash anywhere leaves the keys in
/// `inflight`; restart's batched `drain_inflight_batched`
/// reprocesses them idempotently.
async fn process_batch(
    shard_idx: usize,
    shards: &ShardRouter,
    cache: &RedisCache,
    queue: &QueueProducer,
    claimed_keys: Vec<String>,
) -> Result<(), String> {
    use futures::stream::{self, StreamExt};
    use std::collections::HashSet;

    if claimed_keys.is_empty() {
        return Ok(());
    }

    // ── 1. MGET the entries ──────────────────────────────────────
    let entry_keys: Vec<String> = claimed_keys.iter().map(|k| entry_key(k)).collect();
    let raws = cache
        .mget_master_raw(&entry_keys)
        .await
        .map_err(|e| format!("redis MGET: {}", e))?;

    // Partition: keys whose entry deserialised OK vs missing/garbled.
    // Missing entries are TTL-expired or deleted reservations — they
    // can never be published; we LREM them from inflight and drop.
    let mut to_process: Vec<(String, StoredEntry)> = Vec::with_capacity(claimed_keys.len());
    for (k, raw) in claimed_keys.iter().zip(raws.iter()) {
        match raw {
            Some(s) => match serde_json::from_str::<StoredEntry>(s) {
                Ok(entry) => to_process.push((k.clone(), entry)),
                Err(e) => {
                    tracing::error!(
                        shard = shard_idx,
                        idempotency_key = %k,
                        error = %e,
                        "redis-intake: deserialise reservation, dropping from inflight"
                    );
                }
            },
            None => {
                tracing::debug!(
                    shard = shard_idx,
                    idempotency_key = %k,
                    "redis-intake: reservation TTL'd, dropping from inflight"
                );
            }
        }
    }

    // ── 2. Bulk INSERT (with RETURNING) ──────────────────────────
    let pool = shards.writer(shard_idx);
    let freshly_inserted: Vec<String> = if to_process.is_empty() {
        Vec::new()
    } else {
        let n = to_process.len();
        let mut ids: Vec<String> = Vec::with_capacity(n);
        let mut hashes: Vec<String> = Vec::with_capacity(n);
        let mut accepts: Vec<serde_json::Value> = Vec::with_capacity(n);
        let mut outboxes: Vec<serde_json::Value> = Vec::with_capacity(n);
        for (k, e) in &to_process {
            ids.push(k.clone());
            hashes.push(e.request_hash.clone());
            accepts.push(e.accepted_payload.clone());
            outboxes.push(e.outbox_payload.clone());
        }
        sqlx::query_scalar::<_, String>(
            r#"
            INSERT INTO idempotency_keys
              (idempotency_key, request_hash, status,
               response_payload, outbox_payload,
               expires_at, published, claimed_at)
            SELECT k, h, 'processing', a, o,
                   NOW() + INTERVAL '24 hours', false, NOW()
            FROM unnest($1::text[], $2::text[], $3::jsonb[], $4::jsonb[])
                 AS t(k, h, a, o)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING idempotency_key
            "#,
        )
        .bind(&ids)
        .bind(&hashes)
        .bind(&accepts)
        .bind(&outboxes)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("PG bulk INSERT: {}", e))?
    };

    // ── 3. Conflict subset: bulk SELECT published ────────────────
    let freshly_set: HashSet<&String> = freshly_inserted.iter().collect();
    let conflict_keys: Vec<String> = to_process
        .iter()
        .filter(|(k, _)| !freshly_set.contains(k))
        .map(|(k, _)| k.clone())
        .collect();
    let mut already_published: HashSet<String> = HashSet::new();
    if !conflict_keys.is_empty() {
        let rows: Vec<(String, bool)> = sqlx::query_as(
            "SELECT idempotency_key, published
             FROM idempotency_keys
             WHERE idempotency_key = ANY($1)",
        )
        .bind(&conflict_keys)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("PG bulk SELECT published: {}", e))?;
        for (k, p) in rows {
            if p {
                already_published.insert(k);
            }
        }
    }

    // ── 4. Publish (parallel across channel pool) ────────────────
    let to_publish: Vec<(String, serde_json::Value)> = to_process
        .iter()
        .filter(|(k, _)| !already_published.contains(k))
        .map(|(k, e)| (k.clone(), e.outbox_payload.clone()))
        .collect();

    let publish_results: Vec<(String, bool)> = stream::iter(to_publish)
        .map(|(k, payload)| {
            let queue = queue.clone();
            async move {
                let tp = payload.get("traceparent").and_then(|v| v.as_str());
                let ok = queue.publish_traced(&payload, tp).await.is_ok();
                (k, ok)
            }
        })
        .buffer_unordered(PUBLISH_CONCURRENCY)
        .collect()
        .await;

    let succeeded: Vec<String> = publish_results
        .iter()
        .filter_map(|(k, ok)| if *ok { Some(k.clone()) } else { None })
        .collect();
    let failed: Vec<String> = publish_results
        .iter()
        .filter_map(|(k, ok)| if !*ok { Some(k.clone()) } else { None })
        .collect();

    // ── 5a. Mark succeeded: published=true, lease cleared ────────
    if !succeeded.is_empty() {
        sqlx::query(
            "UPDATE idempotency_keys
               SET published = true,
                   published_at = NOW(),
                   claimed_at = NULL,
                   updated_at = NOW()
             WHERE idempotency_key = ANY($1)",
        )
        .bind(&succeeded)
        .execute(pool)
        .await
        .map_err(|e| format!("PG bulk UPDATE succeeded: {}", e))?;
        metrics::counter!("idempotency_redis_intake_published_total")
            .increment(succeeded.len() as u64);
    }

    // ── 5b. Failed publishes: clear lease so publish_outbox retries ─
    if !failed.is_empty() {
        sqlx::query(
            "UPDATE idempotency_keys
               SET claimed_at = NULL,
                   updated_at = NOW()
             WHERE idempotency_key = ANY($1)
               AND NOT published",
        )
        .bind(&failed)
        .execute(pool)
        .await
        .map_err(|e| format!("PG bulk UPDATE failed: {}", e))?;
        metrics::counter!("idempotency_redis_intake_publish_failures_total")
            .increment(failed.len() as u64);
    }

    // ── 6. LREM every claimed key from inflight ──────────────────
    // succeeded ∪ failed ∪ already_published ∪ dropped (missing/garbled)
    // = claimed_keys (the original list).
    let _ = cache
        .lrem_batch(&inflight_key(shard_idx), &claimed_keys)
        .await
        .map_err(|e| format!("redis bulk LREM: {}", e))?;

    Ok(())
}
