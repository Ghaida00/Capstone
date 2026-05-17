//! Redis-intake background worker — drains the Tier-2 pending list.
//!
//! Consumes one entry at a time from `idempotency:pending:s{shard}`
//! via non-blocking `RPOPLPUSH` (atomic move to `idempotency:inflight:s{shard}`),
//! GETs the full reservation from Redis master, INSERTs into PG
//! `idempotency_keys` with `claimed_at = NOW()` so the publish-outbox
//! worker leaves the row alone, publishes the outbox payload to
//! RabbitMQ, flips `published = true` (clearing the lease), and
//! removes the key from the in-flight list.
//!
//! Crash recovery: on worker start the inflight list is drained
//! first. `process_one` is idempotent across crashes for two cases:
//!   * Crash before publish — recovery sees `published = false`,
//!     publishes once.
//!   * Crash after `published = true` UPDATE — recovery sees the
//!     flag, skips publish, releases the inflight slot.
//!
//! The remaining at-least-once window — crash AFTER broker confirm
//! but BEFORE the `published = true` UPDATE — matches the
//! publish-outbox worker's own model: the next iteration re-publishes
//! the row, and the consumer's `(reference_id, from_account)` UNIQUE
//! constraint absorbs the duplicate at `bulk_claim_slots`.

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

/// Spawns one intake worker per shard. Mirrors
/// [`super::publish_outbox::spawn_publish_outbox`] in shape so
/// shutdown wiring is uniform across the bootstrap.
pub fn spawn_redis_intake(
    shards: ShardRouter,
    cache: RedisCache,
    queue: QueueProducer,
    cancel: CancellationToken,
) -> Vec<JoinHandle<()>> {
    (0..shards.num_shards())
        .map(|shard_idx| {
            let shards = shards.clone();
            let cache = cache.clone();
            let queue = queue.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                run_shard_worker(shard_idx, shards, cache, queue, cancel).await;
            })
        })
        .collect()
}

async fn run_shard_worker(
    shard_idx: usize,
    shards: ShardRouter,
    cache: RedisCache,
    queue: QueueProducer,
    cancel: CancellationToken,
) {
    tracing::info!(shard = shard_idx, "redis-intake worker starting");

    // Bind once — these strings live for the worker's lifetime.
    let pending = pending_key(shard_idx);
    let inflight = inflight_key(shard_idx);

    // First pass: drain anything left in the in-flight list from a
    // previous worker incarnation. RPOPLPUSH src=inflight,
    // dst=inflight so the entry stays claimed if processing
    // crashes again. The inflight list is usually small.
    drain_inflight(shard_idx, &shards, &cache, &queue, &inflight, &cancel).await;

    while !cancel.is_cancelled() {
        match cache.rpoplpush(&pending, &inflight).await {
            Ok(Some(idempotency_key)) => {
                if let Err(e) =
                    process_one(shard_idx, &shards, &cache, &queue, &idempotency_key).await
                {
                    tracing::warn!(
                        shard = shard_idx,
                        idempotency_key = %idempotency_key,
                        error = %e,
                        "redis-intake processing failed; key stays in inflight, will retry"
                    );
                    metrics::counter!("idempotency_redis_intake_failures_total").increment(1);
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(ERROR_BACKOFF) => {}
                    }
                }
                // Successful claim — loop immediately to drain
                // any remaining backlog.
            }
            Ok(None) => {
                // Pending list empty; sleep briefly before next
                // poll. Cancellation-aware so shutdown is prompt.
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(IDLE_TICK) => {}
                }
            }
            Err(e) => {
                tracing::warn!(
                    shard = shard_idx,
                    error = %e,
                    "redis RPOPLPUSH error; backing off"
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

/// Maximum consecutive failures tolerated during recovery before
/// giving up and letting the main loop take over. Bounds hot-loop
/// risk on a poisoned entry without abandoning the whole tail of
/// the inflight list on a single transient error (PG hiccup, Redis
/// blip), which the previous `break`-on-first-error did. The main
/// loop only services `pending`, so abandoning inflight here means
/// stranded entries until the next process restart.
const MAX_DRAIN_CONSECUTIVE_FAILURES: usize = 5;

async fn drain_inflight(
    shard_idx: usize,
    shards: &ShardRouter,
    cache: &RedisCache,
    queue: &QueueProducer,
    inflight: &str,
    cancel: &CancellationToken,
) {
    let mut consecutive_failures: usize = 0;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // RPOPLPUSH inflight → inflight cycles entries through
        // the same list while we work. Non-blocking: returns None
        // on empty so recovery terminates naturally.
        let res = cache.rpoplpush(inflight, inflight).await;
        match res {
            Ok(Some(idempotency_key)) => {
                match process_one(shard_idx, shards, cache, queue, &idempotency_key).await {
                    Ok(()) => {
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        tracing::warn!(
                            shard = shard_idx,
                            idempotency_key = %idempotency_key,
                            error = %e,
                            failures = consecutive_failures,
                            "redis-intake recovery: process_one failed, entry stays in inflight"
                        );
                        metrics::counter!("idempotency_redis_intake_failures_total").increment(1);
                        if consecutive_failures >= MAX_DRAIN_CONSECUTIVE_FAILURES {
                            tracing::error!(
                                shard = shard_idx,
                                "redis-intake recovery: too many consecutive failures, \
                                 exiting drain; remaining inflight entries will be picked \
                                 up on next process restart"
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
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(
                    shard = shard_idx,
                    error = %e,
                    "redis-intake recovery: RPOPLPUSH error"
                );
                break;
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

async fn process_one(
    shard_idx: usize,
    shards: &ShardRouter,
    cache: &RedisCache,
    queue: &QueueProducer,
    idempotency_key: &str,
) -> Result<(), String> {
    // Fetch the full reservation from Redis master.
    let raw = cache
        .get_master_raw(&entry_key(idempotency_key))
        .await
        .map_err(|e| format!("redis GET: {}", e))?;

    let entry: StoredEntry = match raw {
        Some(s) => {
            serde_json::from_str(&s).map_err(|e| format!("deserialize reservation: {}", e))?
        }
        None => {
            // Reservation TTL'd between SETNX and intake. Drop the
            // pending-list entry; nothing to publish.
            tracing::debug!(
                shard = shard_idx,
                idempotency_key = %idempotency_key,
                "redis-intake: reservation key missing (TTL expired or deleted), discarding"
            );
            cache
                .lrem(&inflight_key(shard_idx), 1, idempotency_key)
                .await
                .map_err(|e| format!("redis LREM: {}", e))?;
            return Ok(());
        }
    };

    // 1. Insert the PG row with `claimed_at = NOW()` so the
    //    publish_outbox worker treats this row as leased and skips
    //    it on its 10ms IDLE_TICK. Without the lease stamp, the
    //    outbox worker would race us during the publish-await
    //    window and republish every row at-least-twice. ON CONFLICT
    //    DO NOTHING makes recovery retries idempotent: if the row
    //    already exists (previous attempt crashed), the next step
    //    consults its `published` flag instead of double-publishing.
    let pool = shards.writer(shard_idx);
    sqlx::query(
        r#"
        INSERT INTO idempotency_keys
          (idempotency_key, request_hash, status,
           response_payload, outbox_payload,
           expires_at, published, claimed_at)
        VALUES ($1, $2, 'processing', $3, $4,
                NOW() + INTERVAL '24 hours', false, NOW())
        ON CONFLICT (idempotency_key) DO NOTHING
        "#,
    )
    .bind(idempotency_key)
    .bind(&entry.request_hash)
    .bind(&entry.accepted_payload)
    .bind(&entry.outbox_payload)
    .execute(pool)
    .await
    .map_err(|e| format!("PG INSERT: {}", e))?;

    // 2. Read the row's current `published` flag. On crash recovery
    //    a previous attempt may have already published and flipped
    //    the flag before crashing pre-LREM; in that case we skip
    //    publish entirely and just release the inflight slot.
    //    The remaining duplicate-publish window — crash AFTER broker
    //    confirm but BEFORE the UPDATE in step 4 — is absorbed by
    //    the consumer's `(reference_id, from_account)` UNIQUE.
    let already_published: bool = sqlx::query_scalar(
        "SELECT published FROM idempotency_keys WHERE idempotency_key = $1",
    )
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("PG SELECT published: {}", e))?
    .unwrap_or(false);

    if already_published {
        cache
            .lrem(&inflight_key(shard_idx), 1, idempotency_key)
            .await
            .map_err(|e| format!("redis LREM: {}", e))?;
        return Ok(());
    }

    // 3. Publish to broker. We hold the lease (`claimed_at = NOW()`),
    //    so publish_outbox stays out of our way until the lease
    //    expires. On failure, clear the lease so publish_outbox can
    //    pick the row up on its next IDLE_TICK rather than waiting
    //    `LEASE_SECS` for our stamp to age out — the PG row is
    //    durable, only the broker round-trip is missing.
    let tp = entry
        .outbox_payload
        .get("traceparent")
        .and_then(|v| v.as_str());
    if let Err(e) = queue.publish_traced(&entry.outbox_payload, tp).await {
        tracing::warn!(
            shard = shard_idx,
            idempotency_key = %idempotency_key,
            error = %e,
            "redis-intake: publish failed; clearing lease so publish_outbox retries"
        );
        metrics::counter!("idempotency_redis_intake_publish_failures_total").increment(1);
        let _ = sqlx::query(
            "UPDATE idempotency_keys
               SET claimed_at = NULL,
                   updated_at = NOW()
             WHERE idempotency_key = $1
               AND NOT published",
        )
        .bind(idempotency_key)
        .execute(pool)
        .await;
    } else {
        // 4. Mark published and clear the lease so publish_outbox
        //    never re-claims this row.
        sqlx::query(
            "UPDATE idempotency_keys
               SET published = true,
                   published_at = NOW(),
                   claimed_at = NULL,
                   updated_at = NOW()
             WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .execute(pool)
        .await
        .map_err(|e| format!("PG UPDATE published: {}", e))?;
        metrics::counter!("idempotency_redis_intake_published_total").increment(1);
    }

    // 5. Release the inflight slot. LREM count=1 removes the first
    //    matching occurrence (we only ever push one per claim).
    cache
        .lrem(&inflight_key(shard_idx), 1, idempotency_key)
        .await
        .map_err(|e| format!("redis LREM: {}", e))?;

    Ok(())
}
