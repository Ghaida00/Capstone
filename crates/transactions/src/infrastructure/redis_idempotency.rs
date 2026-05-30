//! Tier-2 Redis-async idempotency reservation.
//!
//! `RedisIdempotencyWriter` commits the reservation entirely in
//! Redis on the request path: one `SET key value NX EX ttl` plus one
//! `LPUSH` onto the per-shard pending list. The Postgres INSERT and
//! broker publish are deferred to `RedisIntakeWorker` (see
//! [`super::redis_intake`]).
//!
//! `HybridIdempotencyWriter` wraps the Redis path with the existing
//! `SqlxIdempotencyWriter` as a fallback: if the Redis call fails
//! (timeout, primary-loss window during Sentinel failover), the
//! reservation is committed to PG instead so the request still
//! gets a 2xx response. After a PG fallback succeeds, a best-effort
//! SETNX populates the Redis-namespaced key (without LPUSH'ing onto
//! the pending list — the PG path's `publish_outbox` worker handles
//! publishing for fallback rows). This keeps a future Redis-path
//! retry of the same idempotency_key on the conflict probe instead
//! of treating the key as fresh and routing it through the intake
//! worker, which would cost a duplicate broker publish that the
//! consumer's `(reference_id, from_account)` UNIQUE then has to
//! absorb. Increments `idempotency_redis_fallback_total` for
//! observability.
//!
//! ## Durability story by mode
//!
//! `pg`: PG INSERT is committed before the handler returns 2xx.
//! Survives any single-process failure.
//!
//! `redis`: only Redis SETNX + LPUSH are committed before 2xx.
//! Durability is bounded by Redis AOF (`appendfsync everysec` =
//! up to 1s of accepted writes lost on master crash) and by
//! Sentinel-failover replication lag. A reservation that's
//! returned `Reserved` to the client but later lost during an
//! unreplicated master-crash failover IS lost — there is no
//! compensation path, the user's idempotency_key has no PG row
//! to poll, and a retry will be treated as a fresh request.
//!
//! `hybrid`: same as `redis` for the success path. The PG fallback
//! ONLY engages when the Redis call returns `Err` synchronously —
//! a Redis SETNX that succeeds and is later lost during failover
//! does NOT trigger fallback.
//!
//! Wire shape stored at `v1:idemp:{idempotency_key}`:
//!
//! ```json
//! { "request_hash": "...",
//!   "accepted_payload": { ... },
//!   "outbox_payload":   { ... },
//!   "shard": 0 }
//! ```
//!
//! The pending list `idempotency:pending:s{shard}` carries only the
//! `idempotency_key` strings; the intake worker GETs the full entry
//! by key from the master pool to avoid replication-lag staleness.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use shared_kernel::cache::redis::{RedisCache, CACHE_KEY_VERSION};

use crate::domain::{IdempotencyAwareWriter, RepoError, ReserveOutcome};

use super::repository::SqlxIdempotencyWriter;

/// Mirrors the PG path's 24 h reservation TTL — replays after that
/// land as fresh requests.
const TTL_SECS: u64 = 86_400;

/// Wire shape persisted at `idempotency:{idempotency_key}`. Carries
/// everything the intake worker needs to land the row in PG and
/// publish it to RabbitMQ, plus the request_hash for replay
/// detection on subsequent reservations of the same key.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RedisReservation {
    pub request_hash: String,
    pub accepted_payload: serde_json::Value,
    pub outbox_payload: serde_json::Value,
    pub shard: usize,
}

/// Versioned Redis key for a single reservation.
pub(crate) fn entry_key(idempotency_key: &str) -> String {
    format!("{}:idemp:{}", CACHE_KEY_VERSION, idempotency_key)
}

/// Pending-list key the intake worker drains for `shard`.
pub(crate) fn pending_key(shard: usize) -> String {
    format!("{}:idemp_pending:s{}", CACHE_KEY_VERSION, shard)
}

/// In-flight-list key. `RPOPLPUSH pending → inflight` makes claim
/// atomic; on worker crash, restart drains `inflight` first so no
/// reservation is dropped between Redis SETNX and PG commit.
pub(crate) fn inflight_key(shard: usize) -> String {
    format!("{}:idemp_inflight:s{}", CACHE_KEY_VERSION, shard)
}

pub(crate) struct RedisIdempotencyWriter {
    cache: RedisCache,
}

impl RedisIdempotencyWriter {
    pub(crate) fn new(cache: RedisCache) -> Self {
        Self { cache }
    }

    /// Best-effort SETNX of the Redis-namespaced entry without
    /// pushing to the pending list. Used by `HybridIdempotencyWriter`
    /// after a successful PG fallback so a future Redis-path retry
    /// of the same `idempotency_key` finds the entry on its conflict
    /// probe (returning Replay or HashConflict) instead of declaring
    /// the key fresh and routing it through the intake worker for a
    /// duplicate publish.
    ///
    /// No LPUSH onto pending: the PG path's `publish_outbox` worker
    /// owns publishing for fallback rows. Errors are intentionally
    /// swallowed — if Redis is still down (the reason we fell back),
    /// the next retry will fall back again and the PG path's own
    /// conflict probe handles replay correctly.
    pub(crate) async fn populate_post_fallback(
        &self,
        shard: usize,
        idempotency_key: &str,
        request_hash: &str,
        accepted_payload: &serde_json::Value,
        outbox_payload: &serde_json::Value,
    ) {
        let entry = RedisReservation {
            request_hash: request_hash.to_string(),
            accepted_payload: accepted_payload.clone(),
            outbox_payload: outbox_payload.clone(),
            shard,
        };
        let key = entry_key(idempotency_key);
        let _ = self.cache.set_nx_ex(&key, &entry, TTL_SECS).await;
    }
}

#[async_trait]
impl IdempotencyAwareWriter for RedisIdempotencyWriter {
    async fn reserve(
        &self,
        shard: usize,
        idempotency_key: &str,
        request_hash: &str,
        accepted_payload: &serde_json::Value,
        outbox_payload: &serde_json::Value,
    ) -> Result<ReserveOutcome, RepoError> {
        let entry = RedisReservation {
            request_hash: request_hash.to_string(),
            accepted_payload: accepted_payload.clone(),
            outbox_payload: outbox_payload.clone(),
            shard,
        };
        let key = entry_key(idempotency_key);

        let inserted = self
            .cache
            .set_nx_ex(&key, &entry, TTL_SECS)
            .await
            .map_err(|e| RepoError::Other(format!("redis SET NX: {}", e)))?;

        if inserted {
            // Push onto the per-shard pending list so the intake
            // worker can drain it. `LPUSH` returns the new length;
            // we don't care about it.
            self.cache
                .lpush(&pending_key(shard), idempotency_key)
                .await
                .map_err(|e| RepoError::Other(format!("redis LPUSH: {}", e)))?;
            metrics::counter!("idempotency_redis_reserved_total").increment(1);
            return Ok(ReserveOutcome::Reserved);
        }

        // Conflict path: GET the existing entry from the master pool
        // (no replication lag). Match request_hash to decide replay
        // vs payload-conflict.
        let raw = self
            .cache
            .get_master_raw(&key)
            .await
            .map_err(|e| RepoError::Other(format!("redis GET: {}", e)))?;

        match raw {
            Some(s) => {
                let cached: RedisReservation = serde_json::from_str(&s)
                    .map_err(|e| RepoError::Other(format!("deserialize reservation: {}", e)))?;
                if cached.request_hash == request_hash {
                    metrics::counter!("idempotency_redis_replay_total").increment(1);
                    Ok(ReserveOutcome::Replay(cached.accepted_payload))
                } else {
                    metrics::counter!("idempotency_redis_hash_conflict_total").increment(1);
                    Ok(ReserveOutcome::HashConflict)
                }
            }
            // SETNX said duplicate but GET returned None — TTL
            // expired between the two ops, or the entry was deleted
            // by the intake worker (which doesn't delete entries
            // today, so this is the TTL race). Surface as
            // HashConflict to be conservative: the caller sees a
            // 4xx instead of silently re-publishing.
            None => {
                metrics::counter!("idempotency_redis_ttl_race_total").increment(1);
                Ok(ReserveOutcome::HashConflict)
            }
        }
    }

    async fn reservation_exists_for_reference(
        &self,
        reference_id: &str,
        num_shards: usize,
    ) -> Result<bool, RepoError> {
        // Mirror the PG path's cross-shard fan-out: the idempotency_key
        // composite is `txn:{shard_idx}:{reference_id}`, so we check
        // that exact Redis key on each shard. First hit wins.
        // Sequential rather than concurrent — num_shards is small
        // (1-3 in this project) and the master GET is sub-ms; the
        // join overhead would dominate.
        for shard_idx in 0..num_shards {
            let key = entry_key(&format!("txn:{}:{}", shard_idx, reference_id));
            let raw = self
                .cache
                .get_master_raw(&key)
                .await
                .map_err(|e| RepoError::Other(format!("redis GET: {}", e)))?;
            if raw.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Tries the Redis path first; on any Redis error, falls through
/// to the PG path. Hot-path latency is the Redis path's; the PG
/// fallback only fires during Redis outages so the request still
/// completes (with the legacy ~10 ms latency).
pub(crate) struct HybridIdempotencyWriter {
    redis: RedisIdempotencyWriter,
    pg: SqlxIdempotencyWriter,
}

impl HybridIdempotencyWriter {
    pub(crate) fn new(redis: RedisIdempotencyWriter, pg: SqlxIdempotencyWriter) -> Self {
        Self { redis, pg }
    }
}

#[async_trait]
impl IdempotencyAwareWriter for HybridIdempotencyWriter {
    async fn reserve(
        &self,
        shard: usize,
        idempotency_key: &str,
        request_hash: &str,
        accepted_payload: &serde_json::Value,
        outbox_payload: &serde_json::Value,
    ) -> Result<ReserveOutcome, RepoError> {
        match self
            .redis
            .reserve(
                shard,
                idempotency_key,
                request_hash,
                accepted_payload,
                outbox_payload,
            )
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Redis idempotency path failed, falling back to PG"
                );
                metrics::counter!("idempotency_redis_fallback_total").increment(1);
                let pg_outcome = self
                    .pg
                    .reserve(
                        shard,
                        idempotency_key,
                        request_hash,
                        accepted_payload,
                        outbox_payload,
                    )
                    .await?;
                // Best-effort: also populate the Redis-namespaced entry
                // so a future Redis-path retry of this key sees it via
                // SETNX-conflict and routes through the Replay /
                // HashConflict probe instead of declaring the key fresh
                // and queueing a duplicate publish via intake.
                self.redis
                    .populate_post_fallback(
                        shard,
                        idempotency_key,
                        request_hash,
                        accepted_payload,
                        outbox_payload,
                    )
                    .await;
                Ok(pg_outcome)
            }
        }
    }

    async fn reservation_exists_for_reference(
        &self,
        reference_id: &str,
        num_shards: usize,
    ) -> Result<bool, RepoError> {
        // Hybrid backend's primary write path is Redis — the entry
        // exists there from the moment `reserve()` returns and stays
        // until the `redis_intake` worker flushes it to PG. Delegate
        // to the Redis half. The caller (`get_status_by_reference`)
        // has already checked PG via the repo — this method covers
        // the OTHER half.
        self.redis
            .reservation_exists_for_reference(reference_id, num_shards)
            .await
    }
}
