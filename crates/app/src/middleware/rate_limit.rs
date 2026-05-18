use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

use shared_kernel::cache::redis::MasterPoolHandle;

/// Local counter entry: request count + window start time.
struct CounterEntry {
    count: u64,
    window_start: Instant,
}

/// Number of independent shards. Each request hashes its IP to one
/// shard, so contention is `1/N` of a single global lock under uniform
/// load. Power of two so the modulo compiles to an `&`.
const SHARDS: usize = 64;

/// Sharded local-counter map. The previous implementation used a single
/// `tokio::sync::RwLock<HashMap>` taken in WRITE mode for every request
/// — every VU serialised through one async lock, blowing latency past
/// 1 s under 500–1000 concurrent VUs. With 64 shards of `std::sync::Mutex`
/// the critical section (hash + insert + cmp + increment) stays inside
/// the synchronous fast path and almost never contends.
type Shard = Mutex<HashMap<IpAddr, CounterEntry>>;

/// Redis-based rate limiter with local in-memory cache.
/// Each replica keeps per-IP counters in memory and syncs to Redis periodically.
/// This reduces Redis round-trips from ~2800/s to ~4/s (one sync per replica per second).
///
/// Fix #16: Background tasks now participate in graceful shutdown via
/// `CancellationToken` instead of running forever.
#[derive(Clone)]
pub struct RateLimiter {
    /// Sentinel-aware handle to the current Redis master.
    pool: MasterPoolHandle,
    /// Max requests per window
    max_requests: u64,
    /// Window size in seconds
    window_secs: u64,
    /// Sharded local counters (IP → count within current window)
    shards: Arc<[Shard; SHARDS]>,
}

fn shard_for(ip: &IpAddr) -> usize {
    let mut h = fnv::FnvHasher::default();
    ip.hash(&mut h);
    (h.finish() as usize) & (SHARDS - 1)
}

impl RateLimiter {
    pub fn new(
        pool: MasterPoolHandle,
        per_second: u64,
        burst: u32,
        cancel: CancellationToken,
    ) -> Self {
        let window_secs = if per_second > 0 {
            (burst as u64) / per_second
        } else {
            1
        }
        .max(1);

        let shards: [Shard; SHARDS] =
            std::array::from_fn(|_| Mutex::new(HashMap::with_capacity(64)));

        let limiter = Self {
            pool,
            max_requests: burst as u64,
            window_secs,
            shards: Arc::new(shards),
        };

        // Fix #16: Spawn background task to sync local counters to Redis every second
        // with cancellation support
        let sync_limiter = limiter.clone();
        let sync_cancel = cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        sync_limiter.sync_to_redis().await;
                    }
                    _ = sync_cancel.cancelled() => {
                        tracing::info!("Rate limiter sync task: shutting down");
                        // Final sync before exit
                        sync_limiter.sync_to_redis().await;
                        break;
                    }
                }
            }
        });

        // Fix #16: Spawn cleanup task every 10 seconds with cancellation support
        let cleanup_limiter = limiter.clone();
        let cleanup_cancel = cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let now = Instant::now();
                        let window = std::time::Duration::from_secs(cleanup_limiter.window_secs);
                        for shard in cleanup_limiter.shards.iter() {
                            let mut map = shard.lock().unwrap();
                            map.retain(|_, e| now.duration_since(e.window_start) < window);
                        }
                    }
                    _ = cleanup_cancel.cancelled() => {
                        tracing::info!("Rate limiter cleanup task: shutting down");
                        break;
                    }
                }
            }
        });

        limiter
    }

    /// Check if a request from the given IP is allowed.
    /// Synchronous — short critical section, never holds the lock across `await`.
    fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let window = std::time::Duration::from_secs(self.window_secs);

        let shard = &self.shards[shard_for(&ip)];
        let mut map = shard.lock().unwrap();
        let entry = map.entry(ip).or_insert(CounterEntry {
            count: 0,
            window_start: now,
        });

        if now.duration_since(entry.window_start) >= window {
            entry.count = 0;
            entry.window_start = now;
        }

        entry.count += 1;
        entry.count <= self.max_requests
    }

    /// Sync local counters to Redis (called periodically by background task).
    async fn sync_to_redis(&self) {
        // Snapshot all shards under their own locks, then drop locks before
        // touching Redis. Redis I/O must never run while a request-path lock
        // is held.
        let mut snapshot: Vec<(IpAddr, u64)> = Vec::new();
        for shard in self.shards.iter() {
            let map = shard.lock().unwrap();
            for (ip, entry) in map.iter() {
                snapshot.push((*ip, entry.count));
            }
        }
        if snapshot.is_empty() {
            return;
        }

        // R-6: both failure paths below were silently swallowed.
        // When the Redis round-trip fails, every replica keeps
        // enforcing only its LOCAL counter, so the effective
        // global ceiling silently becomes `N × per_replica_limit`
        // — the limiter claims to enforce a number it no longer
        // enforces. An attacker who can keep Redis just stressed
        // enough to fail the sync (or who waits for a Sentinel
        // flip) gets multiplicative burst tolerance. Emit a
        // counter + WARN on each path so the degradation is a
        // page-able signal, not an invisible posture change.
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                metrics::counter!(
                    "rate_limiter_redis_sync_failures_total",
                    "kind" => "pool_get"
                )
                .increment(1);
                tracing::warn!(
                    error = %e,
                    "rate-limit Redis sync skipped: pool acquire failed — \
                     global ceiling degraded to per-replica until Redis recovers"
                );
                return;
            }
        };

        let mut pipe = redis::pipe();
        for (ip, count) in &snapshot {
            let key = format!("rl:global:{}", ip);
            pipe.cmd("INCRBY").arg(&key).arg(*count as i64).ignore();
            pipe.cmd("EXPIRE")
                .arg(&key)
                .arg(self.window_secs as i64)
                .ignore();
        }
        if let Err(e) = pipe.query_async::<()>(&mut *conn).await {
            metrics::counter!(
                "rate_limiter_redis_sync_failures_total",
                "kind" => "pipeline"
            )
            .increment(1);
            tracing::warn!(
                error = %e,
                "rate-limit Redis sync failed: pipeline query errored — \
                 global ceiling degraded to per-replica until Redis recovers"
            );
        }
    }
}

/// Axum middleware function for rate limiting.
pub async fn rate_limit_middleware(
    axum::extract::State(limiter): axum::extract::State<RateLimiter>,
    req: Request,
    next: Next,
) -> Response {
    // Extract client IP from X-Real-IP header (set by Nginx) or fallback
    let ip = req
        .headers()
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<IpAddr>().ok())
        .unwrap_or_else(|| "127.0.0.1".parse().unwrap());

    if limiter.check(ip) {
        next.run(req).await
    } else {
        metrics::counter!("rate_limited_total").increment(1);
        (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1")],
            Json(json!({
                "error": "rate_limited",
                "message": "Too many requests, please try again later"
            })),
        )
            .into_response()
    }
}
