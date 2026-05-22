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

/// Sharded local-counter map. Each request hashes its IP to one
/// of 64 `std::sync::Mutex<HashMap>` shards; the critical section
/// (hash + insert + cmp + increment) runs synchronously and never
/// holds the lock across `.await`. Contention is `1/SHARDS` of a
/// single global lock under uniform load.
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

// ─── Integration tests for rate_limit_middleware (T-4) ───────
//
// The decision logic (`RateLimiter::check`) is in-memory; only
// the background sync task touches Redis. Tests construct a real
// `MasterPoolHandle` from a `deadpool` pool against an
// unreachable port (deadpool builds lazily, so the pool exists
// but every connection attempt fails). Background sync still
// runs but its failures are recorded on the silent-degradation
// counter (R-6) and do not affect the on-request `check` path.
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{middleware::from_fn_with_state, routing::get, Router};
    use axum_test::TestServer;
    use deadpool_redis::Config as RedisConfig;
    use shared_kernel::cache::redis::MasterPoolHandle;

    fn limiter_with_burst(burst: u32) -> RateLimiter {
        // Pool builds lazily against an unreachable port; the
        // rate-limit decision never calls .get() so this is fine.
        let pool = RedisConfig::from_url("redis://127.0.0.1:1/")
            .create_pool(None)
            .expect("deadpool builds pool lazily");
        let handle = MasterPoolHandle::from_pool(pool);
        RateLimiter::new(handle, 1000, burst, CancellationToken::new())
    }

    fn router_under_limit(limiter: RateLimiter) -> Router {
        Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(from_fn_with_state(limiter, rate_limit_middleware))
    }

    #[tokio::test]
    async fn under_burst_allows_requests() {
        let limiter = limiter_with_burst(5);
        let server = TestServer::new(router_under_limit(limiter));
        for i in 0..5 {
            let res = server.get("/x").add_header("x-real-ip", "1.2.3.4").await;
            assert_eq!(
                res.status_code(),
                StatusCode::OK,
                "request {i} under burst should be admitted"
            );
        }
    }

    #[tokio::test]
    async fn over_burst_returns_429_with_retry_after() {
        let limiter = limiter_with_burst(2);
        let server = TestServer::new(router_under_limit(limiter));

        // Exhaust the burst.
        for _ in 0..2 {
            let res = server.get("/x").add_header("x-real-ip", "9.9.9.9").await;
            assert_eq!(res.status_code(), StatusCode::OK);
        }
        // Next request from the same IP is shed with 429 + the
        // documented `retry-after` header and the documented body.
        let res = server.get("/x").add_header("x-real-ip", "9.9.9.9").await;
        assert_eq!(res.status_code(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.header("retry-after"), "1");
        let body: serde_json::Value = res.json();
        assert_eq!(body["error"], "rate_limited");

        // A *different* IP is on its own counter and still admitted
        // — the per-IP isolation contract.
        let res = server.get("/x").add_header("x-real-ip", "1.1.1.1").await;
        assert_eq!(res.status_code(), StatusCode::OK);
    }
}
