use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use deadpool_redis::Pool;
use serde_json::json;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Local counter entry: request count + window start time.
struct CounterEntry {
    count: u64,
    window_start: Instant,
}

/// Redis-based rate limiter with local in-memory cache.
/// Each replica keeps per-IP counters in memory and syncs to Redis periodically.
/// This reduces Redis round-trips from ~2800/s to ~4/s (one sync per replica per second).
///
/// Fix #16: Background tasks now participate in graceful shutdown via
/// `CancellationToken` instead of running forever.
#[derive(Clone)]
pub struct RateLimiter {
    pool: Arc<Pool>,
    /// Max requests per window
    max_requests: u64,
    /// Window size in seconds
    window_secs: u64,
    /// Local in-memory counters (IP → count within current window)
    local_counters: Arc<RwLock<HashMap<IpAddr, CounterEntry>>>,
}

impl RateLimiter {
    pub fn new(pool: Pool, per_second: u64, burst: u32, cancel: CancellationToken) -> Self {
        let window_secs = if per_second > 0 {
            (burst as u64) / per_second
        } else {
            1
        }
        .max(1);

        let limiter = Self {
            pool: Arc::new(pool),
            max_requests: burst as u64,
            window_secs,
            local_counters: Arc::new(RwLock::new(HashMap::with_capacity(1024))),
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
                        let mut counters = cleanup_limiter.local_counters.write().await;
                        let now = Instant::now();
                        let window = std::time::Duration::from_secs(cleanup_limiter.window_secs);
                        counters.retain(|_, entry| now.duration_since(entry.window_start) < window);
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
    /// Uses fast local memory check — no Redis call per request.
    async fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let window = std::time::Duration::from_secs(self.window_secs);

        let mut counters = self.local_counters.write().await;
        let entry = counters.entry(ip).or_insert(CounterEntry {
            count: 0,
            window_start: now,
        });

        // Reset window if expired
        if now.duration_since(entry.window_start) >= window {
            entry.count = 0;
            entry.window_start = now;
        }

        entry.count += 1;
        entry.count <= self.max_requests
    }

    /// Sync local counters to Redis (called periodically by background task).
    async fn sync_to_redis(&self) {
        let counters = self.local_counters.read().await;
        if counters.is_empty() {
            return;
        }

        // Best-effort sync — don't block if Redis is down
        if let Ok(mut conn) = self.pool.get().await {
            let mut pipe = redis::pipe();
            for (ip, entry) in counters.iter() {
                let key = format!("rl:global:{}", ip);
                pipe.cmd("INCRBY")
                    .arg(&key)
                    .arg(entry.count as i64)
                    .ignore();
                pipe.cmd("EXPIRE")
                    .arg(&key)
                    .arg(self.window_secs as i64)
                    .ignore();
            }
            let _: Result<(), _> = pipe.query_async(&mut *conn).await;
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

    if limiter.check(ip).await {
        next.run(req).await
    } else {
        metrics::counter!("rate_limited_total").increment(1);
        tracing::debug!(client_ip = %ip, "Rate limit exceeded");
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
