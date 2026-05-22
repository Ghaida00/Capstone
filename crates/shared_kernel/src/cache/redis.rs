use ::redis::AsyncCommands;
use deadpool_redis::{Config as DeadpoolRedisConfig, Connection, Pool, Runtime};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::error::AppError;
use crate::resilience::DependencyBreaker;

/// R-7 Redis-breaker tuning. Threshold = 10 transient failures
/// (covers a Sentinel-promotion blip without tripping — the app
/// rebuilds the master pool within ~5 s per the
/// `REDIS_SENTINEL_MONITOR_INTERVAL_SECS=5` default); recovery =
/// 30 s. Hardcoded; promote to `RedisCacheConfig` when an operator
/// actually wants to tune them.
const REDIS_BREAKER_FAILURE_THRESHOLD: u32 = 10;
const REDIS_BREAKER_RECOVERY_SECS: u64 = 30;

/// Cache-key version prefix. Bump on incompatible struct shape
/// changes so old cached entries miss instead of mis-deserialising.
pub const CACHE_KEY_VERSION: &str = "v1";

/// Kernel-local config slice for [`RedisCache::new`].
///
/// Phase 4 dropped the direct dependency on the binary's `Config`
/// type. The `app` crate constructs this from its own `Config`.
#[derive(Debug, Clone)]
pub struct RedisCacheConfig {
    pub master_url: String,
    pub read_url: Option<String>,
    pub pool_size: usize,
    pub sentinel_nodes: Vec<String>,
    pub sentinel_master_name: String,
    pub sentinel_monitor_interval_secs: u64,
}

/// Redis cache manager with separate read/write connection pools.
///
/// Writes go to master, reads go to replica for reduced master contention.
///
/// Failover behaviour:
/// * If `redis_sentinel_nodes` is configured, the master endpoint is
///   resolved by querying `SENTINEL get-master-addr-by-name` against each
///   Sentinel in turn. A background task re-resolves on a fixed interval;
///   when the master address changes (after Sentinel promotes a replica),
///   the write pool is rebuilt and atomically swapped in.
/// * If Sentinel is not configured, the pool is built from `REDIS_URL`
///   directly and no background task is spawned — behaviour identical to
///   pre-failover versions.
#[derive(Clone)]
pub struct RedisCache {
    /// Write pool (connected to current Redis master)
    write_pool: Arc<RwLock<Pool>>,
    /// Read pool (connected to redis-replica for cache GETs)
    read_pool: Pool,
    /// Sentinel settings; `Some(..)` enables background re-resolution.
    sentinel: Option<Arc<SentinelSettings>>,
    /// R-7: single per-dependency breaker covering both pool
    /// acquisition AND command execution (the two failure kinds R-6
    /// surfaces as `rate_limiter_redis_sync_failures_total{kind=
    /// pool_get|pipeline}`). Cloned with the cache (cheap Arc).
    redis_breaker: DependencyBreaker,
}

/// Clonable handle to the Sentinel-aware Redis master pool.
///
/// `Pool` itself is cheap to clone (Arc-backed), but the *reference* to
/// the current pool lives behind a `RwLock` so the Sentinel monitor can
/// swap it atomically during a master promotion. Consumers acquire a
/// fresh pool on each `.get()` — callers that hold the returned
/// `Connection` across an await are unaffected by a later swap; they
/// finish their op on the old master and the next op sees the new one.
#[derive(Clone)]
pub struct MasterPoolHandle {
    pool: Arc<RwLock<Pool>>,
}

impl MasterPoolHandle {
    /// Acquire a connection against the current master.
    pub async fn get(&self) -> Result<Connection, AppError> {
        let pool = self.pool.read().await.clone();
        pool.get().await.map_err(AppError::RedisPool)
    }

    /// Construct a handle around a caller-owned pool, bypassing the
    /// Sentinel resolution path. The non-Sentinel callers today are
    /// the rate-limiter integration tests (T-4) which need a real
    /// `MasterPoolHandle` for type-level compatibility with
    /// `RateLimiter::new` but exercise no Redis I/O on the
    /// rate-limiting hot path (the `check` method is in-memory). A
    /// future direct-connection caller would also land here.
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool: Arc::new(RwLock::new(pool)),
        }
    }
}

struct SentinelSettings {
    /// URLs like `redis://sentinel-host:26379` (no auth needed for sentinel queries unless configured)
    nodes: Vec<String>,
    /// Master group name (matches `sentinel monitor <name>` in sentinel.conf)
    master_name: String,
    /// Password to use when rebuilding the master pool
    master_password: Option<String>,
    /// Current resolved master address — kept as a string like `host:port`
    current_master: RwLock<String>,
    /// Pool options to rebuild on master change
    pool_size: usize,
}

impl RedisCache {
    /// Create a new Redis cache from configuration.
    ///
    /// `cancel` is used to stop the Sentinel monitor task on shutdown.
    pub async fn new(
        config: &RedisCacheConfig,
        cancel: CancellationToken,
    ) -> Result<Self, AppError> {
        // ── Build initial write pool ─────────────────────────────────
        // When Sentinel is configured, resolve the live master first and
        // ignore `config.master_url`; otherwise use the URL directly.
        let (initial_write_url, sentinel) = if !config.sentinel_nodes.is_empty() {
            let master_password = parse_password_from_url(&config.master_url);
            // A-2: error is already AppError::SentinelError after the
            // anyhow → AppError migration; propagate it directly
            // rather than wrapping it in Internal.
            let master_addr =
                resolve_master_via_sentinel(&config.sentinel_nodes, &config.sentinel_master_name)
                    .await?;
            let url = build_redis_url(&master_addr, master_password.as_deref());
            tracing::info!(master = %master_addr, "Resolved Redis master via Sentinel");

            let settings = SentinelSettings {
                nodes: config.sentinel_nodes.clone(),
                master_name: config.sentinel_master_name.clone(),
                master_password,
                current_master: RwLock::new(master_addr),
                pool_size: config.pool_size,
            };
            (url, Some(Arc::new(settings)))
        } else {
            (config.master_url.clone(), None)
        };

        let write_pool = build_pool(&initial_write_url, config.pool_size)?;

        // Read pool → redis-replica (or fallback to the same URL as write)
        let read_url = config
            .read_url
            .as_deref()
            .unwrap_or(&initial_write_url)
            .to_string();
        let read_pool = build_pool(&read_url, config.pool_size)?;

        tracing::info!(
            pool_size = config.pool_size,
            sentinel_enabled = sentinel.is_some(),
            "Redis cache pools initialized (read/write split)"
        );

        let redis_breaker = DependencyBreaker::new(
            "redis",
            REDIS_BREAKER_FAILURE_THRESHOLD,
            REDIS_BREAKER_RECOVERY_SECS,
        );
        tracing::info!(
            redis_breaker_threshold = REDIS_BREAKER_FAILURE_THRESHOLD,
            redis_breaker_recovery_secs = REDIS_BREAKER_RECOVERY_SECS,
            "RedisCache initialized (R-7 redis breaker registered)"
        );

        let cache = Self {
            write_pool: Arc::new(RwLock::new(write_pool)),
            read_pool,
            sentinel,
            redis_breaker,
        };

        // Warm-up — non-fatal, tolerates temporary unavailability
        tracing::info!("Warming up Redis pools...");
        if let Err(e) = cache.health_check().await {
            tracing::warn!(error = %e, "Redis warm-up failed (non-fatal)");
        }

        // Spawn Sentinel monitor if enabled
        if cache.sentinel.is_some() {
            cache.spawn_sentinel_monitor(config.sentinel_monitor_interval_secs, cancel);
        }

        Ok(cache)
    }

    /// Clonable handle to the current Redis master pool.
    ///
    /// Hand this to subsystems (rate limiter, etc.) that need to write
    /// to the master and should follow Sentinel failover automatically.
    /// Each call to [`MasterPoolHandle::get`] resolves the current pool
    /// through the same `RwLock` the background monitor swaps, so a
    /// promotion is seen on the next op — no explicit refresh needed.
    pub fn master_pool_handle(&self) -> MasterPoolHandle {
        MasterPoolHandle {
            pool: self.write_pool.clone(),
        }
    }

    /// Spawn a background task that re-resolves the master via Sentinel
    /// every `interval_secs` seconds. On master change, rebuild the write
    /// pool and swap it in under the RwLock.
    fn spawn_sentinel_monitor(&self, interval_secs: u64, cancel: CancellationToken) {
        let Some(settings) = self.sentinel.clone() else {
            return;
        };
        let write_pool = self.write_pool.clone();
        let interval = Duration::from_secs(interval_secs.max(1));

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!("Redis Sentinel monitor stopping");
                        break;
                    }
                    _ = ticker.tick() => {
                        match resolve_master_via_sentinel(&settings.nodes, &settings.master_name).await {
                            Ok(new_addr) => {
                                let current = settings.current_master.read().await.clone();
                                if new_addr != current {
                                    tracing::warn!(
                                        old = %current,
                                        new = %new_addr,
                                        "Redis master changed — rebuilding write pool"
                                    );
                                    let new_url = build_redis_url(
                                        &new_addr,
                                        settings.master_password.as_deref(),
                                    );
                                    match build_pool(&new_url, settings.pool_size) {
                                        Ok(new_pool) => {
                                            *write_pool.write().await = new_pool;
                                            *settings.current_master.write().await = new_addr;
                                            metrics::counter!("redis_master_failover_total")
                                                .increment(1);
                                        }
                                        Err(e) => {
                                            tracing::error!(error = %e, "Failed to rebuild Redis write pool");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Sentinel query failed");
                            }
                        }
                    }
                }
            }
        });
    }

    /// R-7: handle to the Redis dependency breaker.
    pub fn breaker(&self) -> &DependencyBreaker {
        &self.redis_breaker
    }

    /// R-7: fold a raw Redis command result into the breaker
    /// accounting. A command success records success (clears the
    /// failure tally); a command error counts as a dep failure
    /// (the command got to the server and the server errored on
    /// the network/connection layer, or the conn died mid-flight
    /// — both are "redis is unhealthy" classes). Logic-shaped
    /// `RedisError`s (WRONGTYPE etc.) are rare and operator-visible
    /// — recording them is acceptable conservatism.
    fn map_redis<T>(&self, res: Result<T, ::redis::RedisError>) -> Result<T, AppError> {
        match res {
            Ok(v) => {
                self.redis_breaker.record_success();
                Ok(v)
            }
            Err(e) => {
                self.redis_breaker.record_failure();
                Err(AppError::Redis(e))
            }
        }
    }

    /// Get a read connection from the replica pool. R-7: gated by
    /// the breaker (fails fast on a known-down Redis with
    /// `AppError::DependencyDown{name:"redis"}` → 503 + Retry-After)
    /// and records pool-acquire failures as the dep's fault.
    async fn read_conn(&self) -> Result<Connection, AppError> {
        if !self.redis_breaker.allow() {
            return Err(AppError::DependencyDown { name: "redis" });
        }
        match self.read_pool.get().await {
            Ok(c) => Ok(c),
            Err(e) => {
                self.redis_breaker.record_failure();
                Err(AppError::RedisPool(e))
            }
        }
    }

    /// Get a write connection from the current master pool.
    /// Breaker-gated and breaker-recording — see `read_conn`.
    async fn write_conn(&self) -> Result<Connection, AppError> {
        if !self.redis_breaker.allow() {
            return Err(AppError::DependencyDown { name: "redis" });
        }
        let pool = self.write_pool.read().await.clone();
        match pool.get().await {
            Ok(c) => Ok(c),
            Err(e) => {
                self.redis_breaker.record_failure();
                Err(AppError::RedisPool(e))
            }
        }
    }

    /// Get a cached value by key (reads from replica).
    ///
    /// A deserialize failure (stale schema, corrupted value) is
    /// treated as a cache miss — the caller refetches and writes
    /// fresh data over the bad entry. Surfacing the error as 5xx
    /// would punish clients for an internal data hygiene problem
    /// they cannot influence.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, AppError> {
        let mut conn = self.read_conn().await?;
        let value: Option<String> = self.map_redis(conn.get(key).await)?;

        match value {
            Some(json) => match serde_json::from_str(&json) {
                Ok(parsed) => Ok(Some(parsed)),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        key = %key,
                        "cache deserialize failed, treating as miss"
                    );
                    metrics::counter!("cache_deserialize_errors_total").increment(1);
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    /// Set a value in cache with TTL (writes to master).
    pub async fn set<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl_secs: u64,
    ) -> Result<(), AppError> {
        let mut conn = self.write_conn().await?;
        let json = serde_json::to_string(value)?;
        let _: () = self.map_redis(conn.set_ex(key, &json, ttl_secs).await)?;
        Ok(())
    }

    /// Delete a cached value (writes to master).
    pub async fn delete(&self, key: &str) -> Result<(), AppError> {
        let mut conn = self.write_conn().await?;
        let _: () = self.map_redis(conn.del(key).await)?;
        Ok(())
    }

    /// Atomic `SET key value NX EX ttl_secs`. Returns true if this
    /// caller created the key, false if it already existed.
    /// Serialises `value` as JSON.
    pub async fn set_nx_ex<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl_secs: u64,
    ) -> Result<bool, AppError> {
        let json = serde_json::to_string(value)?;
        let mut conn = self.write_conn().await?;
        let res: Option<String> = self.map_redis(
            ::redis::cmd("SET")
                .arg(key)
                .arg(json)
                .arg("NX")
                .arg("EX")
                .arg(ttl_secs)
                .query_async(&mut *conn)
                .await,
        )?;
        Ok(res.is_some())
    }

    /// `LPUSH list value`. Returns the new list length.
    pub async fn lpush(&self, list: &str, value: &str) -> Result<i64, AppError> {
        let mut conn = self.write_conn().await?;
        let len: i64 = self.map_redis(conn.lpush(list, value).await)?;
        Ok(len)
    }

    /// Non-blocking `RPOPLPUSH src dst` — atomically moves the
    /// rightmost element of `src` to the left of `dst`. Returns
    /// `Ok(None)` if `src` is empty. Used instead of the blocking
    /// `BRPOPLPUSH` so callers can sleep+loop on their own cadence
    /// without racing the deadpool/redis-crate connection timeout
    /// that would otherwise fire mid-block on idle queues.
    pub async fn rpoplpush(&self, src: &str, dst: &str) -> Result<Option<String>, AppError> {
        let mut conn = self.write_conn().await?;
        let res: Option<String> = self.map_redis(
            ::redis::cmd("RPOPLPUSH")
                .arg(src)
                .arg(dst)
                .query_async(&mut *conn)
                .await,
        )?;
        Ok(res)
    }

    /// `LREM list count value` — remove `count` occurrences of
    /// `value` from `list`. With `count = 1` removes the first
    /// match (left-to-right). Returns the number actually removed.
    pub async fn lrem(&self, list: &str, count: isize, value: &str) -> Result<i64, AppError> {
        let mut conn = self.write_conn().await?;
        let removed: i64 = self.map_redis(conn.lrem(list, count, value).await)?;
        Ok(removed)
    }

    /// Pipelined batched `RPOPLPUSH src dst` — N invocations in one
    /// network round-trip. Each `RPOPLPUSH` is individually atomic
    /// and inflight-safe; the pipeline only removes per-call network
    /// cost. Returns the items actually claimed (fewer than `n` when
    /// `src` runs out).
    pub async fn rpoplpush_batch(
        &self,
        src: &str,
        dst: &str,
        n: usize,
    ) -> Result<Vec<String>, AppError> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.write_conn().await?;
        let mut pipe = ::redis::pipe();
        for _ in 0..n {
            pipe.cmd("RPOPLPUSH").arg(src).arg(dst);
        }
        let results: Vec<Option<String>> =
            self.map_redis(pipe.query_async(&mut *conn).await)?;
        Ok(results.into_iter().flatten().collect())
    }

    /// `MGET` against the master pool (no replication lag). Values
    /// align with input; `None` for missing keys.
    pub async fn mget_master_raw(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<String>>, AppError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.write_conn().await?;
        let values: Vec<Option<String>> = self.map_redis(
            ::redis::cmd("MGET").arg(keys).query_async(&mut *conn).await,
        )?;
        Ok(values)
    }

    /// Pipelined batched `LREM list 1 key` — one round-trip for N
    /// keys. Returns total items removed (sum across the N LREMs).
    pub async fn lrem_batch(
        &self,
        list: &str,
        keys: &[String],
    ) -> Result<i64, AppError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut conn = self.write_conn().await?;
        let mut pipe = ::redis::pipe();
        for k in keys {
            pipe.cmd("LREM").arg(list).arg(1).arg(k);
        }
        let removed: Vec<i64> =
            self.map_redis(pipe.query_async(&mut *conn).await)?;
        Ok(removed.iter().sum())
    }

    /// Raw GET against the MASTER pool (no replication lag),
    /// returning the JSON string. Used by writers that read what
    /// they just wrote — replay detection on the idempotency
    /// reservation path, intake-worker payload retrieval. Skips
    /// the deserialize step so the caller can match on a typed
    /// shape without paying for the cache's "deserialize-error →
    /// miss" semantics.
    pub async fn get_master_raw(&self, key: &str) -> Result<Option<String>, AppError> {
        let mut conn = self.write_conn().await?;
        let value: Option<String> = self.map_redis(conn.get(key).await)?;
        Ok(value)
    }

    /// Check if Redis is healthy — pings BOTH master and replica pools.
    pub async fn health_check(&self) -> Result<bool, AppError> {
        let mut write_conn = self.write_conn().await?;
        let write_result: String =
            self.map_redis(::redis::cmd("PING").query_async(&mut *write_conn).await)?;

        let mut read_conn = self.read_conn().await?;
        let read_result: String =
            self.map_redis(::redis::cmd("PING").query_async(&mut *read_conn).await)?;

        Ok(write_result == "PONG" && read_result == "PONG")
    }
}

// ──────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────

/// Build a deadpool-redis `Pool` from a URL.
fn build_pool(url: &str, pool_size: usize) -> Result<Pool, AppError> {
    DeadpoolRedisConfig::from_url(url)
        .builder()
        .map_err(|e| AppError::Internal(format!("Redis config error: {}", e)))?
        .max_size(pool_size)
        .wait_timeout(Some(Duration::from_secs(3)))
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(|e| AppError::Internal(format!("Redis pool error: {}", e)))
}

/// Query each Sentinel node in order until one returns the current master
/// `(host, port)` and format as `host:port`.
///
/// Uses the raw `SENTINEL get-master-addr-by-name <name>` command so it
/// works without pulling in the `redis::sentinel` high-level module,
/// which has had API changes across minor versions.
async fn resolve_master_via_sentinel(
    sentinel_urls: &[String],
    master_name: &str,
) -> Result<String, AppError> {
    let mut last_err: Option<String> = None;
    for url in sentinel_urls {
        match query_one_sentinel(url, master_name).await {
            Ok(addr) => return Ok(addr),
            Err(e) => {
                last_err = Some(format!("{}: {}", url, e));
            }
        }
    }
    Err(AppError::SentinelError(format!(
        "All sentinels failed: {}",
        last_err.unwrap_or_else(|| "no sentinels configured".into())
    )))
}

async fn query_one_sentinel(url: &str, master_name: &str) -> Result<String, AppError> {
    // `redis::Client::open` -> Result<_, RedisError>: From<RedisError>
    // already exists on AppError, so `?` does the work.
    let client = ::redis::Client::open(url)?;
    let mut conn = tokio::time::timeout(
        Duration::from_secs(2),
        client.get_multiplexed_async_connection(),
    )
    .await
    .map_err(|_| AppError::SentinelError(format!("connect timeout to {url}")))??;

    let result: Vec<String> = tokio::time::timeout(
        Duration::from_secs(2),
        ::redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(master_name)
            .query_async(&mut conn),
    )
    .await
    .map_err(|_| {
        AppError::SentinelError(format!("SENTINEL command timeout for {master_name}"))
    })??;

    if result.len() < 2 {
        return Err(AppError::SentinelError(format!(
            "Sentinel returned malformed reply for {master_name}"
        )));
    }
    Ok(format!("{}:{}", result[0], result[1]))
}

/// Build `redis://[:password@]host:port` from an address string and optional password.
fn build_redis_url(host_port: &str, password: Option<&str>) -> String {
    match password {
        Some(pw) if !pw.is_empty() => format!("redis://:{}@{}", pw, host_port),
        _ => format!("redis://{}", host_port),
    }
}

/// Pull the password out of a URL like `redis://:secret@host:6379`.
/// Returns `None` when the URL carries no password.
fn parse_password_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://")?.1;
    let userinfo = after_scheme.split_once('@')?.0;
    let (_user, pw) = userinfo.split_once(':')?;
    if pw.is_empty() {
        None
    } else {
        Some(pw.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_password_from_url_extracts_password() {
        assert_eq!(
            parse_password_from_url("redis://:secret@host:6379"),
            Some("secret".to_string())
        );
        assert_eq!(
            parse_password_from_url("redis://user:secret@host:6379"),
            Some("secret".to_string())
        );
        assert_eq!(parse_password_from_url("redis://host:6379"), None);
    }

    #[test]
    fn build_redis_url_formats_correctly() {
        assert_eq!(
            build_redis_url("10.0.0.1:6379", Some("pw")),
            "redis://:pw@10.0.0.1:6379"
        );
        assert_eq!(build_redis_url("host:6379", None), "redis://host:6379");
    }
}
