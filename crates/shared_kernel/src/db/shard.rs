use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::error::AppError;
use crate::resilience::DependencyBreaker;

use super::pool::DatabasePool;

/// R-7 DB-breaker tuning. Threshold = 10 transient failures (well
/// past the small per-promotion blip the retry budget is sized to
/// absorb in `.env DB_WRITE_RETRY_*`); recovery = 30 s (covers one
/// full typical Patroni promotion window). Hardcoded for now —
/// promoting to `ShardRouterConfig` is trivial when an operator
/// actually wants to tune them.
const DB_BREAKER_FAILURE_THRESHOLD: u32 = 10;
const DB_BREAKER_RECOVERY_SECS: u64 = 30;

/// Runtime shard count, set on `ShardRouter::new` from config.
/// Default 2 mirrors the historical const so single-binary tests
/// that bypass `ShardRouter::new` still hash correctly.
static NUM_SHARDS: AtomicUsize = AtomicUsize::new(2);

/// Per-shard URL pair.
#[derive(Debug, Clone)]
pub struct ShardUrls {
    pub write_url: String,
    pub read_urls: Vec<String>,
}

/// Kernel-local config slice for [`ShardRouter::new`].
///
/// Phase 4 dropped the direct dependency on the binary's `Config`
/// type so the kernel does not need to know about env-var loading.
/// The `app` crate constructs this from its own `Config`.
#[derive(Debug, Clone)]
pub struct ShardRouterConfig {
    pub shards: Vec<ShardUrls>,
    pub write_pool_size: u32,
    pub read_pool_size: u32,
    pub health_check_interval_secs: u64,
}

/// Application-level shard router.
/// Routes queries to the correct shard based on hash(from_account) % NUM_SHARDS.
///
/// Uses FNV-1a hashing which is deterministic across Rust versions,
/// unlike `DefaultHasher` which may change between compiler releases.
#[derive(Debug, Clone)]
pub struct ShardRouter {
    shards: Vec<DatabasePool>,
    /// R-7 DB breaker. One per-dependency breaker covering every
    /// shard — a single dependency (Postgres) conceptually, even
    /// though it is sharded. Per-shard isolation would be a refinement;
    /// the audit's R-7 prescription names it as one `DbBreaker`.
    db_breaker: DependencyBreaker,
}

impl ShardRouter {
    /// Create a new shard router from configuration.
    ///
    /// Spawns a per-shard background health monitor wired to `cancel` so
    /// read-replica failover flips automatically when a replica goes down.
    pub async fn new(
        config: &ShardRouterConfig,
        cancel: CancellationToken,
    ) -> Result<Self, AppError> {
        if config.shards.is_empty() {
            return Err(AppError::Internal(
                "ShardRouterConfig must contain at least one shard".into(),
            ));
        }
        // Publish runtime shard count for the static `shard_for`.
        NUM_SHARDS.store(config.shards.len(), Ordering::Relaxed);

        let mut shards = Vec::with_capacity(config.shards.len());

        for (i, shard_urls) in config.shards.iter().enumerate() {
            let pool = DatabasePool::new_shard(
                &shard_urls.write_url,
                &shard_urls.read_urls,
                config.write_pool_size,
                config.read_pool_size,
                i,
            )
            .await
            .map_err(|e| AppError::Internal(format!("Failed to connect to shard {}: {}", i, e)))?;

            pool.spawn_health_monitor(i, config.health_check_interval_secs, cancel.child_token());

            tracing::info!(
                shard = i,
                "Shard pool initialized (failover monitor spawned)"
            );
            shards.push(pool);
        }

        let db_breaker =
            DependencyBreaker::new("db", DB_BREAKER_FAILURE_THRESHOLD, DB_BREAKER_RECOVERY_SECS);

        tracing::info!(
            num_shards = shards.len(),
            db_breaker_threshold = DB_BREAKER_FAILURE_THRESHOLD,
            db_breaker_recovery_secs = DB_BREAKER_RECOVERY_SECS,
            "ShardRouter initialized (R-7 db breaker registered)"
        );

        Ok(Self { shards, db_breaker })
    }

    /// R-7: handle to the DB dependency breaker. Call sites that
    /// already use `retry_transient` should migrate to
    /// `retry_transient_with_breaker(&shards.db_breaker(), ...)` to
    /// pick up fail-fast on a known-down DB.
    pub fn db_breaker(&self) -> &DependencyBreaker {
        &self.db_breaker
    }

    /// Get the shard index for a given account. FNV-1a → modulo
    /// over the runtime-configured shard count.
    ///
    /// Static helper kept for tests and historical callers — uses
    /// the process-global `NUM_SHARDS`, so it is not safe when
    /// multiple `ShardRouter`s with different shard counts coexist.
    /// Production code paths (consumer, application services,
    /// repositories) MUST use [`Self::shard_for_account`] which
    /// hashes against this router's own shard count.
    pub fn shard_for(account: &str) -> usize {
        let mut hasher = fnv::FnvHasher::default();
        account.hash(&mut hasher);
        let n = NUM_SHARDS.load(Ordering::Relaxed).max(1);
        (hasher.finish() as usize) % n
    }

    /// Instance-bound counterpart of [`Self::shard_for`]. Always
    /// uses `self.shards.len()` so the result is guaranteed to be
    /// a valid index into this router. Use this on every hot path
    /// that goes on to call `self.writer(shard)` / `self.reader(shard)`
    /// to eliminate the panic window when `NUM_SHARDS` is mutated by
    /// a sibling router.
    pub fn shard_for_account(&self, account: &str) -> usize {
        let mut hasher = fnv::FnvHasher::default();
        account.hash(&mut hasher);
        let n = self.shards.len().max(1);
        (hasher.finish() as usize) % n
    }

    /// Get the writer pool for a specific shard.
    pub fn writer(&self, shard: usize) -> &PgPool {
        self.shards[shard].writer()
    }

    /// Get a reader pool for a specific shard (round-robins across replicas).
    pub fn reader(&self, shard: usize) -> &PgPool {
        self.shards[shard].reader()
    }

    /// Get the writer pool for an account (auto-routes to correct shard).
    pub fn writer_for(&self, from_account: &str) -> &PgPool {
        let shard = self.shard_for_account(from_account);
        self.writer(shard)
    }

    /// Get a reader pool for an account.
    pub fn reader_for(&self, from_account: &str) -> &PgPool {
        let shard = self.shard_for_account(from_account);
        self.reader(shard)
    }

    /// Get all shard pools (for cross-shard queries like listing).
    pub fn all_shards(&self) -> &[DatabasePool] {
        &self.shards
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Close all shard connections.
    pub async fn close(&self) {
        for (i, shard) in self.shards.iter().enumerate() {
            shard.close().await;
            tracing::info!(shard = i, "Shard pool closed");
        }
    }

    /// Snapshot of `(total_replicas, healthy_replicas)` per shard, sourced
    /// from the background health monitor — no extra queries issued.
    pub fn replica_health(&self) -> Vec<(usize, usize)> {
        self.shards
            .iter()
            .map(|s| (s.replica_count(), s.healthy_replica_count()))
            .collect()
    }

    /// Health check all shards.
    pub async fn health_check(&self) -> (bool, bool) {
        let mut all_write_ok = true;
        let mut all_read_ok = true;

        for shard in &self.shards {
            if sqlx::query("SELECT 1")
                .execute(shard.writer())
                .await
                .is_err()
            {
                all_write_ok = false;
            }
            if sqlx::query("SELECT 1")
                .execute(shard.reader())
                .await
                .is_err()
            {
                all_read_ok = false;
            }
        }

        (all_write_ok, all_read_ok)
    }
}
