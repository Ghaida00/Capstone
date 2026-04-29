use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::error::AppError;

use super::pool::DatabasePool;

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

        tracing::info!(num_shards = shards.len(), "ShardRouter initialized");

        Ok(Self { shards })
    }

    /// Get the shard index for a given account. FNV-1a → modulo
    /// over the runtime-configured shard count.
    pub fn shard_for(account: &str) -> usize {
        let mut hasher = fnv::FnvHasher::default();
        account.hash(&mut hasher);
        let n = NUM_SHARDS.load(Ordering::Relaxed).max(1);
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
        let shard = Self::shard_for(from_account);
        self.writer(shard)
    }

    /// Get a reader pool for an account.
    pub fn reader_for(&self, from_account: &str) -> &PgPool {
        let shard = Self::shard_for(from_account);
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
