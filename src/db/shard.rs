use std::hash::{Hash, Hasher};

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::error::AppError;

use super::pool::DatabasePool;

/// Number of database shards.
const NUM_SHARDS: usize = 3;

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
    pub async fn new(config: &Config, cancel: CancellationToken) -> Result<Self, AppError> {
        let mut shards = Vec::with_capacity(NUM_SHARDS);

        let shard_configs = vec![
            (
                &config.database_shard0_write_url,
                &config.database_shard0_read_urls,
            ),
            (
                &config.database_shard1_write_url,
                &config.database_shard1_read_urls,
            ),
            (
                &config.database_shard2_write_url,
                &config.database_shard2_read_urls,
            ),
        ];

        for (i, (write_url, read_urls)) in shard_configs.into_iter().enumerate() {
            let pool = DatabasePool::new_shard(
                write_url,
                read_urls,
                config.db_write_pool_size,
                config.db_read_pool_size,
            )
            .await
            .map_err(|e| AppError::Internal(format!("Failed to connect to shard {}: {}", i, e)))?;

            pool.spawn_health_monitor(i, config.db_health_check_interval_secs, cancel.child_token());

            tracing::info!(shard = i, "Shard pool initialized (failover monitor spawned)");
            shards.push(pool);
        }

        tracing::info!(num_shards = NUM_SHARDS, "ShardRouter initialized");

        Ok(Self { shards })
    }

    /// Get the shard index for a given account.
    ///
    /// Uses FNV-1a hashing for deterministic, version-stable routing.
    pub fn shard_for(account: &str) -> usize {
        let mut hasher = fnv::FnvHasher::default();
        account.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
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
