use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::error::AppError;

/// Holds separate read and write database connection pools for a single shard.
/// Supports multiple read replicas with round-robin selection.
#[derive(Debug, Clone)]
pub struct DatabasePool {
    /// Pool for write operations — connects to primary via pgBouncer
    write_pool: PgPool,
    /// Pools for read operations — multiple replicas
    read_pools: Vec<PgPool>,
    /// Round-robin counter for read pool selection
    read_index: std::sync::Arc<AtomicUsize>,
}

impl DatabasePool {
    /// Create a new shard pool with write + multiple read replicas.
    pub async fn new_shard(
        write_url: &str,
        read_urls: &[String],
        write_pool_size: u32,
        read_pool_size: u32,
    ) -> Result<Self, AppError> {
        let write_pool = PgPoolOptions::new()
            .max_connections(write_pool_size)
            .min_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(1800))
            .connect(write_url)
            .await
            .map_err(|e| AppError::Internal(format!("Write pool error: {}", e)))?;

        let mut read_pools = Vec::new();
        // Per-replica pool size = total read pool size / number of replicas
        let per_replica_size = if read_urls.is_empty() {
            read_pool_size
        } else {
            (read_pool_size / read_urls.len() as u32).max(5)
        };

        for url in read_urls {
            match PgPoolOptions::new()
                .max_connections(per_replica_size)
                .min_connections(3)
                .acquire_timeout(Duration::from_secs(5))
                .idle_timeout(Duration::from_secs(300))
                .max_lifetime(Duration::from_secs(1800))
                .connect(url)
                .await
            {
                Ok(pool) => read_pools.push(pool),
                Err(e) => {
                    tracing::warn!(url = url, error = %e, "Failed to connect to read replica");
                }
            }
        }

        // Fallback: if no read replicas, use write pool for reads
        if read_pools.is_empty() {
            tracing::warn!("No read replicas available, falling back to primary for reads");
            read_pools.push(write_pool.clone());
        }

        Ok(Self {
            write_pool,
            read_pools,
            read_index: std::sync::Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Get the write pool.
    pub fn writer(&self) -> &PgPool {
        &self.write_pool
    }

    /// Get a read pool (round-robin across replicas).
    pub fn reader(&self) -> &PgPool {
        let idx = self.read_index.fetch_add(1, Ordering::Relaxed) % self.read_pools.len();
        &self.read_pools[idx]
    }

    /// Close all connections.
    pub async fn close(&self) {
        self.write_pool.close().await;
        for pool in &self.read_pools {
            pool.close().await;
        }
    }
}
