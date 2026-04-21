use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::error::AppError;

/// Holds separate read and write database connection pools for a single shard.
/// Supports multiple read replicas with health-aware round-robin selection.
///
/// Failover behaviour:
/// * Each read replica has an `AtomicBool` health flag.
/// * `reader()` skips replicas whose flag is `false`.
/// * If every replica is unhealthy, reads fall back to the writer pool
///   (degraded mode — additional load on primary, but queries still succeed).
/// * A background task (spawned via [`DatabasePool::spawn_health_monitor`])
///   probes every replica on a fixed interval and flips the flags.
#[derive(Debug, Clone)]
pub struct DatabasePool {
    /// Pool for write operations — connects to primary via pgBouncer
    write_pool: PgPool,
    /// Pools for read operations — multiple replicas
    read_pools: Vec<PgPool>,
    /// Health flag per replica. `true` = serving traffic.
    read_health: Arc<Vec<AtomicBool>>,
    /// Round-robin counter for read pool selection
    read_index: Arc<AtomicUsize>,
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
        let mut read_health_vec: Vec<AtomicBool> = Vec::new();

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
                Ok(pool) => {
                    read_pools.push(pool);
                    read_health_vec.push(AtomicBool::new(true));
                }
                Err(e) => {
                    tracing::warn!(url = url, error = %e, "Failed to connect to read replica");
                }
            }
        }

        // Fallback: if no read replicas, use write pool for reads (marked healthy)
        if read_pools.is_empty() {
            tracing::warn!("No read replicas available, falling back to primary for reads");
            read_pools.push(write_pool.clone());
            read_health_vec.push(AtomicBool::new(true));
        }

        let pool = Self {
            write_pool,
            read_pools,
            read_health: Arc::new(read_health_vec),
            read_index: Arc::new(AtomicUsize::new(0)),
        };

        // Warm-up: eagerly establish minimum connections and verify connectivity.
        tracing::info!("Warming up write pool...");
        sqlx::query("SELECT 1")
            .execute(pool.writer())
            .await
            .map_err(|e| AppError::Internal(format!("Write pool warm-up failed: {}", e)))?;

        for (i, rp) in pool.read_pools.iter().enumerate() {
            tracing::info!(replica = i, "Warming up read pool...");
            if let Err(e) = sqlx::query("SELECT 1").execute(rp).await {
                tracing::warn!(replica = i, error = %e, "Read pool warm-up failed — marked unhealthy");
                pool.read_health[i].store(false, Ordering::Relaxed);
            }
        }

        Ok(pool)
    }

    /// Get the write pool.
    pub fn writer(&self) -> &PgPool {
        &self.write_pool
    }

    /// Get a read pool (health-aware round-robin across replicas).
    ///
    /// Skips replicas marked unhealthy by the background monitor.
    /// If every replica is unhealthy, falls back to the writer pool
    /// and logs a warning — reads keep working in degraded mode.
    pub fn reader(&self) -> &PgPool {
        let n = self.read_pools.len();
        for _ in 0..n {
            let idx = self.read_index.fetch_add(1, Ordering::Relaxed) % n;
            if self.read_health[idx].load(Ordering::Relaxed) {
                return &self.read_pools[idx];
            }
        }
        // All replicas marked unhealthy → degraded read on primary.
        tracing::warn!("All read replicas unhealthy — falling back to writer pool for reads");
        &self.write_pool
    }

    /// Number of read replicas (including primary-fallback entry).
    pub fn replica_count(&self) -> usize {
        self.read_pools.len()
    }

    /// How many replicas are currently marked healthy.
    pub fn healthy_replica_count(&self) -> usize {
        self.read_health
            .iter()
            .filter(|h| h.load(Ordering::Relaxed))
            .count()
    }

    /// Spawn a background task that probes each replica every
    /// `interval_secs` seconds and flips its health flag.
    ///
    /// The task exits when `cancel` is cancelled.
    pub fn spawn_health_monitor(&self, shard_label: usize, interval_secs: u64, cancel: CancellationToken) {
        let pools = self.read_pools.clone();
        let health = self.read_health.clone();
        let interval = Duration::from_secs(interval_secs.max(1));

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!(shard = shard_label, "DB health monitor stopping");
                        break;
                    }
                    _ = ticker.tick() => {
                        for (i, pool) in pools.iter().enumerate() {
                            let ok = tokio::time::timeout(
                                Duration::from_secs(2),
                                sqlx::query("SELECT 1").execute(pool),
                            )
                            .await
                            .map(|r| r.is_ok())
                            .unwrap_or(false);

                            let was = health[i].swap(ok, Ordering::Relaxed);
                            if was != ok {
                                if ok {
                                    metrics::counter!(
                                        "db_replica_recovered_total",
                                        "shard" => shard_label.to_string(),
                                        "replica" => i.to_string()
                                    )
                                    .increment(1);
                                    tracing::info!(shard = shard_label, replica = i, "Replica recovered");
                                } else {
                                    metrics::counter!(
                                        "db_replica_failover_total",
                                        "shard" => shard_label.to_string(),
                                        "replica" => i.to_string()
                                    )
                                    .increment(1);
                                    tracing::warn!(shard = shard_label, replica = i, "Replica marked unhealthy");
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Close all connections.
    pub async fn close(&self) {
        self.write_pool.close().await;
        for pool in &self.read_pools {
            pool.close().await;
        }
    }
}
