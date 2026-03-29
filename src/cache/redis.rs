use ::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Connection, Pool, Runtime};
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;

use crate::config::Config;
use crate::error::AppError;

/// Redis cache manager with separate read/write connection pools.
/// Writes go to master, reads go to replica for reduced master contention.
#[derive(Clone)]
pub struct RedisCache {
    /// Write pool (connected to redis-master)
    write_pool: Pool,
    /// Read pool (connected to redis-replica for cache GETs)
    read_pool: Pool,
}

impl RedisCache {
    /// Create a new Redis cache from configuration.
    pub async fn new(config: &Config) -> Result<Self, AppError> {
        // Write pool → redis-master
        let write_cfg = RedisConfig::from_url(&config.redis_url);
        let write_pool = write_cfg
            .builder()
            .map_err(|e| AppError::Internal(format!("Redis write config error: {}", e)))?
            .max_size(config.redis_pool_size)
            .wait_timeout(Some(Duration::from_secs(3)))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| AppError::Internal(format!("Redis write pool error: {}", e)))?;

        // Read pool → redis-replica (or fallback to master)
        let read_url = config
            .redis_read_url
            .as_deref()
            .unwrap_or(&config.redis_url);
        let read_cfg = RedisConfig::from_url(read_url);
        let read_pool = read_cfg
            .builder()
            .map_err(|e| AppError::Internal(format!("Redis read config error: {}", e)))?
            .max_size(config.redis_pool_size)
            .wait_timeout(Some(Duration::from_secs(3)))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| AppError::Internal(format!("Redis read pool error: {}", e)))?;

        tracing::info!(
            pool_size = config.redis_pool_size,
            read_url = read_url,
            "Redis cache pools initialized (read/write split)"
        );

        let cache = Self {
            write_pool,
            read_pool,
        };

        // Warm-up: eagerly establish connections and verify connectivity
        tracing::info!("Warming up Redis write pool...");
        if let Err(e) = cache.health_check().await {
            tracing::warn!(error = %e, "Redis write pool warm-up failed (non-fatal)");
        }
        tracing::info!("Warming up Redis read pool...");
        match cache.read_conn().await {
            Ok(mut conn) => {
                let result: Result<String, _> = ::redis::cmd("PING").query_async(&mut *conn).await;
                if let Err(e) = result {
                    tracing::warn!(error = %e, "Redis read pool warm-up failed (non-fatal)");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Redis read pool warm-up connection failed (non-fatal)");
            }
        }

        Ok(cache)
    }

    /// Create a separate connection pool (for rate limiter, etc.)
    pub fn create_pool(&self, config: &Config) -> Result<Pool, AppError> {
        let cfg = RedisConfig::from_url(&config.redis_url);
        cfg.builder()
            .map_err(|e| AppError::Internal(format!("Redis config error: {}", e)))?
            .max_size(config.redis_pool_size)
            .wait_timeout(Some(Duration::from_secs(2)))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| AppError::Internal(format!("Redis pool error: {}", e)))
    }

    /// Get a read connection from the replica pool.
    async fn read_conn(&self) -> Result<Connection, AppError> {
        self.read_pool.get().await.map_err(AppError::RedisPool)
    }

    /// Get a write connection from the master pool.
    async fn write_conn(&self) -> Result<Connection, AppError> {
        self.write_pool.get().await.map_err(AppError::RedisPool)
    }

    /// Get a cached value by key (reads from replica).
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, AppError> {
        let mut conn = self.read_conn().await?;
        let value: Option<String> = conn.get(key).await.map_err(AppError::Redis)?;

        match value {
            Some(json) => {
                let parsed = serde_json::from_str(&json)?;
                Ok(Some(parsed))
            }
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
        let _: () = conn
            .set_ex(key, &json, ttl_secs)
            .await
            .map_err(AppError::Redis)?;
        Ok(())
    }

    /// Delete a cached value (writes to master).
    pub async fn delete(&self, key: &str) -> Result<(), AppError> {
        let mut conn = self.write_conn().await?;
        let _: () = conn.del(key).await.map_err(AppError::Redis)?;
        Ok(())
    }

    /// Check if Redis connection is healthy.
    pub async fn health_check(&self) -> Result<bool, AppError> {
        let mut conn = self.write_conn().await?;
        let result: String = ::redis::cmd("PING")
            .query_async(&mut *conn)
            .await
            .map_err(AppError::Redis)?;
        Ok(result == "PONG")
    }
}
