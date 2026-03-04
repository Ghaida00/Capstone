use std::env;

/// Application configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // Server
    pub host: String,
    pub port: u16,

    // Database Shards (2 shards × write + reads)
    pub database_shard0_write_url: String,
    pub database_shard0_read_urls: Vec<String>,
    pub database_shard1_write_url: String,
    pub database_shard1_read_urls: Vec<String>,
    pub db_write_pool_size: u32,
    pub db_read_pool_size: u32,

    // Redis
    pub redis_url: String,
    pub redis_read_url: Option<String>,
    pub redis_pool_size: usize,

    // RabbitMQ
    pub rabbitmq_url: String,

    // Rate Limiting
    pub rate_limit_per_second: u64,
    pub rate_limit_burst: u32,

    // Circuit Breaker
    pub circuit_breaker_failure_threshold: u32,
    pub circuit_breaker_recovery_timeout_secs: u64,

    // Backpressure
    pub max_concurrent_requests: usize,
}

impl Config {
    /// Load configuration from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        Self {
            host: env_or("APP_HOST", "0.0.0.0"),
            port: env_or("APP_PORT", "3000").parse().expect("APP_PORT must be a number"),

            // Shard 0
            database_shard0_write_url: env_or(
                "DATABASE_SHARD0_WRITE_URL",
                "postgres://gn_user:gn_secure_pass@pgbouncer-shard0:5432/gn_db",
            ),
            database_shard0_read_urls: parse_csv_env(
                "DATABASE_SHARD0_READ_URLS",
                "postgres://gn_user:gn_secure_pass@pg-shard0-replica1:5432/gn_db,postgres://gn_user:gn_secure_pass@pg-shard0-replica2:5432/gn_db",
            ),
            // Shard 1
            database_shard1_write_url: env_or(
                "DATABASE_SHARD1_WRITE_URL",
                "postgres://gn_user:gn_secure_pass@pgbouncer-shard1:5432/gn_db",
            ),
            database_shard1_read_urls: parse_csv_env(
                "DATABASE_SHARD1_READ_URLS",
                "postgres://gn_user:gn_secure_pass@pg-shard1-replica1:5432/gn_db,postgres://gn_user:gn_secure_pass@pg-shard1-replica2:5432/gn_db",
            ),

            db_write_pool_size: env_or("DB_WRITE_POOL_SIZE", "30")
                .parse()
                .expect("DB_WRITE_POOL_SIZE must be a number"),
            db_read_pool_size: env_or("DB_READ_POOL_SIZE", "50")
                .parse()
                .expect("DB_READ_POOL_SIZE must be a number"),

            redis_url: env_or("REDIS_URL", "redis://127.0.0.1:6379"),
            redis_read_url: std::env::var("REDIS_READ_URL").ok(),
            redis_pool_size: env_or("REDIS_POOL_SIZE", "50")
                .parse()
                .expect("REDIS_POOL_SIZE must be a number"),

            rabbitmq_url: env_or("RABBITMQ_URL", "amqp://gn_user:gn_secure_pass@localhost:5672"),

            rate_limit_per_second: env_or("RATE_LIMIT_PER_SECOND", "10000")
                .parse()
                .expect("RATE_LIMIT_PER_SECOND must be a number"),
            rate_limit_burst: env_or("RATE_LIMIT_BURST", "20000")
                .parse()
                .expect("RATE_LIMIT_BURST must be a number"),

            circuit_breaker_failure_threshold: env_or("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "50")
                .parse()
                .expect("CIRCUIT_BREAKER_FAILURE_THRESHOLD must be a number"),
            circuit_breaker_recovery_timeout_secs: env_or(
                "CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS",
                "10",
            )
            .parse()
            .expect("CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS must be a number"),

            max_concurrent_requests: env_or("MAX_CONCURRENT_REQUESTS", "20000")
                .parse()
                .expect("MAX_CONCURRENT_REQUESTS must be a number"),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_csv_env(key: &str, default: &str) -> Vec<String> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}
