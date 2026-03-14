use std::env;
use std::fmt;

/// Application configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // Server
    pub host: String,
    pub port: u16,

    // Database Shards (3 shards × write + reads)
    pub database_shard0_write_url: String,
    pub database_shard0_read_urls: Vec<String>,
    pub database_shard1_write_url: String,
    pub database_shard1_read_urls: Vec<String>,
    pub database_shard2_write_url: String,
    pub database_shard2_read_urls: Vec<String>,
    pub db_write_pool_size: u32,
    pub db_read_pool_size: u32,

    // Timeouts (seconds)
    pub db_query_timeout_secs: u64,
    pub redis_command_timeout_secs: u64,
    pub api_timeout_secs: u64,

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
            port: env_or("APP_PORT", "3000")
                .parse()
                .expect("APP_PORT must be a number"),

            // Shard 0
            database_shard0_write_url: env_or(
                "DATABASE_SHARD0_WRITE_URL",
                "postgres://gn_user:gn_secure_pass@pgbouncer-shard0:5432/gn_db",
            ),
            database_shard0_read_urls: parse_csv_env(
                "DATABASE_SHARD0_READ_URLS",
                "postgres://gn_user:gn_secure_pass@pg-shard0-replica1:5432/gn_db",
            ),
            // Shard 1
            database_shard1_write_url: env_or(
                "DATABASE_SHARD1_WRITE_URL",
                "postgres://gn_user:gn_secure_pass@pgbouncer-shard1:5432/gn_db",
            ),
            database_shard1_read_urls: parse_csv_env(
                "DATABASE_SHARD1_READ_URLS",
                "postgres://gn_user:gn_secure_pass@pg-shard1-replica1:5432/gn_db",
            ),
            // Shard 2
            database_shard2_write_url: env_or(
                "DATABASE_SHARD2_WRITE_URL",
                "postgres://gn_user:gn_secure_pass@pgbouncer-shard2:5432/gn_db",
            ),
            database_shard2_read_urls: parse_csv_env(
                "DATABASE_SHARD2_READ_URLS",
                "postgres://gn_user:gn_secure_pass@pg-shard2-replica1:5432/gn_db",
            ),

            db_write_pool_size: env_or("DB_WRITE_POOL_SIZE", "30")
                .parse()
                .expect("DB_WRITE_POOL_SIZE must be a number"),
            db_read_pool_size: env_or("DB_READ_POOL_SIZE", "50")
                .parse()
                .expect("DB_READ_POOL_SIZE must be a number"),

            db_query_timeout_secs: env_or("DB_QUERY_TIMEOUT_SECS", "5")
                .parse()
                .expect("DB_QUERY_TIMEOUT_SECS must be a number"),
            redis_command_timeout_secs: env_or("REDIS_COMMAND_TIMEOUT_SECS", "3")
                .parse()
                .expect("REDIS_COMMAND_TIMEOUT_SECS must be a number"),
            api_timeout_secs: env_or("API_TIMEOUT_SECS", "30")
                .parse()
                .expect("API_TIMEOUT_SECS must be a number"),

            redis_url: env_or("REDIS_URL", "redis://127.0.0.1:6379"),
            redis_read_url: std::env::var("REDIS_READ_URL").ok(),
            redis_pool_size: env_or("REDIS_POOL_SIZE", "50")
                .parse()
                .expect("REDIS_POOL_SIZE must be a number"),

            rabbitmq_url: env_or(
                "RABBITMQ_URL",
                "amqp://gn_user:gn_secure_pass@localhost:5672",
            ),

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

    /// Validate that all configuration values are within sane bounds.
    /// Called once at startup — the process exits immediately on failure.
    pub fn validate(&self) -> anyhow::Result<()> {
        use anyhow::ensure;

        // Server
        ensure!(self.port > 0, "APP_PORT must be > 0");

        // Pool sizes
        ensure!(
            self.db_write_pool_size >= 1 && self.db_write_pool_size <= 500,
            "DB_WRITE_POOL_SIZE must be 1–500, got {}",
            self.db_write_pool_size
        );
        ensure!(
            self.db_read_pool_size >= 1 && self.db_read_pool_size <= 500,
            "DB_READ_POOL_SIZE must be 1–500, got {}",
            self.db_read_pool_size
        );
        ensure!(
            self.redis_pool_size >= 1 && self.redis_pool_size <= 500,
            "REDIS_POOL_SIZE must be 1–500, got {}",
            self.redis_pool_size
        );

        // Timeouts — downstream timeouts must be shorter than the API timeout
        ensure!(
            self.db_query_timeout_secs > 0,
            "DB_QUERY_TIMEOUT_SECS must be > 0"
        );
        ensure!(
            self.redis_command_timeout_secs > 0,
            "REDIS_COMMAND_TIMEOUT_SECS must be > 0"
        );
        ensure!(self.api_timeout_secs > 0, "API_TIMEOUT_SECS must be > 0");
        ensure!(
            self.db_query_timeout_secs < self.api_timeout_secs,
            "DB_QUERY_TIMEOUT_SECS ({}) must be < API_TIMEOUT_SECS ({})",
            self.db_query_timeout_secs,
            self.api_timeout_secs
        );
        ensure!(
            self.redis_command_timeout_secs < self.api_timeout_secs,
            "REDIS_COMMAND_TIMEOUT_SECS ({}) must be < API_TIMEOUT_SECS ({})",
            self.redis_command_timeout_secs,
            self.api_timeout_secs
        );

        // Backpressure
        ensure!(
            self.max_concurrent_requests >= 1,
            "MAX_CONCURRENT_REQUESTS must be >= 1"
        );

        // Rate limiting
        ensure!(
            self.rate_limit_per_second > 0,
            "RATE_LIMIT_PER_SECOND must be > 0"
        );

        // Circuit breaker
        ensure!(
            self.circuit_breaker_failure_threshold > 0,
            "CIRCUIT_BREAKER_FAILURE_THRESHOLD must be > 0"
        );
        ensure!(
            self.circuit_breaker_recovery_timeout_secs > 0,
            "CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS must be > 0"
        );

        Ok(())
    }
}

/// Display implementation that masks secrets in URLs and passwords.
impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Configuration:")?;
        writeln!(f, "  host:                         {}", self.host)?;
        writeln!(f, "  port:                         {}", self.port)?;
        writeln!(
            f,
            "  db_write_pool_size:           {}",
            self.db_write_pool_size
        )?;
        writeln!(
            f,
            "  db_read_pool_size:            {}",
            self.db_read_pool_size
        )?;
        writeln!(
            f,
            "  db_query_timeout_secs:        {}",
            self.db_query_timeout_secs
        )?;
        writeln!(
            f,
            "  redis_command_timeout_secs:   {}",
            self.redis_command_timeout_secs
        )?;
        writeln!(
            f,
            "  api_timeout_secs:             {}",
            self.api_timeout_secs
        )?;
        writeln!(
            f,
            "  redis_pool_size:              {}",
            self.redis_pool_size
        )?;
        writeln!(
            f,
            "  rate_limit_per_second:        {}",
            self.rate_limit_per_second
        )?;
        writeln!(
            f,
            "  rate_limit_burst:             {}",
            self.rate_limit_burst
        )?;
        writeln!(
            f,
            "  max_concurrent_requests:      {}",
            self.max_concurrent_requests
        )?;
        writeln!(
            f,
            "  circuit_breaker_threshold:    {}",
            self.circuit_breaker_failure_threshold
        )?;
        writeln!(
            f,
            "  circuit_breaker_recovery_s:   {}",
            self.circuit_breaker_recovery_timeout_secs
        )?;
        writeln!(
            f,
            "  database_shard0_write_url:    {}",
            mask_url(&self.database_shard0_write_url)
        )?;
        writeln!(
            f,
            "  database_shard1_write_url:    {}",
            mask_url(&self.database_shard1_write_url)
        )?;
        writeln!(
            f,
            "  database_shard2_write_url:    {}",
            mask_url(&self.database_shard2_write_url)
        )?;
        writeln!(
            f,
            "  redis_url:                    {}",
            mask_url(&self.redis_url)
        )?;
        writeln!(
            f,
            "  rabbitmq_url:                 {}",
            mask_url(&self.rabbitmq_url)
        )?;
        Ok(())
    }
}

/// Mask the password portion of a URL for safe logging.
/// e.g. `postgres://user:secret@host:5432/db` → `postgres://user:****@host:5432/db`
fn mask_url(url: &str) -> String {
    // Find "://" then look for ":" after the username and "@" after the password
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_pos) = after_scheme.find('@') {
            let userinfo = &after_scheme[..at_pos];
            if let Some(colon_pos) = userinfo.find(':') {
                let username = &userinfo[..colon_pos];
                let host_part = &after_scheme[at_pos..];
                return format!("{}://{}:****{}", &url[..scheme_end], username, host_part);
            }
        }
    }
    url.to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a valid config with sane defaults for testing.
    fn test_config() -> Config {
        Config {
            host: "0.0.0.0".to_string(),
            port: 3000,
            database_shard0_write_url: "postgres://user:pass@host:5432/db".to_string(),
            database_shard0_read_urls: vec!["postgres://user:pass@host:5432/db".to_string()],
            database_shard1_write_url: "postgres://user:pass@host:5432/db".to_string(),
            database_shard1_read_urls: vec!["postgres://user:pass@host:5432/db".to_string()],
            database_shard2_write_url: "postgres://user:pass@host:5432/db".to_string(),
            database_shard2_read_urls: vec!["postgres://user:pass@host:5432/db".to_string()],
            db_write_pool_size: 30,
            db_read_pool_size: 50,
            db_query_timeout_secs: 5,
            redis_command_timeout_secs: 3,
            api_timeout_secs: 30,
            redis_url: "redis://127.0.0.1:6379".to_string(),
            redis_read_url: None,
            redis_pool_size: 50,
            rabbitmq_url: "amqp://user:pass@localhost:5672".to_string(),
            rate_limit_per_second: 10000,
            rate_limit_burst: 20000,
            circuit_breaker_failure_threshold: 50,
            circuit_breaker_recovery_timeout_secs: 10,
            max_concurrent_requests: 20000,
        }
    }

    #[test]
    fn valid_config_passes_validation() {
        assert!(test_config().validate().is_ok());
    }

    #[test]
    fn zero_pool_size_fails() {
        let mut cfg = test_config();
        cfg.db_write_pool_size = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn pool_size_over_max_fails() {
        let mut cfg = test_config();
        cfg.db_read_pool_size = 501;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn db_timeout_greater_than_api_timeout_fails() {
        let mut cfg = test_config();
        cfg.db_query_timeout_secs = 31;
        cfg.api_timeout_secs = 30;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn redis_timeout_greater_than_api_timeout_fails() {
        let mut cfg = test_config();
        cfg.redis_command_timeout_secs = 30;
        cfg.api_timeout_secs = 30;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_concurrent_requests_fails() {
        let mut cfg = test_config();
        cfg.max_concurrent_requests = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_rate_limit_fails() {
        let mut cfg = test_config();
        cfg.rate_limit_per_second = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn mask_url_hides_password() {
        assert_eq!(
            mask_url("postgres://gn_user:gn_secure_pass@host:5432/db"),
            "postgres://gn_user:****@host:5432/db"
        );
    }

    #[test]
    fn mask_url_handles_amqp() {
        assert_eq!(
            mask_url("amqp://user:secret@rabbit:5672"),
            "amqp://user:****@rabbit:5672"
        );
    }

    #[test]
    fn mask_url_leaves_plain_string_unchanged() {
        assert_eq!(mask_url("no-scheme-here"), "no-scheme-here");
    }

    #[test]
    fn display_does_not_contain_raw_password() {
        let cfg = test_config();
        let output = format!("{}", cfg);
        assert!(!output.contains("pass"), "Display should mask passwords");
        assert!(
            output.contains("****"),
            "Display should contain masked markers"
        );
    }
}
