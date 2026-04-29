use std::env;
use std::fmt;

/// Application configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // Server
    pub host: String,
    pub port: u16,

    // Database Shards (write + reads per shard).
    // Shard 2 fields are kept populated for forward-compatibility
    // but are no longer pushed into the ShardRouter — see the
    // "shard 2 disabled" markers in src/bootstrap.rs and
    // docker-compose.yml. To restore the 3-shard topology, flip
    // those markers and bump NUM_SHARDS back to 3 in
    // shared_kernel/src/db/shard.rs.
    pub database_shard0_write_url: String,
    pub database_shard0_read_urls: Vec<String>,
    pub database_shard1_write_url: String,
    pub database_shard1_read_urls: Vec<String>,
    pub database_shard2_write_url: String,
    // Loaded from env but not currently wired into ShardRouter
    // while shard 2 is disabled. Kept so re-enabling shard 2
    // is a one-line flip in src/bootstrap.rs.
    #[allow(dead_code)]
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

    // Redis Sentinel (failover)
    pub redis_sentinel_nodes: Vec<String>,
    pub redis_sentinel_master_name: String,
    pub redis_sentinel_monitor_interval_secs: u64,

    // DB Failover
    pub db_health_check_interval_secs: u64,
    pub db_write_retry_max_attempts: u32,
    // Parsed from env (`DB_WRITE_RETRY_BACKOFF_MS`) for forward
    // compatibility but not yet threaded into the `retry_transient`
    // call sites — they hardcode a backoff today. Wiring it through
    // is tracked outside Phase 4 scope.
    #[allow(dead_code)]
    pub db_write_retry_backoff_ms: u64,

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

    // CORS
    pub cors_allowed_origins: Vec<String>,

    // Authentication (disabled by default for load testing)
    pub enable_auth: bool,
    pub auth_secret: Option<String>,

    /// Toggle the cross-module `accounts.get_balance` check inside
    /// the transactions create hot path. Default OFF: every create
    /// otherwise pays a Redis GET (warm) or DB SELECT (cold) just
    /// to confirm the sender exists, which adds 1–10 ms to p50 and
    /// is duplicate work — the consumer re-validates balance under
    /// `UPDATE … WHERE balance >= $1` before debiting. Set
    /// `TX_VERIFY_FROM_ACCOUNT=true` to restore the fail-fast 400
    /// for unknown senders.
    pub verify_from_account_exists: bool,

    /// Hard cap on slot-wait inside the backpressure middleware (ms).
    /// Previous default was 500 ms which added that to the tail of
    /// every request that ultimately got 503'd. With the latency
    /// budget tightened to <50 ms p95, the wait must be a small
    /// fraction of that — 50 ms is the new default; 0 disables
    /// queueing entirely.
    pub backpressure_wait_ms: u64,
}

impl Config {
    /// Load configuration from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        Self {
            host: env_or("APP_HOST", "0.0.0.0"),
            port: env_or("APP_PORT", "3000")
                .parse()
                .expect("APP_PORT must be a number"),

            // ── Shard URL defaults ───────────────────────────────────
            // Write URLs point at the per-shard pgBouncer, which in
            // turn points at pg-haproxy. HAProxy forwards to whichever
            // node is currently primary under Patroni (leader-lock
            // holder in etcd), so the hostnames here are stable across
            // failovers.
            //
            // Read URLs list BOTH nodes of each shard because roles
            // flip on promotion. src/db/pool.rs runs health-aware
            // round-robin across them — reads against a node that has
            // temporarily become primary still succeed, they just
            // don't benefit from replica load-splitting until the old
            // primary rejoins.
            //
            // See docs/ha-architecture.md for the full topology and
            // the Patroni migration path (under which these defaults
            // stay unchanged because the HA tool sits behind HAProxy).

            // Shard 0
            database_shard0_write_url: env_or(
                "DATABASE_SHARD0_WRITE_URL",
                "postgres://peakload_user:peakload_secure_pass@pgbouncer-shard0:5432/peakload_db",
            ),
            database_shard0_read_urls: parse_csv_env(
                "DATABASE_SHARD0_READ_URLS",
                "postgres://peakload_user:peakload_secure_pass@pg-shard0-node-a:5432/peakload_db,postgres://peakload_user:peakload_secure_pass@pg-shard0-node-b:5432/peakload_db",
            ),
            // Shard 1
            database_shard1_write_url: env_or(
                "DATABASE_SHARD1_WRITE_URL",
                "postgres://peakload_user:peakload_secure_pass@pgbouncer-shard1:5432/peakload_db",
            ),
            database_shard1_read_urls: parse_csv_env(
                "DATABASE_SHARD1_READ_URLS",
                "postgres://peakload_user:peakload_secure_pass@pg-shard1-node-a:5432/peakload_db,postgres://peakload_user:peakload_secure_pass@pg-shard1-node-b:5432/peakload_db",
            ),
            // Shard 2
            database_shard2_write_url: env_or(
                "DATABASE_SHARD2_WRITE_URL",
                "postgres://peakload_user:peakload_secure_pass@pgbouncer-shard2:5432/peakload_db",
            ),
            database_shard2_read_urls: parse_csv_env(
                "DATABASE_SHARD2_READ_URLS",
                "postgres://peakload_user:peakload_secure_pass@pg-shard2-node-a:5432/peakload_db,postgres://peakload_user:peakload_secure_pass@pg-shard2-node-b:5432/peakload_db",
            ),

            // Pool sizes raised: previous defaults bottlenecked at the
            // app↔pgBouncer hop. With 4 replicas each pool is now sized
            // to soak its share of 1000 concurrent VUs without queueing.
            db_write_pool_size: env_or("DB_WRITE_POOL_SIZE", "60")
                .parse()
                .expect("DB_WRITE_POOL_SIZE must be a number"),
            db_read_pool_size: env_or("DB_READ_POOL_SIZE", "80")
                .parse()
                .expect("DB_READ_POOL_SIZE must be a number"),

            // Tighter outer→inner timeouts: under load we'd rather fail
            // fast (and let the client retry / get a 503) than block a
            // request thread for 5–30 s.
            db_query_timeout_secs: env_or("DB_QUERY_TIMEOUT_SECS", "2")
                .parse()
                .expect("DB_QUERY_TIMEOUT_SECS must be a number"),
            redis_command_timeout_secs: env_or("REDIS_COMMAND_TIMEOUT_SECS", "1")
                .parse()
                .expect("REDIS_COMMAND_TIMEOUT_SECS must be a number"),
            api_timeout_secs: env_or("API_TIMEOUT_SECS", "10")
                .parse()
                .expect("API_TIMEOUT_SECS must be a number"),

            redis_url: env_or("REDIS_URL", "redis://127.0.0.1:6379"),
            redis_read_url: std::env::var("REDIS_READ_URL").ok(),
            redis_pool_size: env_or("REDIS_POOL_SIZE", "100")
                .parse()
                .expect("REDIS_POOL_SIZE must be a number"),

            redis_sentinel_nodes: parse_csv_env("REDIS_SENTINEL_NODES", ""),
            redis_sentinel_master_name: env_or(
                "REDIS_SENTINEL_MASTER_NAME",
                "peakload-master",
            ),
            redis_sentinel_monitor_interval_secs: env_or(
                "REDIS_SENTINEL_MONITOR_INTERVAL_SECS",
                "5",
            )
            .parse()
            .expect("REDIS_SENTINEL_MONITOR_INTERVAL_SECS must be a number"),

            db_health_check_interval_secs: env_or("DB_HEALTH_CHECK_INTERVAL_SECS", "5")
                .parse()
                .expect("DB_HEALTH_CHECK_INTERVAL_SECS must be a number"),
            // Defaults tuned for the Patroni promotion window
            // (~5–15s, bounded by the etcd leader-lease TTL; see
            // docs/ha-architecture.md §2). With the linear backoff in
            // src/db/failover.rs (sleep = backoff_ms * attempt),
            // 6 × 200ms adds up to 200+400+600+800+1000+1200 ≈ 4.2s of
            // retry, which soaks the typical HAProxy-flip window. Requests
            // outliving the window fail and rely on the HTTP caller to retry.
            db_write_retry_max_attempts: env_or("DB_WRITE_RETRY_MAX_ATTEMPTS", "6")
                .parse()
                .expect("DB_WRITE_RETRY_MAX_ATTEMPTS must be a number"),
            db_write_retry_backoff_ms: env_or("DB_WRITE_RETRY_BACKOFF_MS", "200")
                .parse()
                .expect("DB_WRITE_RETRY_BACKOFF_MS must be a number"),

            rabbitmq_url: env_or(
                "RABBITMQ_URL",
                "amqp://peakload_user:peakload_secure_pass@localhost:5672",
            ),

            // Per-IP limiter ceiling. Old defaults (10 000 / 20 000)
            // were modelled for many distinct clients; under a
            // single-source load test the entire k6 fleet shares
            // one IP and routinely overshoots, producing the
            // `not rate limited` failures seen in the spike
            // scenario. Bumped so the limiter is effectively a
            // safety belt during benchmarks; keep an eye on it
            // before exposing the service publicly.
            rate_limit_per_second: env_or("RATE_LIMIT_PER_SECOND", "100000")
                .parse()
                .expect("RATE_LIMIT_PER_SECOND must be a number"),
            rate_limit_burst: env_or("RATE_LIMIT_BURST", "200000")
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

            // Capped at the realistic concurrent envelope of 4×app
            // replicas: queueing 20 000 requests behind a saturated
            // pipeline only inflates p99 by the queue-drain time —
            // returning 503 fast is strictly better. Old value
            // (20 000) is left here as a comment so we remember
            // why the dial was turned down.
            max_concurrent_requests: env_or("MAX_CONCURRENT_REQUESTS", "2000")
                .parse()
                .expect("MAX_CONCURRENT_REQUESTS must be a number"),

            cors_allowed_origins: parse_csv_env("CORS_ALLOWED_ORIGINS", "*"),

            enable_auth: env_or("ENABLE_AUTH", "false")
                .parse()
                .unwrap_or(false),
            auth_secret: std::env::var("AUTH_SECRET").ok(),

            verify_from_account_exists: env_or("TX_VERIFY_FROM_ACCOUNT", "false")
                .parse()
                .unwrap_or(false),

            backpressure_wait_ms: env_or("BACKPRESSURE_WAIT_MS", "50")
                .parse()
                .expect("BACKPRESSURE_WAIT_MS must be a number"),
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

        // Failover
        ensure!(
            self.db_health_check_interval_secs > 0,
            "DB_HEALTH_CHECK_INTERVAL_SECS must be > 0"
        );
        ensure!(
            self.db_write_retry_max_attempts >= 1,
            "DB_WRITE_RETRY_MAX_ATTEMPTS must be >= 1"
        );
        ensure!(
            self.redis_sentinel_monitor_interval_secs > 0,
            "REDIS_SENTINEL_MONITOR_INTERVAL_SECS must be > 0"
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
            "  cors_allowed_origins:         {:?}",
            self.cors_allowed_origins
        )?;
        writeln!(f, "  enable_auth:                  {}", self.enable_auth)?;
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
            redis_sentinel_nodes: vec![],
            redis_sentinel_master_name: "peakload-master".to_string(),
            redis_sentinel_monitor_interval_secs: 5,
            db_health_check_interval_secs: 5,
            db_write_retry_max_attempts: 3,
            db_write_retry_backoff_ms: 50,
            rabbitmq_url: "amqp://user:pass@localhost:5672".to_string(),
            rate_limit_per_second: 10000,
            rate_limit_burst: 20000,
            circuit_breaker_failure_threshold: 50,
            circuit_breaker_recovery_timeout_secs: 10,
            max_concurrent_requests: 20000,
            cors_allowed_origins: vec!["*".to_string()],
            enable_auth: false,
            auth_secret: None,
            verify_from_account_exists: false,
            backpressure_wait_ms: 50,
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
            mask_url("postgres://peakload_user:peakload_secure_pass@host:5432/db"),
            "postgres://peakload_user:****@host:5432/db"
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
