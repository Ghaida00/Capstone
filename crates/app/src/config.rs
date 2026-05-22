use std::env;
use std::fmt;

pub use transactions::IdempotencyBackend;

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
    /// Disabled shard 2 — kept as `Option` after S-6 dropped
    /// credentialed defaults from `from_env`. `None` is the
    /// normal state (the compose service for shard 2 is
    /// commented out); re-enabling shard 2 requires the env var
    /// AND a corresponding pgBouncer/Patroni stack.
    pub database_shard2_write_url: Option<String>,
    pub db_write_pool_size: u32,
    pub db_read_pool_size: u32,

    // Timeouts (seconds). NOTE: `DB_QUERY_TIMEOUT_SECS` is NOT a
    // field on this struct. The Postgres statement timeout is
    // enforced server-side as an `ALTER DATABASE` default by
    // `db/bootstrap/bootstrap-schema.sh` (D-1/R-3 in the audit) —
    // required because the write path is pooled by pgBouncer in
    // transaction mode, which rejects client-supplied connection
    // options. The carried `db_query_timeout_secs` knob from the
    // pre-WP1b design was removed in WP10 because it was dead
    // config — never read by any pool constructor, but its
    // presence implied a control surface that did not exist.
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

    // R-9: startup degradation posture. One of normal |
    // read_only | essential_only. Runtime-flippable via
    // `PUT /api/v2/admin/degradation`; this is only the seed.
    pub degradation_mode: String,

    // Authentication (disabled by default for load testing)
    pub enable_auth: bool,
    pub auth_secret: Option<String>,
    /// S-2: required when ENABLE_AUTH=true — the validator rejects
    /// tokens whose `iss` does not match. `None` allowed only when
    /// auth is off (the validator is never constructed).
    pub auth_expected_issuer: Option<String>,
    /// S-2: same shape for the `aud` claim.
    pub auth_expected_audience: Option<String>,
    /// S-2: tunable JWT clock-skew leeway (the lib default is 60 s
    /// — tuned down here to 30 s and made explicit so the choice
    /// is reader-visible rather than a library implementation
    /// detail).
    pub auth_leeway_secs: u64,

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

    /// Selects which `IdempotencyAwareWriter` impl the POST handler
    /// uses to commit a reservation. See
    /// `crates/transactions/src/infrastructure/redis_idempotency.rs`
    /// for the Redis-async path.
    ///   * `pg` (legacy) — synchronous PG INSERT into
    ///     `idempotency_keys`, the durability story unchanged.
    ///   * `redis` — Redis SETNX on the hot path; PG INSERT happens
    ///     in a background `RedisIntakeWorker` per shard. Lowest
    ///     POST latency. Requires Redis AOF + replica.
    ///   * `hybrid` (default) — try Redis first; on Redis error
    ///     fall back to the PG path so a Redis outage degrades
    ///     latency rather than rejecting requests.
    pub idempotency_backend: IdempotencyBackend,

    /// Number of redis-intake workers to run per shard. Each worker
    /// is an independent serial drain of that shard's
    /// `idempotency:pending` list; running several multiplies stage-2
    /// publish throughput. Atomic `RPOPLPUSH` makes the workers pull
    /// disjoint keys, so no coordination is needed. Env:
    /// `REDIS_INTAKE_CONCURRENCY`, default 4.
    pub redis_intake_concurrency: usize,
}

/// Parse `IDEMPOTENCY_BACKEND` from env. Defaults to `hybrid` and
/// panics on any unknown value so misconfigurations fail fast at
/// startup rather than landing requests on the wrong code path.
fn idempotency_backend_from_env() -> IdempotencyBackend {
    let raw = env_or("IDEMPOTENCY_BACKEND", "hybrid");
    IdempotencyBackend::parse(&raw).unwrap_or_else(|other| {
        panic!(
            "IDEMPOTENCY_BACKEND must be one of: pg, redis, hybrid (got: {})",
            other
        )
    })
}

impl Config {
    /// `rate_limit_per_second` at or above this is effectively no
    /// limit for human traffic (it only ever trips a deliberate
    /// load generator). `validate()` emits a startup WARN so an
    /// operator running with a benchmark override in production
    /// sees it in the logs rather than discovering it during an
    /// abuse incident. 50 000 r/s/IP ≈ 4.3 billion requests/day
    /// from one source — far past any legitimate client.
    pub const RATE_LIMIT_WARN_THRESHOLD: u64 = 50_000;

    /// True when the per-IP limiter is set so high it is a no-op
    /// for real traffic (R-5). Extracted as a predicate so the
    /// threshold is unit-testable without capturing log output.
    pub fn rate_limit_effectively_off(&self) -> bool {
        self.rate_limit_per_second >= Self::RATE_LIMIT_WARN_THRESHOLD
    }

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

            // S-6: credentialed defaults removed — env vars are
            // REQUIRED. The compose stack supplies them via .env
            // (which substitutes ${POSTGRES_USER}/${POSTGRES_PASSWORD}
            // into each URL), so the running stack continues to
            // boot; a deployment that forgets to set them fails
            // closed at process start rather than silently embedding
            // the demo password. Pairs with the Twelve-Factor "open-
            // source-the-code without leaking credentials" litmus
            // test (audit S-6).
            //
            // Shard 0
            database_shard0_write_url: must_env("DATABASE_SHARD0_WRITE_URL"),
            database_shard0_read_urls: must_csv_env("DATABASE_SHARD0_READ_URLS"),
            // Shard 1
            database_shard1_write_url: must_env("DATABASE_SHARD1_WRITE_URL"),
            database_shard1_read_urls: must_csv_env("DATABASE_SHARD1_READ_URLS"),
            // Shard 2 — disabled in compose; .env does not define
            // the URL. Optional so unset is the normal case, not
            // a boot failure. Re-enabling shard 2 requires setting
            // the env var alongside the corresponding compose service.
            database_shard2_write_url: std::env::var("DATABASE_SHARD2_WRITE_URL").ok(),
            // Pool sizes raised: previous defaults bottlenecked at the
            // app↔pgBouncer hop. With 4 replicas each pool is now sized
            // to soak its share of 1000 concurrent VUs without queueing.
            db_write_pool_size: env_or("DB_WRITE_POOL_SIZE", "100")
                .parse()
                .expect("DB_WRITE_POOL_SIZE must be a number"),
            db_read_pool_size: env_or("DB_READ_POOL_SIZE", "80")
                .parse()
                .expect("DB_READ_POOL_SIZE must be a number"),

            // Tighter outer→inner timeouts: under load we'd rather fail
            // fast (and let the client retry / get a 503) than block a
            // request thread for 5–30 s. (Postgres statement_timeout
            // is server-side now — see field comment above.)
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
            redis_sentinel_master_name: env_or("REDIS_SENTINEL_MASTER_NAME", "peakload-master"),
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

            // S-6: same fail-closed treatment as the DB URLs.
            rabbitmq_url: must_env("RABBITMQ_URL"),

            // Per-IP limiter ceiling. R-5: the *default* is the
            // production-safe value; benchmarks override it via
            // env (the load-test `.env` / compose sets a high
            // value explicitly). Shipping the bench value as the
            // default is "dangerous defaults, safe overrides" —
            // the limiter is then effectively off in every
            // environment where someone forgot to set the env var,
            // which is the normal case, not the exception. 2000
            // r/s per IP is a defensible starting point for this
            // API; `validate()` WARNs if it is ever raised to a
            // level where the limiter is a no-op for human traffic
            // (see RATE_LIMIT_WARN_THRESHOLD).
            rate_limit_per_second: env_or("RATE_LIMIT_PER_SECOND", "2000")
                .parse()
                .expect("RATE_LIMIT_PER_SECOND must be a number"),
            rate_limit_burst: env_or("RATE_LIMIT_BURST", "4000")
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

            // S-3: default is empty, NOT "*". `validate()` rejects
            // an empty allowlist so the deployment must make the
            // wildcard decision explicit. Existing `.env` files
            // already set `CORS_ALLOWED_ORIGINS=*` (running stack)
            // — no regression; `validate()` additionally emits a
            // WARN when the resolved list contains `*` so a
            // production deploy that left the dev wildcard in
            // place is visible in logs.
            cors_allowed_origins: parse_csv_env("CORS_ALLOWED_ORIGINS", ""),

            degradation_mode: env_or("DEGRADATION_MODE", "normal"),

            enable_auth: env_or("ENABLE_AUTH", "false").parse().unwrap_or(false),
            auth_secret: std::env::var("AUTH_SECRET").ok(),
            auth_expected_issuer: std::env::var("AUTH_EXPECTED_ISSUER")
                .ok()
                .filter(|s| !s.is_empty()),
            auth_expected_audience: std::env::var("AUTH_EXPECTED_AUDIENCE")
                .ok()
                .filter(|s| !s.is_empty()),
            auth_leeway_secs: env_or("AUTH_LEEWAY_SECS", "30")
                .parse()
                .expect("AUTH_LEEWAY_SECS must be a number"),

            verify_from_account_exists: env_or("TX_VERIFY_FROM_ACCOUNT", "false")
                .parse()
                .unwrap_or(false),

            backpressure_wait_ms: env_or("BACKPRESSURE_WAIT_MS", "50")
                .parse()
                .expect("BACKPRESSURE_WAIT_MS must be a number"),

            idempotency_backend: idempotency_backend_from_env(),

            redis_intake_concurrency: env_or("REDIS_INTAKE_CONCURRENCY", "4")
                .parse()
                .expect("REDIS_INTAKE_CONCURRENCY must be a number"),
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

        // Timeouts — downstream timeouts must be shorter than the
        // API timeout. (Postgres statement_timeout is server-side
        // now and not represented in this struct — see the field
        // comment in the timeouts block.)
        ensure!(
            self.redis_command_timeout_secs > 0,
            "REDIS_COMMAND_TIMEOUT_SECS must be > 0"
        );
        ensure!(self.api_timeout_secs > 0, "API_TIMEOUT_SECS must be > 0");
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

        // S-2: when JWT enforcement is on, the validator REQUIRES
        // expected issuer + audience values — without them every
        // token's iss/aud check is implicitly skipped (or rejects
        // every token, depending on lib defaults). Fail-closed at
        // boot rather than discovering at first auth'd request.
        if self.enable_auth {
            ensure!(
                self.auth_secret.as_deref().is_some_and(|s| !s.is_empty()),
                "AUTH_SECRET must be set (and non-empty) when ENABLE_AUTH=true"
            );
            ensure!(
                self.auth_expected_issuer.is_some(),
                "AUTH_EXPECTED_ISSUER must be set (and non-empty) when ENABLE_AUTH=true (S-2)"
            );
            ensure!(
                self.auth_expected_audience.is_some(),
                "AUTH_EXPECTED_AUDIENCE must be set (and non-empty) when ENABLE_AUTH=true (S-2)"
            );
        }

        // S-3: CORS allowlist must be explicitly set. An empty
        // list is the unsafe-by-default state (would either reject
        // every cross-origin browser request OR — in the old wide-
        // open default — accept every one). Force the deployment
        // to make the decision.
        ensure!(
            !self.cors_allowed_origins.is_empty(),
            "CORS_ALLOWED_ORIGINS must be set (non-empty list, or \"*\" for development)"
        );
        if self.cors_allowed_origins.iter().any(|o| o == "*") {
            tracing::warn!(
                origins = ?self.cors_allowed_origins,
                "CORS_ALLOWED_ORIGINS contains \"*\" — every origin is allowed. \
                 Expected only in development; restrict to an explicit allowlist \
                 in production (S-3)."
            );
        }

        // R-9: degradation posture must be a recognised value, or
        // the process would silently fall back to Normal and an
        // operator who set DEGRADATION_MODE=read_only for a
        // maintenance window would be serving writes anyway.
        ensure!(
            crate::degradation::DegradationMode::parse(&self.degradation_mode).is_some(),
            "DEGRADATION_MODE must be one of: normal | read_only | essential_only (got {:?})",
            self.degradation_mode
        );

        // Rate limiting
        ensure!(
            self.rate_limit_per_second > 0,
            "RATE_LIMIT_PER_SECOND must be > 0"
        );
        // R-5: not a hard failure (benchmarks legitimately raise
        // it) but it must never pass silently — an unbounded
        // limiter in production is a security gap, not a config
        // typo.
        if self.rate_limit_effectively_off() {
            tracing::warn!(
                rate_limit_per_second = self.rate_limit_per_second,
                threshold = Self::RATE_LIMIT_WARN_THRESHOLD,
                "RATE_LIMIT_PER_SECOND is at or above the no-op threshold — the per-IP \
                 limiter is effectively OFF for human traffic. Expected only on a \
                 load-test host; if this is production, an override leaked."
            );
        }

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

        // Stage-2 intake worker fan-out. Lower bound 1 (zero workers
        // would silently strand the pending list); upper bound 64
        // guards a typo'd value from oversubscribing the PG pool and
        // AMQP channels.
        ensure!(
            self.redis_intake_concurrency >= 1 && self.redis_intake_concurrency <= 64,
            "REDIS_INTAKE_CONCURRENCY must be 1-64, got {}",
            self.redis_intake_concurrency
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
        writeln!(
            f,
            "  degradation_mode:             {}",
            self.degradation_mode
        )?;
        writeln!(f, "  enable_auth:                  {}", self.enable_auth)?;
        writeln!(
            f,
            "  auth_expected_issuer:         {}",
            self.auth_expected_issuer.as_deref().unwrap_or("(unset)")
        )?;
        writeln!(
            f,
            "  auth_expected_audience:       {}",
            self.auth_expected_audience.as_deref().unwrap_or("(unset)")
        )?;
        writeln!(
            f,
            "  auth_leeway_secs:             {}",
            self.auth_leeway_secs
        )?;
        writeln!(
            f,
            "  idempotency_backend:          {}",
            self.idempotency_backend
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
            self.database_shard2_write_url
                .as_deref()
                .map(mask_url)
                .unwrap_or_else(|| "(unset — shard 2 disabled)".to_string())
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
        writeln!(
            f,
            "  redis_intake_concurrency:     {}",
            self.redis_intake_concurrency
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

/// S-6: env var with NO compiled-in fallback. Aborts process
/// startup if unset — the failure message names the variable so
/// the operator sees it in stderr instead of an opaque "boot
/// failed".
fn must_env(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| {
        panic!("{key} must be set (S-6: no credentialed defaults; see .env.example)")
    })
}

/// S-6: comma-separated env var with NO fallback (same fail-closed
/// posture as `must_env` for the read-replica URL lists).
fn must_csv_env(key: &str) -> Vec<String> {
    must_env(key)
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
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
            database_shard2_write_url: Some("postgres://user:pass@host:5432/db".to_string()),
            db_write_pool_size: 30,
            db_read_pool_size: 50,
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
            rabbitmq_url: "amqp://user:pass@localhost:5672".to_string(),
            rate_limit_per_second: 10000,
            rate_limit_burst: 20000,
            circuit_breaker_failure_threshold: 50,
            circuit_breaker_recovery_timeout_secs: 10,
            max_concurrent_requests: 20000,
            cors_allowed_origins: vec!["*".to_string()],
            degradation_mode: "normal".to_string(),
            enable_auth: false,
            auth_secret: None,
            auth_expected_issuer: None,
            auth_expected_audience: None,
            auth_leeway_secs: 30,
            verify_from_account_exists: false,
            backpressure_wait_ms: 50,
            idempotency_backend: IdempotencyBackend::Pg,
            redis_intake_concurrency: 4,
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
    fn empty_cors_allowlist_fails_validation() {
        // S-3: an unset CORS_ALLOWED_ORIGINS env var must abort
        // boot rather than silently default to "deny everything"
        // (or, worse, the prior "*" default).
        let mut cfg = test_config();
        cfg.cors_allowed_origins = vec![];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn wildcard_cors_allowlist_validates_with_warn() {
        // S-3: "*" is a legitimate dev/test setting; validate()
        // accepts it but emits a startup WARN (side-effect not
        // captured here — covered by the integration boot probe).
        let mut cfg = test_config();
        cfg.cors_allowed_origins = vec!["*".to_string()];
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn safe_rate_limit_not_flagged_as_effectively_off() {
        let mut cfg = test_config();
        cfg.rate_limit_per_second = 2000; // the new production-safe default
        assert!(!cfg.rate_limit_effectively_off());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn benchmark_rate_limit_is_flagged_but_still_valid() {
        // R-5: a benchmark override is allowed (validate still Ok —
        // it is a WARN, not a hard failure) but MUST be flagged so
        // it cannot pass silently into production.
        let mut cfg = test_config();
        cfg.rate_limit_per_second = 100_000;
        assert!(cfg.rate_limit_effectively_off());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn rate_limit_warn_threshold_boundary() {
        let mut cfg = test_config();
        cfg.rate_limit_per_second = Config::RATE_LIMIT_WARN_THRESHOLD - 1;
        assert!(!cfg.rate_limit_effectively_off());
        cfg.rate_limit_per_second = Config::RATE_LIMIT_WARN_THRESHOLD;
        assert!(cfg.rate_limit_effectively_off());
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

    #[test]
    fn zero_redis_intake_concurrency_fails() {
        let mut cfg = test_config();
        cfg.redis_intake_concurrency = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn over_max_redis_intake_concurrency_fails() {
        let mut cfg = test_config();
        cfg.redis_intake_concurrency = 65;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn default_redis_intake_concurrency_passes() {
        let mut cfg = test_config();
        cfg.redis_intake_concurrency = 4;
        assert!(cfg.validate().is_ok());
    }
}
