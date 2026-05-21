use std::sync::Arc;
use std::time::Duration;

use axum::{
    error_handling::HandleErrorLayer,
    http::{HeaderValue, StatusCode},
    middleware as axum_middleware,
    routing::get,
    BoxError, Router,
};
use tokio_util::sync::CancellationToken;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;

use crate::config::Config;
use crate::middleware::backpressure::BackpressureController;
use crate::middleware::circuit_breaker::CircuitBreaker;
use crate::middleware::rate_limit::RateLimiter;
use crate::AppState;
use shared_kernel::cache::redis::{RedisCache, RedisCacheConfig};
use shared_kernel::db::shard::{ShardRouter, ShardRouterConfig, ShardUrls};
use shared_kernel::events::EventSubscriber;
use shared_kernel::queue::producer::QueueProducer;

// ─── Tracing ────────────────────────────────────────────────────

/// Initialise the global `tracing` subscriber.
///
/// Default level is `info`; `RUST_LOG` overrides it (e.g. `RUST_LOG=debug`).
/// Default output is JSON on stdout — one event per line, structured fields
/// preserved.
/// Plain compact format is used when `RUST_LOG_PRETTY` is set or the binary
/// is built with `debug_assertions` (i.e. `cargo run` / `cargo test`).
///
/// When `OTEL_EXPORTER_OTLP_ENDPOINT` is set and non-blank, an OTLP/gRPC
/// batch span exporter is layered onto the subscriber and registered as
/// the global tracer provider. `OTEL_SERVICE_NAME` (default
/// `peakload-capstone`) is attached as the `service.name` resource
/// attribute. With the endpoint unset or blank (the latter is what
/// Compose passes for a cleared variable), no OTLP pipeline runs and
/// the subscriber behaves as above.
///
/// Filter composition: a single `EnvFilter` from `RUST_LOG` (default
/// `info`) is applied as a Layer on the Registry. Both the OTel and fmt
/// layers see only events that pass that filter — h2/hyper/sqlx DEBUG
/// noise is dropped at the registry, never reaching the OTel hot path.
/// The OTel layer is NOT wrapped in `Filtered<_,_>` (per-layer
/// `.with_filter()`), because that wrapper hides the OTel layer's
/// `WithContext` from `subscriber.downcast_ref::<WithContext>()`,
/// breaking `tracing_opentelemetry::OpenTelemetrySpanExt::context()`
/// at the publish boundary (traceparent injection silently writes
/// nothing). This is the canonical `tracing-opentelemetry` pattern.
/// Resolve the OTLP exporter endpoint from its raw env value. Tracing
/// exports only when the endpoint is present and non-blank; both an
/// unset variable and an empty/whitespace one (what Compose passes for
/// a cleared variable) yield `None`, leaving the OTLP pipeline off.
fn resolve_otlp_endpoint(raw: Option<String>) -> Option<String> {
    raw.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

pub fn init_tracing() {
    let log_directives = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let pretty = std::env::var("RUST_LOG_PRETTY").is_ok() || cfg!(debug_assertions);

    let global_filter =
        EnvFilter::try_new(&log_directives).unwrap_or_else(|_| EnvFilter::new("info"));

    let otel_layer = resolve_otlp_endpoint(std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
        .map(|endpoint| {
            let exporter = SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .expect("OTLP SpanExporter build");
            let service_name = std::env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| "peakload-capstone".to_string());
            let resource = Resource::builder()
                .with_attribute(KeyValue::new("service.name", service_name))
                .build();
            let provider = SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                .build();
            let tracer = provider.tracer("peakload-capstone");
            opentelemetry::global::set_tracer_provider(provider);
            tracing_opentelemetry::layer().with_tracer(tracer)
        });

    if pretty {
        tracing_subscriber::registry()
            .with(global_filter)
            .with(otel_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_target(false)
                    .with_thread_ids(false)
                    .with_thread_names(false),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(global_filter)
            .with(otel_layer)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    }
}

// ─── Metrics ────────────────────────────────────────────────────

/// Install the Prometheus recorder and pre-register every metric so
/// they appear in `/metrics` from the very first scrape.
pub fn init_metrics() -> metrics_exporter_prometheus::PrometheusHandle {
    // O-4: render http_request_duration_seconds as a real histogram with
    // SLO-straddling buckets (the exporter defaults histograms to summary;
    // `histogram_quantile()` is impossible without buckets). Boundaries
    // straddle the stated 500 ms P95 SLO and the observed sub-10 ms baseline.
    // Scope is intentionally `Matcher::Full` (exact name): other
    // histograms (e.g. `transactions_batch_size`) remain summaries —
    // broadening to `Matcher::Prefix`/`Suffix` would change their
    // exposition shape silently.
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full("http_request_duration_seconds".to_string()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )
        .expect("set_buckets_for_metric")
        .install_recorder()
        .expect("Failed to install Prometheus recorder");

    // Describe
    metrics::describe_counter!("http_requests_total", "Total HTTP requests");
    metrics::describe_histogram!("http_request_duration_seconds", "HTTP request duration");
    metrics::describe_counter!("transactions_created_total", "Transactions created");
    metrics::describe_counter!("transactions_processed_total", "Transactions processed");
    metrics::describe_counter!("cache_hits_total", "Cache hits");
    metrics::describe_counter!("cache_misses_total", "Cache misses");
    metrics::describe_counter!("rate_limited_total", "Rate limited requests");
    metrics::describe_counter!(
        "rate_limiter_redis_sync_failures_total",
        "Rate-limiter Redis sync failures (label `kind`: `pool_get` = could not acquire a Redis connection; `pipeline` = the INCRBY/EXPIRE pipeline errored). Each failure means the global ceiling silently degraded to per-replica enforcement (effective limit = N × per_replica) until Redis recovers — page on sustained non-zero rate (R-6)."
    );
    metrics::describe_counter!("backpressure_shed_total", "Backpressure shed requests");
    metrics::describe_gauge!("backpressure_in_flight", "In-flight requests");
    metrics::describe_gauge!("circuit_breaker_state", "Circuit breaker state");
    metrics::describe_gauge!(
        "degradation_mode",
        "R-9 operator-controlled degradation posture: 0 normal, 1 read_only (writes 503'd, reads served), 2 essential_only. Monotonic in severity — alert on > 0."
    );
    metrics::describe_gauge!(
        "dependency_breaker_state",
        "R-7 per-dependency circuit-breaker state (label `dep`: db|redis|rabbitmq): 0 closed, 1 open (failing fast), 2 half-open (probing recovery). Distinct from the coarse circuit_breaker_state HTTP-edge breaker."
    );
    metrics::describe_counter!(
        "dependency_breaker_trips_total",
        "R-7 per-dependency breaker open transitions (label `dep`). Each increment = that upstream's error class crossed the failure threshold (or a half-open probe failed) and calls now fail fast with 503 + Retry-After until the recovery timeout."
    );
    metrics::describe_counter!(
        "background_task_panics_total",
        "A-3 background-task panic count (label `task`: which background spawn class panicked). Increments under panic=unwind when a JoinSet-tracked task panics — previously such panics were observed only by the default panic hook (silent). Alert on any non-zero rate."
    );
    metrics::describe_counter!(
        "idempotency_cache_set_attempts_total",
        "A-3 idempotency cache-set attempts (one per replay-write attempt; failures are tolerated by design — the next replay re-populates from the DB)."
    );
    metrics::describe_counter!("idempotency_hits_total", "Idempotency hits");
    metrics::describe_counter!("rabbitmq_reconnections_total", "RabbitMQ reconnections");
    metrics::describe_histogram!(
        "transactions_batch_size",
        "Per-shard consumer batch sizes (label `shard`: sender-shard index). ACK count per shard per flush (completed + failed + skipped). Aggregate distribution = histogram_quantile without the shard label; per-shard fault-domain view = by (shard) (O-10)."
    );
    metrics::describe_counter!("dlq_messages_total", "Dead letter queue messages");
    metrics::describe_counter!(
        "transactions_list_tail_skew_dropped_total",
        "Rows dropped from a transactions-list page because at least one shard's coverage did not reach them (D-3 safe-cursor truncation)"
    );
    metrics::describe_counter!(
        "transactions_find_by_id_shard_timeout_total",
        "Per-shard find_by_id fan-out timeouts (label `shard`: index of the slow shard) — a slow shard turned into a per-shard error rather than pinning the API wall-clock (D-4)"
    );
    metrics::describe_counter!(
        "db_read_fallback_to_primary_total",
        "Per-shard read-pool fallback to primary because every replica was marked unhealthy (label `shard`: index of the affected shard). Rate signal for the silent-degradation case (D-5); alert on sustained non-zero rate to catch replica outages before they amplify load on the primary."
    );
    metrics::describe_counter!(
        "cross_shard_step_failures_total",
        "Cross-shard outbox step failures (label `step`: `credit` or `refund`) — credit failures stranded a sender debit; refund failures left a sender un-compensated"
    );
    metrics::describe_counter!(
        "cross_shard_outbox_terminal_failures_total",
        "Cross-shard outbox terminal failures after MAX_ATTEMPTS retries (label `step`: `credit` only — refund path never terminal-fails by design). Each increment means a sender's audit row was transitioned 'processing' → 'failed' with `failure_reason` populated; the funds have been debited and the recipient credit never landed. Page on any non-zero rate (R-2). Reconciliation procedure: docs/runbooks/cross-shard-outbox-reconciliation.md."
    );
    metrics::describe_counter!(
        "cross_shard_refund_stuck_total",
        "Refund-path retry attempts while past MAX_ATTEMPTS (label-less). Sender is debited; the compensating refund has failed at least 10 times and the row is in a 5min defer-lease loop. Reads as 'retry attempts per stuck row × time' — not a count of distinct stuck rows. Ticket on sustained non-zero rate for 30+ min (R-2)."
    );

    // Failover metrics
    metrics::describe_counter!(
        "db_replica_failover_total",
        "Replica transitions healthy → unhealthy"
    );
    metrics::describe_counter!(
        "db_replica_recovered_total",
        "Replica transitions unhealthy → healthy"
    );
    metrics::describe_counter!("db_retry_attempt_total", "Transient DB errors retried");
    metrics::describe_counter!(
        "db_retry_success_total",
        "DB ops that succeeded after retry"
    );
    metrics::describe_counter!("db_retry_exhausted_total", "DB ops that exhausted retries");
    metrics::describe_counter!(
        "redis_master_failover_total",
        "Redis master address changes detected via Sentinel"
    );

    // Phase 3: shared_kernel event bus + notifications module.
    metrics::describe_counter!(
        "events_published_total",
        "Cross-module events published to shared_kernel::events bus"
    );
    metrics::describe_counter!(
        "events_publish_errors_total",
        "Failures publishing to shared_kernel::events bus"
    );
    metrics::describe_counter!(
        "events_build_errors_total",
        "Failures serialising an event payload before publish"
    );
    metrics::describe_counter!(
        "notifications_appended_total",
        "Notifications appended to the in-memory log by the dispatcher"
    );
    metrics::describe_counter!(
        "notifications_events_lagged_total",
        "Events the notifications dispatcher dropped due to lag on the broadcast bus"
    );
    metrics::describe_counter!(
        "notifications_payload_decode_errors_total",
        "Events the notifications dispatcher could not decode (malformed payload)"
    );

    // Redis-async idempotency reservation path (request side).
    metrics::describe_counter!(
        "idempotency_redis_reserved_total",
        "Reservations committed via the Redis-async path (SETNX succeeded)"
    );
    metrics::describe_counter!(
        "idempotency_redis_replay_total",
        "Replays detected on the Redis path (existing entry, matching request_hash)"
    );
    metrics::describe_counter!(
        "idempotency_redis_hash_conflict_total",
        "Hash mismatches on the Redis path (existing entry, different request_hash)"
    );
    metrics::describe_counter!(
        "idempotency_redis_ttl_race_total",
        "SETNX-conflict GET-miss races on the Redis path (TTL expired between ops)"
    );
    metrics::describe_counter!(
        "idempotency_redis_fallback_total",
        "Hybrid-mode fallthroughs from the Redis path to the PG path"
    );

    // Redis-intake background worker (drains the Tier-2 pending list).
    metrics::describe_counter!(
        "idempotency_redis_intake_failures_total",
        "Intake worker process_one failures (entry stays in inflight, retried)"
    );
    metrics::describe_counter!(
        "idempotency_redis_intake_errors_total",
        "Intake worker BRPOPLPUSH errors (Redis transient)"
    );
    metrics::describe_counter!(
        "idempotency_redis_intake_publish_failures_total",
        "Intake worker broker publish failures (lease cleared, publish_outbox retries)"
    );
    metrics::describe_counter!(
        "idempotency_redis_intake_published_total",
        "Outbox payloads successfully published by the intake worker"
    );

    // Outbox-publisher worker counters.
    metrics::describe_counter!(
        "publish_outbox_shipped_total",
        "Outbox rows successfully published and marked published=true"
    );
    metrics::describe_counter!(
        "publish_outbox_publish_failures_total",
        "Outbox publish failures (broker unhealthy; row stays leased for retry)"
    );
    metrics::describe_counter!(
        "publish_outbox_iteration_errors_total",
        "Outbox iteration errors (PG transient on Phase 1 claim)"
    );

    // AMQP channel-callback events (consumer + publisher sides).
    metrics::describe_counter!(
        "amqp_consumer_cancel_total",
        "Broker-initiated basic.cancel events (queue deleted, mirror failover, policy reset)"
    );
    metrics::describe_counter!(
        "amqp_channel_close_total",
        "Broker-initiated channel.close events (label `side`: consumer / publisher)"
    );
    metrics::describe_counter!(
        "amqp_flow_total",
        "Broker-initiated channel.flow requests (label `side`: consumer / publisher)"
    );
    metrics::describe_counter!(
        "amqp_publish_failed_total",
        "Outbox publish failed terminally (label `kind`: nack_or_returned, etc.)"
    );
    metrics::describe_counter!("amqp_publish_nack_total", "Broker NACKed a publish confirm");
    metrics::describe_counter!(
        "amqp_publish_return_total",
        "Broker returned a mandatory publish as unrouteable"
    );
    metrics::describe_counter!(
        "amqp_ack_failures_total",
        "Consumer ACK / NACK failures (label `kind`: ack, nack_requeue, nack_dlq)"
    );

    // Initialise to zero
    metrics::counter!("transactions_created_total").absolute(0);
    metrics::counter!("transactions_processed_total").absolute(0);
    metrics::counter!("cache_hits_total").absolute(0);
    metrics::counter!("cache_misses_total").absolute(0);
    metrics::counter!("rate_limited_total").absolute(0);
    metrics::counter!("backpressure_shed_total").absolute(0);
    metrics::counter!("idempotency_hits_total").absolute(0);
    metrics::counter!("rabbitmq_reconnections_total").absolute(0);
    metrics::counter!("dlq_messages_total").absolute(0);
    metrics::gauge!("backpressure_in_flight").set(0.0);
    metrics::gauge!("circuit_breaker_state").set(0.0);
    // Histogram intentionally not zero-initialised: a synthetic 0
    // sample skews p50 toward 0 until enough real data lands.

    handle
}

// ─── Infrastructure ─────────────────────────────────────────────

/// Intermediate struct holding all initialised infrastructure before
/// it is assembled into the final `AppState` + middleware.
pub struct Infrastructure {
    pub shard_router: ShardRouter,
    pub cache: RedisCache,
    pub queue_producer: QueueProducer,
    pub rate_limiter: RateLimiter,
    pub circuit_breaker: CircuitBreaker,
    pub backpressure: BackpressureController,
}

/// Create all infrastructure resources (DB shards, Redis, RabbitMQ,
/// middleware components).
///
/// Fix #16: `cancel` token is passed to `RateLimiter` so its background
/// tasks can shut down gracefully.
pub async fn init_infrastructure(
    config: &Config,
    cancel: CancellationToken,
) -> anyhow::Result<Infrastructure> {
    tracing::info!("Connecting to database shards...");
    let shard_config = ShardRouterConfig {
        shards: vec![
            ShardUrls {
                write_url: config.database_shard0_write_url.clone(),
                read_urls: config.database_shard0_read_urls.clone(),
            },
            ShardUrls {
                write_url: config.database_shard1_write_url.clone(),
                read_urls: config.database_shard1_read_urls.clone(),
            },
            // Shard 2 disabled — see "shard 2 disabled" markers in
            // docker-compose.yml / haproxy.cfg / shard.rs. Re-enabling
            // requires re-adding `database_shard2_read_urls` to Config
            // and the env-parsing block, then a ShardUrls entry here.
        ],
        write_pool_size: config.db_write_pool_size,
        read_pool_size: config.db_read_pool_size,
        health_check_interval_secs: config.db_health_check_interval_secs,
    };
    let shard_router = ShardRouter::new(&shard_config, cancel.child_token()).await?;

    tracing::info!("Connecting to Redis...");
    let redis_config = RedisCacheConfig {
        master_url: config.redis_url.clone(),
        read_url: config.redis_read_url.clone(),
        pool_size: config.redis_pool_size,
        sentinel_nodes: config.redis_sentinel_nodes.clone(),
        sentinel_master_name: config.redis_sentinel_master_name.clone(),
        sentinel_monitor_interval_secs: config.redis_sentinel_monitor_interval_secs,
    };
    let cache = RedisCache::new(&redis_config, cancel.child_token()).await?;

    tracing::info!("Connecting to RabbitMQ...");
    let queue_producer = QueueProducer::new(&config.rabbitmq_url).await?;

    let rate_limiter = RateLimiter::new(
        cache.master_pool_handle(),
        config.rate_limit_per_second,
        config.rate_limit_burst,
        cancel,
    );
    let circuit_breaker = CircuitBreaker::new(
        config.circuit_breaker_failure_threshold,
        config.circuit_breaker_recovery_timeout_secs,
    );
    let backpressure =
        BackpressureController::new(config.max_concurrent_requests, config.backpressure_wait_ms);

    Ok(Infrastructure {
        shard_router,
        cache,
        queue_producer,
        rate_limiter,
        circuit_breaker,
        backpressure,
    })
}

// ─── Router ─────────────────────────────────────────────────────

/// Build the full Axum `Router` from the given `AppState` and
/// middleware components.
///
/// Exposed as a standalone function so it can be reused by integration
/// tests (via `tower::ServiceExt::oneshot`) without binding a TCP port.
pub fn build_router(
    state: AppState,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    backpressure: BackpressureController,
    event_subscriber: Arc<dyn EventSubscriber>,
    notifications_cancel: CancellationToken,
    config: &Config,
) -> Router {
    // Fix #18: Optional JWT auth, disabled by default. When enabled we
    // install the decoding key once and then attach the middleware —
    // otherwise the middleware is still attached but in pass-through mode
    // so we have a single code path and tests never need to special-case.
    if config.enable_auth {
        // S-1 / S-2: validate() already guarantees secret +
        // expected_issuer + expected_audience are set when
        // ENABLE_AUTH=true, so these unwraps are unreachable
        // defence-in-depth.
        let secret = config.auth_secret.as_deref().unwrap_or("");
        let issuer = config.auth_expected_issuer.as_deref().unwrap_or("");
        let audience = config.auth_expected_audience.as_deref().unwrap_or("");
        crate::middleware::auth::init_auth(&crate::middleware::auth::AuthConfig {
            secret,
            expected_issuer: issuer,
            expected_audience: audience,
            leeway_secs: config.auth_leeway_secs,
        });
        tracing::info!(
            expected_issuer = issuer,
            expected_audience = audience,
            leeway_secs = config.auth_leeway_secs,
            "JWT auth middleware enabled (HS256-pinned; iss/aud/exp/sub required) [S-1/S-2]"
        );
    } else {
        tracing::info!("JWT auth middleware disabled (ENABLE_AUTH=false)");
    }
    let auth_state = crate::middleware::auth::AuthState {
        enabled: config.enable_auth,
    };
    // Separate clone for the admin sub-router (R-2). Kept distinct
    // here so a future change to the v2 protection-stack auth
    // policy does not silently relax the admin gate.
    let auth_state_admin = auth_state.clone();

    // R-9: the degradation flag is shared by the write-path
    // middleware (every v2 sub-router) and the admin flip
    // endpoints (via AppState). Clone the handle out before the
    // sub-routers consume `state`.
    let degradation = state.degradation.clone();

    // Fix #10: Build CORS layer from configuration
    let cors = build_cors_layer(config);

    // ─── Modular-monolith routers ───────────────────────────
    //
    // After Step B (the v1 cull) the only HTTP surface is the
    // three v2 sub-routers + `/health` + `/metrics`. Each
    // sub-router carries the same protection stack
    // (auth → rate-limit → circuit-breaker → backpressure) via
    // `apply_protection_stack`, so a v2 client experiences
    // identical 401/429/503 semantics across modules.
    let accounts_deps = accounts::init(state.shard_router.clone(), state.cache.clone());
    let accounts_router = apply_protection_stack(
        accounts::router(accounts_deps.clone()),
        auth_state.clone(),
        rate_limiter.clone(),
        circuit_breaker.clone(),
        backpressure.clone(),
        degradation.clone(),
    );

    // Phase 2: transactions module wired with the cross-module
    // dep injected (transactions → accounts), even though the
    // current use cases do not yet exercise it. See module-level
    // comment in `src/modules/transactions/mod.rs`.
    let transactions_deps = transactions::init(
        state.shard_router.clone(),
        state.cache.clone(),
        accounts_deps.service.clone(),
        config.verify_from_account_exists,
        config.idempotency_backend,
    );
    let transactions_router = apply_protection_stack(
        transactions::router(transactions_deps),
        auth_state.clone(),
        rate_limiter.clone(),
        circuit_breaker.clone(),
        backpressure.clone(),
        degradation.clone(),
    );

    // ─── Phase 3 modular-monolith: mount `/api/v2/notifications/*` ───
    //
    // The notifications module owns its own HTTP surface and a
    // long-running event-dispatch task. `init` returns the deps
    // bundle AND a `JoinHandle` for the dispatcher; we drop the
    // handle here because the dispatcher honours its
    // `CancellationToken` and exits during graceful shutdown along
    // with every other subsystem.
    //
    // Same protection stack as the other v2 sub-routers — auth,
    // rate-limit, circuit breaker, backpressure all apply.
    let (notifications_deps, _notifications_handle) =
        notifications::init(event_subscriber, notifications_cancel);
    let notifications_router = apply_protection_stack(
        notifications::router(notifications_deps),
        auth_state,
        rate_limiter,
        circuit_breaker,
        backpressure,
        degradation,
    );

    // `nest_service` (rather than `nest`) because each module
    // router is already fully stateful (`.with_state(deps)` was
    // applied inside `*::router`) and thus its state type is
    // `()`, whereas the parent router carries `AppState`. The
    // `nest_service` method accepts any `Service`, bridging the
    // mismatch without forcing module deps to implement
    // `FromRef<AppState>`.
    // R-2: admin sub-router for operator queries on cross-shard
    // outbox state and audit-row 'processing' rows. Uses
    // `require_admin_middleware` (refuses outright when
    // `ENABLE_AUTH=false`, requires JWT `role="admin"` when on)
    // instead of the v2 protection stack — the operator surface
    // does not need rate-limit / circuit-breaker / backpressure
    // gating (low traffic, already authorised, no customer-impacting
    // throughput concern). Carries `AppState` directly so the
    // handlers can reach `state.shard_router`.
    let admin_router = crate::admin::router().layer(axum_middleware::from_fn_with_state(
        auth_state_admin,
        crate::middleware::auth::require_admin_middleware,
    ));

    Router::new()
        .nest("/api/v2/admin", admin_router)
        .nest_service("/api/v2/accounts", accounts_router)
        .nest_service("/api/v2/transactions", transactions_router)
        .nest_service("/api/v2/notifications", notifications_router)
        .route("/health", get(crate::health::health_check))
        .route("/metrics", get(crate::health::prometheus_metrics))
        .with_state(state)
        .layer(axum_middleware::from_fn(
            crate::middleware::request_id::request_id_middleware,
        ))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|err: BoxError| async move {
                    if err.is::<tower::timeout::error::Elapsed>() {
                        (
                            StatusCode::REQUEST_TIMEOUT,
                            "Request took too long".to_string(),
                        )
                    } else {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unhandled internal error: {}", err),
                        )
                    }
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(
                    config.api_timeout_secs,
                ))),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &axum::http::Request<_>| {
                    tracing::info_span!(
                        "http.request",
                        http.request.method = %req.method(),
                        url.path = %req.uri().path(),
                        request_id = tracing::field::Empty,
                        http.response.status_code = tracing::field::Empty,
                    )
                })
                .on_response(
                    |res: &axum::http::Response<_>,
                     _latency: std::time::Duration,
                     span: &tracing::Span| {
                        span.record("http.response.status_code", res.status().as_u16());
                    },
                ),
        )
        .layer(cors)
}

/// Apply the standard request-protection stack to a router.
///
/// Order matters: `.layer` wraps outside-in, so the call sequence below
/// produces this request flow at runtime. The metrics layer is placed
/// outermost so it observes the FINAL response status — including
/// 429/503 short-circuits emitted by rate-limit / circuit-breaker /
/// backpressure inside the protection stack.
///
/// ```text
///   request → metrics → backpressure → circuit_breaker → rate_limit → auth → handler
/// ```
///
/// Applied to every `/api/v2/{accounts,transactions,notifications}/*`
/// sub-router so they all share identical 401/429/503 semantics.
/// (Originally also applied to the legacy `/api/v1/*` routes; those
/// were removed in the Step-B v1 cull.) Generic over the router
/// state so each module's `router()` — which already applied its
/// own deps state and returns `Router<()>` — composes cleanly under
/// `nest_service`.
fn apply_protection_stack<S>(
    router: Router<S>,
    auth_state: crate::middleware::auth::AuthState,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    backpressure: BackpressureController,
    degradation: crate::degradation::DegradationFlag,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(axum_middleware::from_fn(
            crate::middleware::metrics::metrics_middleware,
        ))
        // R-9: write-path degradation gate. Placed just inside the
        // metrics layer so a degraded-mode 503 is still RED-counted,
        // but outside auth/rate-limit/breaker so a read-only window
        // sheds writes before they consume any of those budgets.
        .layer(axum_middleware::from_fn_with_state(
            degradation,
            crate::degradation::degradation_middleware,
        ))
        .layer(axum_middleware::from_fn_with_state(
            auth_state,
            crate::middleware::auth::auth_middleware,
        ))
        .layer(axum_middleware::from_fn_with_state(
            rate_limiter,
            crate::middleware::rate_limit::rate_limit_middleware,
        ))
        .layer(axum_middleware::from_fn_with_state(
            circuit_breaker,
            crate::middleware::circuit_breaker::circuit_breaker_middleware,
        ))
        .layer(axum_middleware::from_fn_with_state(
            backpressure,
            crate::middleware::backpressure::backpressure_middleware,
        ))
}

/// Fix #10: Build CORS layer from config instead of blanket `Any`.
///
/// For development (CORS_ALLOWED_ORIGINS=*), allows all origins.
/// For production, restricts to configured domains.
fn build_cors_layer(config: &Config) -> CorsLayer {
    use tower_http::cors::Any;

    let is_wildcard =
        config.cors_allowed_origins.len() == 1 && config.cors_allowed_origins[0] == "*";

    if is_wildcard {
        // Development mode — wide open
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    } else {
        // Production mode — restrict to specific origins
        let origins: Vec<HeaderValue> = config
            .cors_allowed_origins
            .iter()
            .filter_map(|o| o.parse::<HeaderValue>().ok())
            .collect();

        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_otlp_endpoint_returns_none_for_unset() {
        assert_eq!(resolve_otlp_endpoint(None), None);
    }

    #[test]
    fn resolve_otlp_endpoint_returns_none_for_empty_string() {
        assert_eq!(resolve_otlp_endpoint(Some(String::new())), None);
    }

    #[test]
    fn resolve_otlp_endpoint_returns_none_for_whitespace_only() {
        assert_eq!(resolve_otlp_endpoint(Some("   ".to_string())), None);
    }

    #[test]
    fn resolve_otlp_endpoint_returns_some_for_real_endpoint() {
        assert_eq!(
            resolve_otlp_endpoint(Some("http://otel-collector:4317".to_string())),
            Some("http://otel-collector:4317".to_string())
        );
    }

    #[test]
    fn resolve_otlp_endpoint_trims_padded_input() {
        assert_eq!(
            resolve_otlp_endpoint(Some("  http://x:4317  ".to_string())),
            Some("http://x:4317".to_string())
        );
    }
}
