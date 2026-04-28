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

/// Initialise the global `tracing` subscriber with three layers:
/// 1. `EnvFilter` — controls verbosity via `RUST_LOG`
/// 2. `fmt::Layer` — structured JSON logs to stdout
/// 3. `OpenTelemetryLayer` — exports traces for context propagation
///
/// The OTel exporter defaults to `stdout`. To send traces to an
/// OTLP collector (Jaeger, Grafana Tempo), swap `opentelemetry_stdout`
/// for `opentelemetry-otlp` and configure the endpoint.
pub fn init_tracing() {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_opentelemetry::OpenTelemetryLayer;

    let exporter = opentelemetry_stdout::SpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build();
    let tracer = provider.tracer("peakload-capstone");

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json())
        .with(OpenTelemetryLayer::new(tracer))
        .init();
}

// ─── Metrics ────────────────────────────────────────────────────

/// Install the Prometheus recorder and pre-register every metric so
/// they appear in `/metrics` from the very first scrape.
pub fn init_metrics() -> metrics_exporter_prometheus::PrometheusHandle {
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
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
    metrics::describe_counter!("backpressure_shed_total", "Backpressure shed requests");
    metrics::describe_gauge!("backpressure_in_flight", "In-flight requests");
    metrics::describe_gauge!("circuit_breaker_state", "Circuit breaker state");
    metrics::describe_counter!("idempotency_hits_total", "Idempotency hits");
    metrics::describe_counter!("rabbitmq_reconnections_total", "RabbitMQ reconnections");
    metrics::describe_histogram!("transactions_batch_size", "Batch sizes");
    metrics::describe_counter!("dlq_messages_total", "Dead letter queue messages");

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
    metrics::histogram!("transactions_batch_size").record(0.0);

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
            // docker-compose.yml / haproxy.cfg / shard.rs.
            // ShardUrls {
            //     write_url: config.database_shard2_write_url.clone(),
            //     read_urls: config.database_shard2_read_urls.clone(),
            // },
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
    let backpressure = BackpressureController::new(config.max_concurrent_requests);

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
        if let Some(secret) = config.auth_secret.as_deref() {
            crate::middleware::auth::init_auth(secret);
            tracing::info!("JWT auth middleware enabled");
        } else {
            tracing::error!("ENABLE_AUTH=true but AUTH_SECRET is missing — all requests will 500");
        }
    } else {
        tracing::info!("JWT auth middleware disabled (ENABLE_AUTH=false)");
    }
    let auth_state = crate::middleware::auth::AuthState {
        enabled: config.enable_auth,
    };

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
    );

    // Phase 2: transactions module wired with the cross-module
    // dep injected (transactions → accounts), even though the
    // current use cases do not yet exercise it. See module-level
    // comment in `src/modules/transactions/mod.rs`.
    let transactions_deps = transactions::init(
        state.shard_router.clone(),
        state.cache.clone(),
        state.queue_producer.clone(),
        accounts_deps.service.clone(),
    );
    let transactions_router = apply_protection_stack(
        transactions::router(transactions_deps),
        auth_state.clone(),
        rate_limiter.clone(),
        circuit_breaker.clone(),
        backpressure.clone(),
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
    );

    // `nest_service` (rather than `nest`) because each module
    // router is already fully stateful (`.with_state(deps)` was
    // applied inside `*::router`) and thus its state type is
    // `()`, whereas the parent router carries `AppState`. The
    // `nest_service` method accepts any `Service`, bridging the
    // mismatch without forcing module deps to implement
    // `FromRef<AppState>`.
    Router::new()
        .nest_service("/api/v2/accounts", accounts_router)
        .nest_service("/api/v2/transactions", transactions_router)
        .nest_service("/api/v2/notifications", notifications_router)
        .route("/health", get(crate::health::health_check))
        .route("/metrics", get(crate::health::prometheus_metrics))
        .with_state(state)
        .layer(axum_middleware::from_fn(
            crate::middleware::request_id::request_id_middleware,
        ))
        .layer(axum_middleware::from_fn(
            crate::middleware::metrics::metrics_middleware,
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
        .layer(TraceLayer::new_for_http())
        .layer(cors)
}

/// Apply the standard request-protection stack to a router.
///
/// Order matters: `.layer` wraps outside-in, so the call sequence below
/// produces this request flow at runtime:
///
/// ```text
///   request → backpressure → circuit_breaker → rate_limit → auth → handler
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
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
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
