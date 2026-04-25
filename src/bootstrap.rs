use std::time::Duration;

use axum::{
    error_handling::HandleErrorLayer,
    http::{HeaderValue, StatusCode},
    middleware as axum_middleware,
    routing::{get, post},
    BoxError, Router,
};
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::{
    cors::CorsLayer,
    trace::TraceLayer,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::cache::redis::RedisCache;
use crate::config::Config;
use crate::db::shard::ShardRouter;
use crate::middleware::backpressure::BackpressureController;
use crate::middleware::circuit_breaker::CircuitBreaker;
use crate::middleware::rate_limit::RateLimiter;
use crate::queue::producer::QueueProducer;
use crate::AppState;

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
    metrics::describe_counter!("db_retry_success_total", "DB ops that succeeded after retry");
    metrics::describe_counter!("db_retry_exhausted_total", "DB ops that exhausted retries");
    metrics::describe_counter!(
        "redis_master_failover_total",
        "Redis master address changes detected via Sentinel"
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
    let shard_router = ShardRouter::new(config, cancel.child_token()).await?;

    tracing::info!("Connecting to Redis...");
    let cache = RedisCache::new(config, cancel.child_token()).await?;

    tracing::info!("Connecting to RabbitMQ...");
    let queue_producer = QueueProducer::new(config).await?;

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
            tracing::error!(
                "ENABLE_AUTH=true but AUTH_SECRET is missing — all requests will 500"
            );
        }
    } else {
        tracing::info!("JWT auth middleware disabled (ENABLE_AUTH=false)");
    }
    let auth_state = crate::middleware::auth::AuthState {
        enabled: config.enable_auth,
    };

    let api_routes = Router::new()
        .route(
            "/transactions",
            post(crate::api::handlers::create_transaction),
        )
        .route(
            "/transactions",
            get(crate::api::handlers::list_transactions),
        )
        .route(
            "/transactions/{id}",
            get(crate::api::handlers::get_transaction),
        )
        .route(
            "/transactions/status/{reference_id}",
            get(crate::api::handlers::get_transaction_status),
        )
        .route(
            "/users/{account_number}/balance",
            get(crate::api::handlers::get_balance),
        )
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
        ));

    // Fix #10: Build CORS layer from configuration
    let cors = build_cors_layer(config);

    // ─── Phase 1 modular-monolith: mount `/api/v2/accounts/*` ───
    //
    // See docs/architecture/phase1-accounts-walkthrough.md. The
    // new module owns its own HTTP surface at `/api/v2/accounts`
    // and lives PARALLEL to the legacy `/api/v1/users/.../balance`
    // endpoint — both paths serve identical responses until the
    // cutover task deletes the legacy handler.
    //
    // Intentional gap for Phase 1 partial: the v2 sub-router does
    // NOT reuse the v1 auth/rate-limit/circuit-breaker/backpressure
    // stack. Acceptable for a proof-of-shape but MUST be addressed
    // before v2 replaces v1. Tracking task: migration-plan Phase 1
    // exit criteria.
    let accounts_deps = crate::modules::accounts::init(
        state.shard_router.clone(),
        state.cache.clone(),
    );
    let accounts_router = crate::modules::accounts::router(accounts_deps.clone());

    // Phase 2: transactions module wired with the cross-module
    // dep injected (transactions → accounts), even though the
    // current use cases do not yet exercise it. See module-level
    // comment in `src/modules/transactions/mod.rs`.
    let transactions_deps = crate::modules::transactions::init(
        state.shard_router.clone(),
        state.cache.clone(),
        state.queue_producer.clone(),
        accounts_deps.service.clone(),
    );
    let transactions_router =
        crate::modules::transactions::router(transactions_deps);

    // `nest_service` (rather than `nest`) because each module
    // router is already fully stateful (`.with_state(deps)` was
    // applied inside `*::router`) and thus its state type is
    // `()`, whereas the parent router carries `AppState`. The
    // `nest_service` method accepts any `Service`, bridging the
    // mismatch without forcing module deps to implement
    // `FromRef<AppState>`.
    Router::new()
        .nest("/api/v1", api_routes)
        .nest_service("/api/v2/accounts", accounts_router)
        .nest_service("/api/v2/transactions", transactions_router)
        .route("/health", get(crate::api::handlers::health_check))
        .route("/metrics", get(crate::api::handlers::prometheus_metrics))
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

/// Fix #10: Build CORS layer from config instead of blanket `Any`.
///
/// For development (CORS_ALLOWED_ORIGINS=*), allows all origins.
/// For production, restricts to configured domains.
fn build_cors_layer(config: &Config) -> CorsLayer {
    use tower_http::cors::Any;

    let is_wildcard = config.cors_allowed_origins.len() == 1
        && config.cors_allowed_origins[0] == "*";

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
