#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod api;
mod cache;
mod config;
mod db;
mod error;
mod middleware;
mod queue;

use std::net::SocketAddr;
use std::time::Duration;

use axum::{
    error_handling::HandleErrorLayer,
    http::StatusCode,
    middleware as axum_middleware,
    routing::{get, post},
    BoxError, Router,
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::cache::redis::RedisCache;
use crate::config::Config;
use crate::db::shard::ShardRouter;
use crate::middleware::backpressure::BackpressureController;
use crate::middleware::circuit_breaker::CircuitBreaker;
use crate::middleware::rate_limit::RateLimiter;
use crate::queue::producer::QueueProducer;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub shard_router: ShardRouter,
    pub cache: RedisCache,
    pub queue_producer: QueueProducer,
    pub circuit_breaker: CircuitBreaker,
    pub backpressure: BackpressureController,
    pub metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    tracing::info!("Starting GN High-Performance Backend (3-shard, mimalloc)");

    let config = Config::from_env();

    // ─── Prometheus metrics ────────────────────────────────
    let prometheus_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = prometheus_builder
        .install_recorder()
        .expect("Failed to install Prometheus recorder");

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

    // ─── Initialize metrics to 0 so they appear in /metrics immediately ─
    metrics::counter!("transactions_created_total").absolute(0);
    metrics::counter!("transactions_processed_total").absolute(0);
    metrics::counter!("cache_hits_total").absolute(0);
    metrics::counter!("cache_misses_total").absolute(0);
    metrics::counter!("rate_limited_total").absolute(0);
    metrics::counter!("backpressure_shed_total").absolute(0);
    metrics::counter!("idempotency_hits_total").absolute(0);
    metrics::counter!("rabbitmq_reconnections_total").absolute(0);
    metrics::gauge!("backpressure_in_flight").set(0.0);
    metrics::gauge!("circuit_breaker_state").set(0.0);
    metrics::histogram!("transactions_batch_size").record(0.0);


    // ─── Initialize ShardRouter (3 shards × write + reads) ─
    tracing::info!("Connecting to database shards...");
    let shard_router = ShardRouter::new(&config).await?;

    // ─── Redis cache (read/write split) ────────────────────
    tracing::info!("Connecting to Redis...");
    let cache = RedisCache::new(&config)?;

    // ─── RabbitMQ producer ─────────────────────────────────
    tracing::info!("Connecting to RabbitMQ...");
    let queue_producer = QueueProducer::new(&config).await?;

    // ─── Middleware components ──────────────────────────────
    let rate_limit_pool = cache.create_pool(&config)?;
    let rate_limiter = RateLimiter::new(
        rate_limit_pool,
        config.rate_limit_per_second,
        config.rate_limit_burst,
    );
    let circuit_breaker = CircuitBreaker::new(
        config.circuit_breaker_failure_threshold,
        config.circuit_breaker_recovery_timeout_secs,
    );
    let backpressure = BackpressureController::new(config.max_concurrent_requests);

    // ─── Application state ─────────────────────────────────
    let state = AppState {
        shard_router: shard_router.clone(),
        cache,
        queue_producer,
        circuit_breaker: circuit_breaker.clone(),
        backpressure: backpressure.clone(),
        metrics_handle,
    };

    // ─── Start shard-aware consumer ────────────────────────
    tracing::info!("Starting shard-aware queue consumer...");
    let consumer_handle =
        queue::consumer::QueueConsumer::start(&config, shard_router.clone()).await?;

    // ─── Router ────────────────────────────────────────────
    let api_routes = Router::new()
        .route("/transactions", post(api::handlers::create_transaction))
        .route("/transactions", get(api::handlers::list_transactions))
        .route("/transactions/{id}", get(api::handlers::get_transaction))
        .layer(axum_middleware::from_fn_with_state(
            rate_limiter.clone(),
            middleware::rate_limit::rate_limit_middleware,
        ))
        .layer(axum_middleware::from_fn_with_state(
            circuit_breaker.clone(),
            middleware::circuit_breaker::circuit_breaker_middleware,
        ))
        .layer(axum_middleware::from_fn_with_state(
            backpressure.clone(),
            middleware::backpressure::backpressure_middleware,
        ));

    let app = Router::new()
        .nest("/api/v1", api_routes)
        .route("/health", get(api::handlers::health_check))
        .route("/metrics", get(api::handlers::prometheus_metrics))
        .with_state(state)
        .layer(axum_middleware::from_fn(
            middleware::request_id::request_id_middleware,
        ))
        .layer(axum_middleware::from_fn(
            middleware::metrics::metrics_middleware,
        ))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|err: BoxError| async move {
                    if err.is::<tower::timeout::error::Elapsed>() {
                        (StatusCode::REQUEST_TIMEOUT, "Request took too long".to_string())
                    } else {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("Unhandled internal error: {}", err))
                    }
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(30)))
        )
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    // ─── Start Server ──────────────────────────────────────
    let addr = SocketAddr::new(config.host.parse()?, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!(address = %addr, "Server started — 3-shard architecture ready");

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Server error");
            }
        }
        _ = async { consumer_handle.await } => {
            tracing::warn!("Queue consumer task ended");
        }
    }

    tracing::info!("Shutting down...");
    shard_router.close().await;
    tracing::info!("Goodbye!");

    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { tracing::info!("Received Ctrl+C, shutting down..."); }
        _ = terminate => { tracing::info!("Received SIGTERM, shutting down..."); }
    }
}
