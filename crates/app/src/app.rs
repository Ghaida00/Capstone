use std::net::SocketAddr;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::bootstrap;
use crate::config::Config;
use crate::AppState;
use shared_kernel::events::{EventPublisher, EventSubscriber, InProcessEventBus};

/// Top-level application struct.
///
/// Owns the fully-initialised state, configuration, and a
/// `CancellationToken` for coordinated graceful shutdown. Provides
/// a single `.run()` entry-point for the production server.
///
/// Integration tests can bypass `App` entirely and call
/// `bootstrap::build_router()` directly.
pub struct App {
    pub state: AppState,
    pub config: Config,
    pub cancel: CancellationToken,
    rate_limiter: crate::middleware::rate_limit::RateLimiter,
    circuit_breaker: crate::middleware::circuit_breaker::CircuitBreaker,
    backpressure: crate::middleware::backpressure::BackpressureController,
    /// Shared-kernel cross-module bus. One instance per process,
    /// exposed as both a publisher (handed to the queue consumer)
    /// and a subscriber (handed to the notifications module). See
    /// `docs/architecture/phase3-notifications-walkthrough.md`.
    event_publisher: Arc<dyn EventPublisher>,
    event_subscriber: Arc<dyn EventSubscriber>,
}

impl App {
    /// Bootstrap the entire application: tracing, metrics, infrastructure.
    pub async fn new() -> anyhow::Result<Self> {
        bootstrap::init_tracing();
        tracing::info!("Starting Peakload Capstone High-Performance Backend (3-shard, mimalloc)");

        let config = Config::from_env();
        config.validate()?;
        tracing::info!("{}", config);

        let metrics_handle = bootstrap::init_metrics();
        let cancel = CancellationToken::new();

        // Fix #16: pass cancel token so rate limiter tasks can shut down
        let infra = bootstrap::init_infrastructure(&config, cancel.child_token()).await?;

        let state = AppState {
            shard_router: infra.shard_router,
            cache: infra.cache,
            queue_producer: infra.queue_producer,
            metrics_handle,
        };

        // Single in-process bus, two trait-object views handed
        // out separately so neither side can accidentally do the
        // other's job.
        let bus = InProcessEventBus::new();
        let event_publisher: Arc<dyn EventPublisher> = Arc::new(bus.clone());
        let event_subscriber: Arc<dyn EventSubscriber> = Arc::new(bus);

        Ok(Self {
            state,
            config,
            cancel,
            rate_limiter: infra.rate_limiter,
            circuit_breaker: infra.circuit_breaker,
            backpressure: infra.backpressure,
            event_publisher,
            event_subscriber,
        })
    }

    /// Run the production server and queue consumer until shutdown.
    ///
    /// Shutdown flow:
    /// 1. OS signal (Ctrl+C / SIGTERM) fires.
    /// 2. `CancellationToken` is cancelled, notifying all subsystems.
    /// 3. Axum server stops accepting new connections.
    /// 4. Queue consumer flushes remaining buffer and exits.
    /// 5. Database connection pools are closed.
    pub async fn run(self) -> anyhow::Result<()> {
        // Start the shard-aware queue consumer with a child token.
        // After the Step-A rewire (cutover-readiness §1) the consumer
        // lives in `transactions::infrastructure::consumer`; the
        // bootstrap calls one entry point and the `transactions`
        // crate owns its full write path. The publisher half of the
        // shared-kernel bus is threaded in so successful batch flushes
        // emit `transactions.committed` events for `notifications`.
        tracing::info!("Starting shard-aware queue consumer...");
        let consumer_handle = transactions::start_consumer(
            &self.config.rabbitmq_url,
            self.state.shard_router.clone(),
            self.event_publisher.clone(),
            self.cancel.child_token(),
        )
        .await?;

        // Cache invalidator subscribes to commit events and DELs
        // stale tx_status / acc cache keys. Bounds staleness at
        // commit-time rather than cache TTL.
        let _cache_inv_handle = transactions::spawn_cache_invalidator(
            self.event_subscriber.clone(),
            self.state.cache.clone(),
            self.cancel.child_token(),
        );

        // Periodic sweep of expired idempotency rows. Without this
        // the table grows unbounded.
        let _idem_cleanup_handle = transactions::spawn_idempotency_cleanup(
            self.state.shard_router.clone(),
            self.cancel.child_token(),
        );

        // Cross-shard credit outbox drainer.
        let _outbox_handle = transactions::spawn_cross_shard_processor(
            self.state.shard_router.clone(),
            self.cancel.child_token(),
        );

        // Build the router. The subscriber half of the bus goes
        // into the notifications module so its dispatch loop can
        // drain events into the in-memory log.
        let app = bootstrap::build_router(
            self.state.clone(),
            self.rate_limiter,
            self.circuit_breaker,
            self.backpressure,
            self.event_subscriber.clone(),
            self.cancel.child_token(),
            &self.config,
        );

        // Bind and start the server
        let addr = SocketAddr::new(self.config.host.parse()?, self.config.port);
        let listener = tokio::net::TcpListener::bind(addr).await?;

        tracing::info!(address = %addr, "Server started — 3-shard architecture ready");

        // Clone the token for the shutdown closure
        let cancel = self.cancel.clone();
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            wait_for_signal().await;
            tracing::info!("Shutdown signal received — cancelling all subsystems...");
            cancel.cancel();
        });

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Server error");
                }
            }
            _ = consumer_handle => {
                tracing::warn!("Queue consumer task ended");
            }
        }

        tracing::info!("Shutting down — closing connection pools...");
        self.state.shard_router.close().await;
        tracing::info!("Goodbye!");

        Ok(())
    }
}

/// Listen for Ctrl+C or SIGTERM.
async fn wait_for_signal() {
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
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
