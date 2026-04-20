use std::net::SocketAddr;

use tokio_util::sync::CancellationToken;

use crate::bootstrap;
use crate::config::Config;
use crate::AppState;

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
}

impl App {
    /// Bootstrap the entire application: tracing, metrics, infrastructure.
    pub async fn new() -> anyhow::Result<Self> {
        bootstrap::init_tracing();
        tracing::info!("Starting GN High-Performance Backend (3-shard, mimalloc)");

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

        Ok(Self {
            state,
            config,
            cancel,
            rate_limiter: infra.rate_limiter,
            circuit_breaker: infra.circuit_breaker,
            backpressure: infra.backpressure,
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
        // Start the shard-aware queue consumer with a child token
        tracing::info!("Starting shard-aware queue consumer...");
        let consumer_handle = crate::queue::consumer::QueueConsumer::start(
            &self.config,
            self.state.shard_router.clone(),
            self.cancel.child_token(),
        )
        .await?;

        // Build the router
        let app = bootstrap::build_router(
            self.state.clone(),
            self.rate_limiter,
            self.circuit_breaker,
            self.backpressure,
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
