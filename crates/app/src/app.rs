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
    /// [ADR-0004](../../../docs/adr/0004-in-process-event-bus.md)
    /// for the rationale and the planned AMQP swap surface.
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

        // R-9: seed the degradation posture from validated config
        // (validate() above already rejected an unrecognised value,
        // so the unwrap_or is unreachable defence-in-depth).
        let degradation = crate::degradation::DegradationFlag::new(
            crate::degradation::DegradationMode::parse(&config.degradation_mode)
                .unwrap_or(crate::degradation::DegradationMode::Normal),
        );

        let state = AppState {
            shard_router: infra.shard_router,
            cache: infra.cache,
            queue_producer: infra.queue_producer,
            metrics_handle,
            degradation,
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
        // After the Step-A rewire (ADR-0007 Step A) the consumer
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
        let cache_inv_handle = transactions::spawn_cache_invalidator(
            self.event_subscriber.clone(),
            self.state.cache.clone(),
            self.cancel.child_token(),
        );

        // Periodic sweep of expired idempotency rows. Without this
        // the table grows unbounded.
        let idem_cleanup_handle = transactions::spawn_idempotency_cleanup(
            self.state.shard_router.clone(),
            self.cancel.child_token(),
        );

        // Cross-shard credit outbox drainer.
        let outbox_handle = transactions::spawn_cross_shard_processor(
            self.state.shard_router.clone(),
            self.cancel.child_token(),
        );

        // Publish-outbox drainer: ships `idempotency_keys.outbox_payload`
        // rows to RabbitMQ so the create handler returns 202 without
        // waiting for the broker confirm. One worker per shard.
        let publish_outbox_handles = transactions::spawn_publish_outbox(
            self.state.shard_router.clone(),
            self.state.queue_producer.clone(),
            self.cancel.child_token(),
        );

        // Redis-intake drainer: only relevant under the Redis or
        // Hybrid idempotency backend. Drains the per-shard
        // `idempotency:pending` list, INSERTs into PG, and
        // publishes to the broker. With backend=Pg the lists stay
        // empty and the worker would just block on BRPOPLPUSH for
        // its full timeout, so skip the spawn entirely.
        let redis_intake_handles = match self.config.idempotency_backend {
            crate::config::IdempotencyBackend::Pg => Vec::new(),
            crate::config::IdempotencyBackend::Redis
            | crate::config::IdempotencyBackend::Hybrid => transactions::spawn_redis_intake(
                self.state.shard_router.clone(),
                self.state.cache.clone(),
                self.state.queue_producer.clone(),
                self.cancel.child_token(),
            ),
        };

        // Pre-warm the write pipeline before binding the listener.
        // The first publish on a fresh producer pool exercises the
        // full channel + confirm round-trip; the first per-shard PG
        // query opens a pooled connection through pgBouncer →
        // HAProxy → Patroni. Skipping this lets the very first
        // POST eat both setup costs (5+ s observed under live
        // probing). Failures are logged and ignored — health
        // probes will surface the same condition once traffic
        // starts.
        prewarm_pipeline(&self.state).await;

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

        // Either the server or the consumer can trigger shutdown.
        // The OS-signal path runs `cancel.cancel()` from inside the
        // server's `with_graceful_shutdown` closure; the consumer-
        // exit path needs to call it explicitly so the server, the
        // cache invalidator, the cleanup sweep, the cross-shard
        // drainer, and the publish-outbox workers all observe the
        // shutdown signal instead of being silently abandoned.
        let mut consumer_handle = consumer_handle;
        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Server error");
                }
            }
            res = &mut consumer_handle => {
                match res {
                    Ok(()) => tracing::warn!("Queue consumer task ended"),
                    Err(e) => tracing::error!(error = ?e, "Queue consumer task panicked"),
                }
                self.cancel.cancel();
            }
        }
        // Cancel here too: covers the server-exit branch where
        // `with_graceful_shutdown` was bypassed (e.g. bind error,
        // panic surfaced as `serve` returning Err).
        self.cancel.cancel();

        tracing::info!("Cancellation triggered — draining auxiliary tasks...");
        // Auxiliary tasks have no `catch_unwind`, so a panic surfaces
        // here as a `JoinError`. Log it explicitly per task instead
        // of swallowing it — drain continues regardless so a single
        // panicked task cannot block shutdown.
        // `is_finished` gates against tokio's "JoinHandle polled after
        // completion" panic when the consumer arm of the `select!`
        // above already polled this handle to completion.
        if !consumer_handle.is_finished() {
            if let Err(e) = consumer_handle.await {
                tracing::error!(error = ?e, task = "consumer", "task panicked");
            }
        }
        if let Err(e) = cache_inv_handle.await {
            tracing::error!(error = ?e, task = "cache_invalidator", "task panicked");
        }
        if let Err(e) = idem_cleanup_handle.await {
            tracing::error!(error = ?e, task = "idempotency_cleanup", "task panicked");
        }
        if let Err(e) = outbox_handle.await {
            tracing::error!(error = ?e, task = "cross_shard_outbox", "task panicked");
        }
        for (idx, h) in publish_outbox_handles.into_iter().enumerate() {
            if let Err(e) = h.await {
                tracing::error!(error = ?e, task = "publish_outbox", shard = idx, "task panicked");
            }
        }
        for (idx, h) in redis_intake_handles.into_iter().enumerate() {
            if let Err(e) = h.await {
                tracing::error!(error = ?e, task = "redis_intake", shard = idx, "task panicked");
            }
        }

        tracing::info!("All subsystems drained — closing connection pools...");
        self.state.shard_router.close().await;
        tracing::info!("Goodbye!");

        Ok(())
    }
}

/// Drive one no-op round-trip through every component on the
/// write hot path so the first real request doesn't pay the
/// lazy-init tax. Exercises:
///   * the producer's channel pool + publisher-confirm path via
///     `QueueProducer::health_check_active` (publishes to the DLX
///     exchange with `mandatory=false`, broker drops silently),
///   * each shard's writer pool via a `SELECT 1` round-trip
///     through pgBouncer + HAProxy + Patroni.
///
/// Failures are logged and ignored — the live `/health` probe
/// will catch persistent breakage once traffic starts.
async fn prewarm_pipeline(state: &AppState) {
    let t = std::time::Instant::now();

    let producer_ok = state.queue_producer.health_check_active().await;

    let mut shard_results = Vec::with_capacity(state.shard_router.num_shards());
    for shard in 0..state.shard_router.num_shards() {
        let pool = state.shard_router.writer(shard);
        let ok = sqlx::query("SELECT 1").execute(pool).await.is_ok();
        shard_results.push((shard, ok));
    }

    tracing::info!(
        elapsed_ms = t.elapsed().as_millis() as u64,
        producer_ok,
        shards = ?shard_results,
        "Pipeline pre-warmed before accepting traffic"
    );
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
