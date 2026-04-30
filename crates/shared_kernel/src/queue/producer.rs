use amqprs::{
    callbacks::DefaultConnectionCallback,
    channel::{
        BasicPublishArguments, ConfirmSelectArguments, ExchangeDeclareArguments,
        QueueBindArguments, QueueDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    BasicProperties,
};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::RwLock;

use crate::error::AppError;
use crate::queue::callback::{ConfirmRegistry, ConfirmResult, LoggingChannelCallback};

const EXCHANGE_NAME: &str = "peakload.transactions";
const QUEUE_NAME: &str = "transactions.process";
const ROUTING_KEY: &str = "transaction.created";
const DLX_EXCHANGE: &str = "peakload.transactions.dlx";
const DLX_QUEUE: &str = "transactions.dead_letter";

/// AMQP channels per connection.
const CHANNELS_PER_CONNECTION: usize = 4;
/// Connections in the producer pool. Multiple TCP connections are
/// the RabbitMQ-recommended HA pattern; one connection drop no
/// longer kills every in-flight publish.
const CONNECTION_POOL_SIZE: usize = 2;
/// Total channel-slot count = connections × channels-per-connection.
const CHANNEL_POOL_SIZE: usize = CONNECTION_POOL_SIZE * CHANNELS_PER_CONNECTION;

/// One channel + its confirm-tracking state. Publishes serialise
/// per-channel via `publish_lock` so our internal tag counter
/// (`next_tag`) matches broker's broker-side counter.
struct ConfirmingChannel {
    channel: Arc<amqprs::channel::Channel>,
    registry: Arc<ConfirmRegistry>,
    next_tag: AtomicU64,
    publish_lock: TokioMutex<()>,
}

/// Pool of connections + their confirming channels.
struct ConnState {
    _connections: Vec<Connection>,
    channels: Vec<Arc<ConfirmingChannel>>,
}

/// RabbitMQ message producer with automatic reconnection.
///
/// Hot path is lock-free: each `publish` clones an `Arc<ConnState>` out
/// of an `RwLock` (read side, never contends), picks a channel via
/// round-robin, and calls `basic_publish` directly. Reconnect rebuilds
/// the whole `ConnState` under the write side of the lock.
#[derive(Clone)]
pub struct QueueProducer {
    state: Arc<RwLock<Arc<ConnState>>>,
    rr: Arc<AtomicUsize>,
    connected: Arc<AtomicBool>,
    config: Arc<ProducerConfig>,
}

struct ProducerConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    vhost: String,
}

/// Per-publish retry budget on transient errors. Keeps the
/// blast radius of a single broker hiccup bounded — a failed
/// publish on a fresh channel after reconnect retries up to
/// `MAX_PUBLISH_ATTEMPTS` times with linear backoff. Each
/// retry takes a fresh channel so a wedged channel does not
/// trap the whole publish.
const MAX_PUBLISH_ATTEMPTS: u32 = 3;
const PUBLISH_RETRY_BACKOFF_MS: u64 = 25;

/// Bound on AMQP control operations (open connection, open
/// channel) so a slow broker can't deadlock app startup. The
/// timeout fires as an `AppError::Internal`, which is what the
/// caller would have produced from the underlying error anyway.
const AMQP_CONTROL_TIMEOUT_SECS: u64 = 10;

impl QueueProducer {
    pub async fn new(amqp_url: &str) -> Result<Self, AppError> {
        let parts = parse_amqp_url_full(amqp_url)?;

        let producer_config = Arc::new(ProducerConfig {
            host: parts.host.clone(),
            port: parts.port,
            username: parts.username.clone(),
            password: parts.password.clone(),
            vhost: parts.vhost.clone(),
        });

        let state = Self::connect_and_setup(
            &parts.host,
            parts.port,
            &parts.username,
            &parts.password,
            &parts.vhost,
        )
        .await?;

        tracing::info!(
            exchange = EXCHANGE_NAME,
            queue = QUEUE_NAME,
            channels = CHANNEL_POOL_SIZE,
            "RabbitMQ producer initialized (amqprs, channel pool)"
        );

        Ok(Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            rr: Arc::new(AtomicUsize::new(0)),
            connected: Arc::new(AtomicBool::new(true)),
            config: producer_config,
        })
    }

    async fn connect_and_setup(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        vhost: &str,
    ) -> Result<ConnState, AppError> {
        let mut args = OpenConnectionArguments::new(host, port, username, password);
        args.virtual_host(vhost);

        // First connection — used to declare topology and serve
        // the first chunk of channels.
        let connection =
            tokio::time::timeout(Duration::from_secs(AMQP_CONTROL_TIMEOUT_SECS), Connection::open(&args))
                .await
                .map_err(|_| AppError::Internal("RabbitMQ connection open timed out".into()))?
                .map_err(|e| AppError::Internal(format!("RabbitMQ connection error: {}", e)))?;

        connection
            .register_callback(DefaultConnectionCallback)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ callback error: {}", e)))?;

        // Declare topology on a throw-away channel (no confirm
        // mode) so the topology declarations don't try to track
        // confirms. Real publish channels are wrapped below.
        let setup_channel = tokio::time::timeout(
            Duration::from_secs(AMQP_CONTROL_TIMEOUT_SECS),
            connection.open_channel(None),
        )
        .await
        .map_err(|_| AppError::Internal("RabbitMQ open_channel timed out".into()))?
        .map_err(|e| AppError::Internal(format!("RabbitMQ channel error: {}", e)))?;
        setup_channel
            .register_callback(amqprs::callbacks::DefaultChannelCallback)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ channel callback error: {}", e)))?;

        let dlx_args = ExchangeDeclareArguments::new(DLX_EXCHANGE, "direct");
        setup_channel
            .exchange_declare(dlx_args)
            .await
            .map_err(|e| AppError::Internal(format!("DLX exchange declare error: {}", e)))?;

        let dlq_args = QueueDeclareArguments::durable_client_named(DLX_QUEUE);
        setup_channel
            .queue_declare(dlq_args)
            .await
            .map_err(|e| AppError::Internal(format!("DLX queue declare error: {}", e)))?;

        let dlq_bind_args = QueueBindArguments::new(DLX_QUEUE, DLX_EXCHANGE, "dead_letter");
        setup_channel
            .queue_bind(dlq_bind_args)
            .await
            .map_err(|e| AppError::Internal(format!("DLX queue bind error: {}", e)))?;

        let ex_args = ExchangeDeclareArguments::new(EXCHANGE_NAME, "direct");
        setup_channel
            .exchange_declare(ex_args)
            .await
            .map_err(|e| AppError::Internal(format!("Exchange declare error: {}", e)))?;

        let mut queue_field_table = amqprs::FieldTable::new();
        queue_field_table.insert(
            "x-dead-letter-exchange".try_into().unwrap(),
            amqprs::FieldValue::S(DLX_EXCHANGE.try_into().unwrap()),
        );
        queue_field_table.insert(
            "x-dead-letter-routing-key".try_into().unwrap(),
            amqprs::FieldValue::S("dead_letter".try_into().unwrap()),
        );

        let mut main_queue_args = QueueDeclareArguments::durable_client_named(QUEUE_NAME);
        main_queue_args.arguments(queue_field_table);
        setup_channel
            .queue_declare(main_queue_args)
            .await
            .map_err(|e| AppError::Internal(format!("Queue declare error: {}", e)))?;

        let bind_args = QueueBindArguments::new(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        setup_channel
            .queue_bind(bind_args)
            .await
            .map_err(|e| AppError::Internal(format!("Queue bind error: {}", e)))?;

        let mut channels: Vec<Arc<ConfirmingChannel>> =
            Vec::with_capacity(CHANNEL_POOL_SIZE);
        channels.push(Self::wrap_channel(setup_channel).await?);
        for _ in 1..CHANNELS_PER_CONNECTION {
            let ch = tokio::time::timeout(
                Duration::from_secs(AMQP_CONTROL_TIMEOUT_SECS),
                connection.open_channel(None),
            )
            .await
            .map_err(|_| AppError::Internal("RabbitMQ open_channel timed out".into()))?
            .map_err(|e| AppError::Internal(format!("RabbitMQ channel error: {}", e)))?;
            channels.push(Self::wrap_channel(ch).await?);
        }

        let mut connections: Vec<Connection> = Vec::with_capacity(CONNECTION_POOL_SIZE);
        connections.push(connection);
        for _ in 1..CONNECTION_POOL_SIZE {
            let extra = tokio::time::timeout(
                Duration::from_secs(AMQP_CONTROL_TIMEOUT_SECS),
                Connection::open(&args),
            )
            .await
            .map_err(|_| AppError::Internal("RabbitMQ connection open timed out".into()))?
            .map_err(|e| AppError::Internal(format!("RabbitMQ connection error: {}", e)))?;
            extra
                .register_callback(DefaultConnectionCallback)
                .await
                .map_err(|e| AppError::Internal(format!("RabbitMQ callback error: {}", e)))?;
            for _ in 0..CHANNELS_PER_CONNECTION {
                let ch = tokio::time::timeout(
                    Duration::from_secs(AMQP_CONTROL_TIMEOUT_SECS),
                    extra.open_channel(None),
                )
                .await
                .map_err(|_| AppError::Internal("RabbitMQ open_channel timed out".into()))?
                .map_err(|e| AppError::Internal(format!("RabbitMQ channel error: {}", e)))?;
                channels.push(Self::wrap_channel(ch).await?);
            }
            connections.push(extra);
        }

        Ok(ConnState {
            _connections: connections,
            channels,
        })
    }

    async fn wrap_channel(
        ch: amqprs::channel::Channel,
    ) -> Result<Arc<ConfirmingChannel>, AppError> {
        let registry = Arc::new(ConfirmRegistry::new());
        ch.register_callback(LoggingChannelCallback::new(registry.clone()))
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ channel callback error: {}", e)))?;
        ch.confirm_select(ConfirmSelectArguments::default())
            .await
            .map_err(|e| AppError::Internal(format!("confirm_select error: {}", e)))?;
        Ok(Arc::new(ConfirmingChannel {
            channel: Arc::new(ch),
            registry,
            next_tag: AtomicU64::new(0),
            publish_lock: TokioMutex::new(()),
        }))
    }

    /// Publish a message to the transaction queue with publisher
    /// confirms enabled. Each attempt awaits broker ACK before
    /// returning, so an `Ok(())` is durably accepted. Retries on
    /// transient errors / NACKs / timeouts; mandatory=true catches
    /// unrouteable messages.
    pub async fn publish<T: Serialize>(&self, message: &T) -> Result<(), AppError> {
        let payload = serde_json::to_vec(message)?;

        let properties = BasicProperties::default()
            .with_content_type("application/json")
            .with_delivery_mode(2)
            .finish();

        let mut args = BasicPublishArguments::new(EXCHANGE_NAME, ROUTING_KEY);
        args.mandatory = true;

        let mut last_err: Option<String> = None;
        for attempt in 1..=MAX_PUBLISH_ATTEMPTS {
            let snapshot = self.state.read().await.clone();
            let idx = self.rr.fetch_add(1, Ordering::Relaxed) % snapshot.channels.len();
            let cc = snapshot.channels[idx].clone();

            match publish_with_confirm(&cc, properties.clone(), payload.clone(), args.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let err_msg = e.to_string();
                    tracing::warn!(
                        error = %err_msg,
                        attempt,
                        max = MAX_PUBLISH_ATTEMPTS,
                        "RabbitMQ publish failed"
                    );
                    last_err = Some(err_msg);
                    // Mark stale + (single-flight) rebuild on the
                    // first failure. Subsequent attempts re-pick
                    // from the freshly-rebuilt channel pool.
                    if self.connected.swap(false, Ordering::SeqCst) {
                        let mut guard = self.state.write().await;
                        // Re-check under lock: another concurrent
                        // failure may have already rebuilt.
                        if !self.connected.load(Ordering::SeqCst) {
                            tracing::warn!("RabbitMQ: rebuilding channel pool...");
                            match Self::connect_and_setup(
                                &self.config.host,
                                self.config.port,
                                &self.config.username,
                                &self.config.password,
                                &self.config.vhost,
                            )
                            .await
                            {
                                Ok(new_state) => {
                                    *guard = Arc::new(new_state);
                                    self.connected.store(true, Ordering::SeqCst);
                                    metrics::counter!("rabbitmq_reconnections_total")
                                        .increment(1);
                                }
                                Err(rebuild_err) => {
                                    tracing::error!(
                                        error = %rebuild_err,
                                        "RabbitMQ rebuild failed"
                                    );
                                    last_err =
                                        Some(format!("rebuild failed: {}", rebuild_err));
                                }
                            }
                        }
                    }
                }
            }

            if attempt < MAX_PUBLISH_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(
                    PUBLISH_RETRY_BACKOFF_MS * attempt as u64,
                ))
                .await;
            }
        }

        Err(AppError::Internal(format!(
            "RabbitMQ publish failed after {} attempts: {}",
            MAX_PUBLISH_ATTEMPTS,
            last_err.unwrap_or_else(|| "unknown".into())
        )))
    }

    /// Cheap health flag — reflects the last publish outcome.
    /// Use `health_check_active` for an actual liveness probe.
    pub fn health_check(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Active broker probe — issues a lightweight publish to the
    /// dead-letter exchange (so it can't pollute the work queue)
    /// to confirm the connection genuinely round-trips. Updates
    /// the health flag so subsequent cheap calls reflect reality.
    ///
    /// Use sparingly — once per HTTP `/health` poll is fine,
    /// per-request would dominate latency.
    pub async fn health_check_active(&self) -> bool {
        let snapshot = self.state.read().await.clone();
        if snapshot.channels.is_empty() {
            self.connected.store(false, Ordering::SeqCst);
            return false;
        }
        let cc = snapshot.channels[0].clone();
        let properties = BasicProperties::default()
            .with_content_type("application/octet-stream")
            .finish();
        let mut args = BasicPublishArguments::new(DLX_EXCHANGE, "health-probe");
        args.mandatory = false;
        let ok = publish_with_confirm(&cc, properties, Vec::new(), args).await.is_ok();
        self.connected.store(ok, Ordering::SeqCst);
        ok
    }
}

/// Publish + await broker confirm. Per-channel `publish_lock`
/// serialises so our internal tag matches broker's tag (which
/// starts at 1 after `confirm_select` and increments per publish).
async fn publish_with_confirm(
    cc: &Arc<ConfirmingChannel>,
    properties: BasicProperties,
    payload: Vec<u8>,
    args: BasicPublishArguments,
) -> Result<(), AppError> {
    const CONFIRM_TIMEOUT_SECS: u64 = 5;
    let _guard = cc.publish_lock.lock().await;
    let tag = cc.next_tag.fetch_add(1, Ordering::SeqCst) + 1;
    let (tx, rx) = oneshot::channel();
    cc.registry.register(tag, tx).await;
    let publish_res = cc.channel.basic_publish(properties, payload, args).await;
    drop(_guard);
    if let Err(e) = publish_res {
        cc.registry.drop_tag(tag).await;
        return Err(AppError::Internal(format!("basic_publish: {}", e)));
    }
    match tokio::time::timeout(Duration::from_secs(CONFIRM_TIMEOUT_SECS), rx).await {
        Ok(Ok(ConfirmResult::Ack)) => Ok(()),
        Ok(Ok(ConfirmResult::Nack)) => {
            // Tag was already removed from the registry on resolve;
            // no extra cleanup needed.
            Err(AppError::Internal("publish nacked by broker".into()))
        }
        Ok(Err(_)) => {
            // The oneshot sender side was dropped without ever
            // resolving — the registry entry would otherwise leak
            // and slowly grow the BTreeMap until reconnect.
            cc.registry.drop_tag(tag).await;
            Err(AppError::Internal("confirm sender dropped".into()))
        }
        Err(_) => {
            cc.registry.drop_tag(tag).await;
            Err(AppError::Internal("publish confirm timeout".into()))
        }
    }
}

/// Parsed AMQP URL components. Vhost is `/` by default if the URL
/// path is empty — matches RabbitMQ default.
pub struct AmqpUrlParts {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub vhost: String,
}

/// Parse `amqp://user:pass@host:port/vhost` into components.
///
/// Uses the `url` crate so percent-encoded usernames/passwords
/// (containing `:` `@` `/` `%`) round-trip correctly. The previous
/// hand-rolled split silently mis-parsed any password that
/// contained a `:` and dropped the vhost path entirely (defaulting
/// every connection to `/` regardless of what the URL specified).
pub fn parse_amqp_url_full(amqp_url: &str) -> Result<AmqpUrlParts, AppError> {
    let parsed = url::Url::parse(amqp_url)
        .map_err(|e| AppError::Internal(format!("Invalid AMQP URL: {}", e)))?;

    if parsed.scheme() != "amqp" && parsed.scheme() != "amqps" {
        return Err(AppError::Internal(format!(
            "Invalid AMQP URL scheme: {}",
            parsed.scheme()
        )));
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| AppError::Internal("AMQP URL missing host".into()))?
        .to_string();
    let port = parsed.port().unwrap_or(5672);
    let username = percent_decode(parsed.username());
    let password = percent_decode(parsed.password().unwrap_or(""));

    // Path is `/` then optional vhost. A bare `/` URL means default
    // vhost `/`; `/myvhost` means vhost `myvhost`.
    let path = parsed.path();
    let vhost = if path.is_empty() || path == "/" {
        "/".to_string()
    } else {
        // Strip the leading slash and percent-decode (vhosts may
        // contain `/` themselves which RabbitMQ encodes as `%2F`).
        let raw = path.strip_prefix('/').unwrap_or(path);
        percent_decode(raw)
    };

    Ok(AmqpUrlParts {
        host,
        port,
        username,
        password,
        vhost,
    })
}

fn percent_decode(s: &str) -> String {
    // Lightweight percent-decoder. The `url` crate's `Url::username`
    // / `Url::password` already partially decode, but only for
    // characters that are *required* to be percent-encoded in a
    // URL; we run another pass so encoded forms of allowed chars
    // (e.g. `%40` for `@` in passwords) also round-trip.
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|s| u8::from_str_radix(s, 16).ok());
            if let Some(b) = hex {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

/// Backwards-compat shim — older callers used the 4-tuple form
/// (host, port, username, password) and silently dropped vhost.
/// New code should use `parse_amqp_url_full` and pass vhost to
/// `OpenConnectionArguments::virtual_host`.
pub fn parse_amqp_url(url: &str) -> Result<(String, u16, String, String), AppError> {
    let p = parse_amqp_url_full(url)?;
    Ok((p.host, p.port, p.username, p.password))
}
