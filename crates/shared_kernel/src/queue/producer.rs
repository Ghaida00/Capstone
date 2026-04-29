use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        BasicPublishArguments, ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    BasicProperties,
};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::AppError;

const EXCHANGE_NAME: &str = "peakload.transactions";
const QUEUE_NAME: &str = "transactions.process";
const ROUTING_KEY: &str = "transaction.created";
const DLX_EXCHANGE: &str = "peakload.transactions.dlx";
const DLX_QUEUE: &str = "transactions.dead_letter";

/// Number of AMQP channels in the publish pool. Each channel inside
/// amqprs serialises its own writes; previously a single shared
/// channel under a `Mutex` meant every concurrent publish queued
/// behind the prior one. Eight channels lets eight publishes proceed
/// in parallel — enough for our 500–1000 VU envelope without forcing
/// RabbitMQ to manage a huge channel set.
const CHANNEL_POOL_SIZE: usize = 8;

/// One AMQP connection plus a pool of channels for parallel publishes.
struct ConnState {
    _connection: Connection,
    channels: Vec<Arc<amqprs::channel::Channel>>,
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
}

impl QueueProducer {
    pub async fn new(amqp_url: &str) -> Result<Self, AppError> {
        let (host, port, username, password) = parse_amqp_url(amqp_url)?;

        let producer_config = Arc::new(ProducerConfig {
            host: host.clone(),
            port,
            username: username.clone(),
            password: password.clone(),
        });

        let state = Self::connect_and_setup(&host, port, &username, &password).await?;

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
    ) -> Result<ConnState, AppError> {
        let args = OpenConnectionArguments::new(host, port, username, password);

        let connection = Connection::open(&args)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ connection error: {}", e)))?;

        connection
            .register_callback(DefaultConnectionCallback)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ callback error: {}", e)))?;

        // Declare topology exactly once on the first channel; every
        // other channel just publishes to the same exchange.
        let setup_channel = connection
            .open_channel(None)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ channel error: {}", e)))?;
        setup_channel
            .register_callback(DefaultChannelCallback)
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

        let mut channels: Vec<Arc<amqprs::channel::Channel>> =
            Vec::with_capacity(CHANNEL_POOL_SIZE);
        channels.push(Arc::new(setup_channel));
        for _ in 1..CHANNEL_POOL_SIZE {
            let ch = connection
                .open_channel(None)
                .await
                .map_err(|e| AppError::Internal(format!("RabbitMQ channel error: {}", e)))?;
            ch.register_callback(DefaultChannelCallback)
                .await
                .map_err(|e| {
                    AppError::Internal(format!("RabbitMQ channel callback error: {}", e))
                })?;
            channels.push(Arc::new(ch));
        }

        Ok(ConnState {
            _connection: connection,
            channels,
        })
    }

    /// Publish a message to the transaction queue. Hot path is
    /// lock-free under steady state; only reconnect takes the
    /// `RwLock` write side.
    pub async fn publish<T: Serialize>(&self, message: &T) -> Result<(), AppError> {
        let payload = serde_json::to_vec(message)?;

        let properties = BasicProperties::default()
            .with_content_type("application/json")
            .with_delivery_mode(2)
            .finish();

        let args = BasicPublishArguments::new(EXCHANGE_NAME, ROUTING_KEY);

        let snapshot = self.state.read().await.clone();
        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % snapshot.channels.len();
        let channel = snapshot.channels[idx].clone();

        match channel
            .basic_publish(properties.clone(), payload.clone(), args.clone())
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "RabbitMQ publish failed, reconnecting...");
                self.connected.store(false, Ordering::SeqCst);
            }
        }

        // Reconnect — only one task does the actual rebuild; others
        // race onto the new state once it lands.
        let mut guard = self.state.write().await;
        if !self.connected.load(Ordering::SeqCst) {
            tracing::warn!("RabbitMQ: rebuilding channel pool...");
            let new_state = Self::connect_and_setup(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &self.config.password,
            )
            .await?;
            *guard = Arc::new(new_state);
            self.connected.store(true, Ordering::SeqCst);
            metrics::counter!("rabbitmq_reconnections_total").increment(1);
        }
        let snapshot = guard.clone();
        drop(guard);

        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % snapshot.channels.len();
        let channel = snapshot.channels[idx].clone();
        channel
            .basic_publish(properties, payload, args)
            .await
            .map_err(|e| AppError::Internal(format!("Publish error after reconnect: {}", e)))?;
        Ok(())
    }

    pub fn health_check(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}

/// Parse amqp://user:pass@host:port into components.
pub fn parse_amqp_url(url: &str) -> Result<(String, u16, String, String), AppError> {
    let url = url
        .strip_prefix("amqp://")
        .ok_or_else(|| AppError::Internal("Invalid AMQP URL scheme".into()))?;

    let (userinfo, hostport) = url
        .split_once('@')
        .ok_or_else(|| AppError::Internal("Invalid AMQP URL: missing @".into()))?;

    let (username, password) = userinfo
        .split_once(':')
        .ok_or_else(|| AppError::Internal("Invalid AMQP URL: missing password".into()))?;

    let (host, port_str) = hostport.split_once(':').unwrap_or((hostport, "5672"));

    let port_str = port_str.split('/').next().unwrap_or("5672");
    let port: u16 = port_str
        .parse()
        .map_err(|_| AppError::Internal("Invalid AMQP port".into()))?;

    Ok((
        host.to_string(),
        port,
        username.to_string(),
        password.to_string(),
    ))
}
