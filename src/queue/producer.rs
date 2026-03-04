use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        BasicPublishArguments, ExchangeDeclareArguments, QueueBindArguments,
        QueueDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    BasicProperties,
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::error::AppError;

const EXCHANGE_NAME: &str = "gn.transactions";
const QUEUE_NAME: &str = "transactions.process";
const ROUTING_KEY: &str = "transaction.created";
const DLX_EXCHANGE: &str = "gn.transactions.dlx";
const DLX_QUEUE: &str = "transactions.dead_letter";

/// RabbitMQ message producer with automatic reconnection.
#[derive(Clone)]
pub struct QueueProducer {
    channel: Arc<Mutex<Option<amqprs::channel::Channel>>>,
    connected: Arc<std::sync::atomic::AtomicBool>,
    config: Arc<ProducerConfig>,
}

/// Stored config for reconnection.
struct ProducerConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
}

impl QueueProducer {
    /// Connect to RabbitMQ and set up exchanges and queues.
    pub async fn new(config: &Config) -> Result<Self, AppError> {
        let (host, port, username, password) = parse_amqp_url(&config.rabbitmq_url)?;

        let producer_config = Arc::new(ProducerConfig {
            host: host.clone(),
            port,
            username: username.clone(),
            password: password.clone(),
        });

        let channel = Self::connect_and_setup(&host, port, &username, &password).await?;

        tracing::info!(
            exchange = EXCHANGE_NAME,
            queue = QUEUE_NAME,
            "RabbitMQ producer initialized (amqprs)"
        );

        Ok(Self {
            channel: Arc::new(Mutex::new(Some(channel))),
            connected: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            config: producer_config,
        })
    }

    /// Internal: establish connection and declare topology.
    async fn connect_and_setup(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
    ) -> Result<amqprs::channel::Channel, AppError> {
        let args = OpenConnectionArguments::new(host, port, username, password);

        let connection = Connection::open(&args)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ connection error: {}", e)))?;

        connection
            .register_callback(DefaultConnectionCallback)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ callback error: {}", e)))?;

        let channel = connection
            .open_channel(None)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ channel error: {}", e)))?;

        channel
            .register_callback(DefaultChannelCallback)
            .await
            .map_err(|e| AppError::Internal(format!("RabbitMQ channel callback error: {}", e)))?;

        // Declare Dead Letter Exchange
        let dlx_args = ExchangeDeclareArguments::new(DLX_EXCHANGE, "direct");
        channel
            .exchange_declare(dlx_args)
            .await
            .map_err(|e| AppError::Internal(format!("DLX exchange declare error: {}", e)))?;

        // Declare Dead Letter Queue
        let dlq_args = QueueDeclareArguments::durable_client_named(DLX_QUEUE);
        channel
            .queue_declare(dlq_args)
            .await
            .map_err(|e| AppError::Internal(format!("DLX queue declare error: {}", e)))?;

        let dlq_bind_args = QueueBindArguments::new(DLX_QUEUE, DLX_EXCHANGE, "dead_letter");
        channel
            .queue_bind(dlq_bind_args)
            .await
            .map_err(|e| AppError::Internal(format!("DLX queue bind error: {}", e)))?;

        // Declare main exchange
        let ex_args = ExchangeDeclareArguments::new(EXCHANGE_NAME, "direct");
        channel
            .exchange_declare(ex_args)
            .await
            .map_err(|e| AppError::Internal(format!("Exchange declare error: {}", e)))?;

        // Declare main queue with DLX arguments
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
        channel
            .queue_declare(main_queue_args)
            .await
            .map_err(|e| AppError::Internal(format!("Queue declare error: {}", e)))?;

        // Bind queue to exchange
        let bind_args = QueueBindArguments::new(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        channel
            .queue_bind(bind_args)
            .await
            .map_err(|e| AppError::Internal(format!("Queue bind error: {}", e)))?;

        Ok(channel)
    }

    /// Attempt to reconnect to RabbitMQ.
    async fn reconnect(&self) -> Result<(), AppError> {
        tracing::warn!("RabbitMQ: attempting reconnection...");

        let new_channel = Self::connect_and_setup(
            &self.config.host,
            self.config.port,
            &self.config.username,
            &self.config.password,
        )
        .await?;

        let mut channel_guard = self.channel.lock().await;
        *channel_guard = Some(new_channel);
        self.connected
            .store(true, std::sync::atomic::Ordering::SeqCst);

        tracing::info!("RabbitMQ: reconnected successfully");
        metrics::counter!("rabbitmq_reconnections_total").increment(1);
        Ok(())
    }

    /// Publish a message to the transaction queue.
    /// Automatically attempts reconnection on failure.
    pub async fn publish<T: Serialize>(&self, message: &T) -> Result<(), AppError> {
        let payload = serde_json::to_vec(message)?;

        let properties = BasicProperties::default()
            .with_content_type("application/json")
            .with_delivery_mode(2) // persistent
            .finish();

        let args = BasicPublishArguments::new(EXCHANGE_NAME, ROUTING_KEY);

        // First attempt
        {
            let channel_guard = self.channel.lock().await;
            if let Some(ref channel) = *channel_guard {
                match channel
                    .basic_publish(properties.clone(), payload.clone(), args.clone())
                    .await
                {
                    Ok(_) => {
                        tracing::trace!("Message published");
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "RabbitMQ publish failed, will retry after reconnect");
                        self.connected
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
        }

        // Reconnect and retry once
        self.reconnect().await?;

        let channel_guard = self.channel.lock().await;
        if let Some(ref channel) = *channel_guard {
            channel
                .basic_publish(properties, payload, args)
                .await
                .map_err(|e| AppError::Internal(format!("Publish error after reconnect: {}", e)))?;
        } else {
            return Err(AppError::Internal("No RabbitMQ channel available".into()));
        }

        tracing::trace!("Message published (after reconnect)");
        Ok(())
    }

    /// Check if RabbitMQ connection is healthy.
    pub fn health_check(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
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

    let (host, port_str) = hostport
        .split_once(':')
        .unwrap_or((hostport, "5672"));

    let port_str = port_str.split('/').next().unwrap_or("5672");
    let port: u16 = port_str
        .parse()
        .map_err(|_| AppError::Internal("Invalid AMQP port".into()))?;

    Ok((host.to_string(), port, username.to_string(), password.to_string()))
}
