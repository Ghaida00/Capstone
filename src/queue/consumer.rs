use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicQosArguments},
    connection::{Connection, OpenConnectionArguments},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::config::Config;
use crate::db::models::CreateTransactionRequest;
use crate::db::shard::ShardRouter;
use crate::error::AppError;

const QUEUE_NAME: &str = "transactions.process";
const CONSUMER_TAG: &str = "gn-consumer";
const BATCH_SIZE: usize = 50;
const BATCH_FLUSH_MS: u64 = 100;

/// Pending message awaiting batch flush.
#[derive(Clone)]
struct PendingMessage {
    delivery_tag: u64,
    request: CreateTransactionRequest,
    shard: usize,
}

/// Message payload from queue (includes shard info).
#[derive(serde::Deserialize)]
struct QueuePayload {
    from_account: String,
    to_account: String,
    amount: f64,
    currency: String,
    reference_id: Option<String>,
    description: Option<String>,
    #[serde(default)]
    shard: usize,
    #[serde(default)]
    #[allow(dead_code)]
    request_id: String,
}

/// RabbitMQ consumer that processes transactions in batches, shard-aware.
pub struct QueueConsumer;

impl QueueConsumer {
    /// Start consuming messages from the transaction queue.
    ///
    /// The `cancel` token enables coordinated graceful shutdown: when
    /// triggered, the flush-timer loop will drain any remaining buffer
    /// and then exit rather than sleeping forever.
    pub async fn start(
        config: &Config,
        shard_router: ShardRouter,
        cancel: CancellationToken,
    ) -> Result<JoinHandle<()>, AppError> {
        let (host, port, username, password) =
            super::producer::parse_amqp_url(&config.rabbitmq_url)?;

        let args = OpenConnectionArguments::new(&host, port, &username, &password);

        let connection = Connection::open(&args).await.map_err(|e| {
            AppError::Internal(format!("Consumer RabbitMQ connection error: {}", e))
        })?;

        connection
            .register_callback(DefaultConnectionCallback)
            .await
            .map_err(|e| AppError::Internal(format!("Consumer callback error: {}", e)))?;

        let channel = connection
            .open_channel(None)
            .await
            .map_err(|e| AppError::Internal(format!("Consumer channel error: {}", e)))?;

        channel
            .register_callback(DefaultChannelCallback)
            .await
            .map_err(|e| AppError::Internal(format!("Consumer channel callback error: {}", e)))?;

        channel
            .basic_qos(BasicQosArguments::new(0, BATCH_SIZE as u16, false))
            .await
            .map_err(|e| AppError::Internal(format!("QOS error: {}", e)))?;

        let consumer = BatchTransactionConsumer {
            shard_router: shard_router.clone(),
            buffer: Arc::new(Mutex::new(Vec::with_capacity(BATCH_SIZE))),
        };

        // Spawn flush timer — respects the cancellation token
        let buffer_ref = consumer.buffer.clone();
        let router_ref = consumer.shard_router.clone();
        let flush_cancel = cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(BATCH_FLUSH_MS)) => {
                        let mut buf = buffer_ref.lock().await;
                        if !buf.is_empty() {
                            let batch: Vec<PendingMessage> = buf.drain(..).collect();
                            drop(buf);
                            if let Err(e) = flush_batch_to_shards(&batch, &router_ref).await {
                                tracing::error!(error = %e, "Timer flush error");
                            }
                        }
                    }
                    _ = flush_cancel.cancelled() => {
                        // Final flush on shutdown
                        tracing::info!("Consumer flush timer: cancellation received, draining buffer...");
                        let mut buf = buffer_ref.lock().await;
                        if !buf.is_empty() {
                            let batch: Vec<PendingMessage> = buf.drain(..).collect();
                            drop(buf);
                            if let Err(e) = flush_batch_to_shards(&batch, &router_ref).await {
                                tracing::error!(error = %e, "Final flush error during shutdown");
                            } else {
                                tracing::info!(count = batch.len(), "Final buffer flushed successfully");
                            }
                        }
                        tracing::info!("Consumer flush timer exiting");
                        break;
                    }
                }
            }
        });

        let consume_args = BasicConsumeArguments::new(QUEUE_NAME, CONSUMER_TAG);

        channel
            .basic_consume(consumer, consume_args)
            .await
            .map_err(|e| AppError::Internal(format!("Consume error: {}", e)))?;

        tracing::info!(
            queue = QUEUE_NAME,
            batch_size = BATCH_SIZE,
            "RabbitMQ shard-aware batch consumer started"
        );

        // Keep the connection alive until cancellation
        let handle = tokio::spawn(async move {
            let _connection = connection;
            let _channel = channel;
            cancel.cancelled().await;
            tracing::info!("Consumer connection holder: cancellation received, closing...");
        });

        Ok(handle)
    }
}

struct BatchTransactionConsumer {
    shard_router: ShardRouter,
    buffer: Arc<Mutex<Vec<PendingMessage>>>,
}

#[async_trait]
impl AsyncConsumer for BatchTransactionConsumer {
    async fn consume(
        &mut self,
        channel: &amqprs::channel::Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let delivery_tag = deliver.delivery_tag();

        let payload: QueuePayload = match serde_json::from_slice(&content) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "Invalid message format, NACKing");
                let _ = channel
                    .basic_nack(BasicNackArguments::new(delivery_tag, false, false))
                    .await;
                return;
            }
        };

        let shard = payload.shard;
        let request = CreateTransactionRequest {
            from_account: payload.from_account,
            to_account: payload.to_account,
            amount: payload.amount,
            currency: payload.currency,
            reference_id: payload.reference_id,
            description: payload.description,
        };

        let should_flush;
        {
            let mut buf = self.buffer.lock().await;
            buf.push(PendingMessage {
                delivery_tag,
                request,
                shard,
            });
            should_flush = buf.len() >= BATCH_SIZE;
        }

        if should_flush {
            let batch: Vec<PendingMessage> = {
                let mut buf = self.buffer.lock().await;
                buf.drain(..).collect()
            };

            let max_tag = batch.iter().map(|m| m.delivery_tag).max().unwrap_or(0);

            match flush_batch_to_shards(&batch, &self.shard_router).await {
                Ok(count) => {
                    if let Err(e) = channel
                        .basic_ack(BasicAckArguments::new(max_tag, true))
                        .await
                    {
                        tracing::error!(error = %e, "Failed to batch ACK");
                    }
                    metrics::counter!("transactions_processed_total").increment(count as u64);
                    metrics::histogram!("transactions_batch_size").record(count as f64);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Batch flush error, NACKing");
                    let _ = channel
                        .basic_nack(BasicNackArguments::new(max_tag, true, true))
                        .await;
                }
            }
        } else {
            if let Err(e) = channel
                .basic_ack(BasicAckArguments::new(delivery_tag, false))
                .await
            {
                tracing::error!(error = %e, "Failed to ACK message");
            }
        }
    }
}

/// Flush a batch to the correct shards using bulk INSERT per shard.
async fn flush_batch_to_shards(
    batch: &[PendingMessage],
    router: &ShardRouter,
) -> Result<usize, AppError> {
    if batch.is_empty() {
        return Ok(0);
    }

    // Group messages by shard
    let mut shard_groups: std::collections::HashMap<usize, Vec<&PendingMessage>> =
        std::collections::HashMap::new();
    for msg in batch {
        shard_groups.entry(msg.shard).or_default().push(msg);
    }

    let total = batch.len();

    // Insert into each shard in parallel
    let mut handles = Vec::new();
    for (shard_idx, messages) in shard_groups {
        let pool = router.writer(shard_idx).clone();

        // clone messages supaya owned
        let owned_messages: Vec<PendingMessage> =
            messages.into_iter().cloned().collect();

        let count = owned_messages.len();

        let mut ids: Vec<Uuid> = Vec::with_capacity(count);
        let mut from_accounts: Vec<String> = Vec::with_capacity(count);
        let mut to_accounts: Vec<String> = Vec::with_capacity(count);
        let mut amounts: Vec<f64> = Vec::with_capacity(count);
        let mut currencies: Vec<String> = Vec::with_capacity(count);
        let mut reference_ids: Vec<Option<String>> = Vec::with_capacity(count);

        for msg in &owned_messages {
            ids.push(Uuid::new_v4());
            from_accounts.push(msg.request.from_account.clone());
            to_accounts.push(msg.request.to_account.clone());
            amounts.push(msg.request.amount);
            currencies.push(msg.request.currency.clone());
            reference_ids.push(msg.request.reference_id.clone());
        }

        handles.push(tokio::spawn(async move {

            apply_balance_updates(&pool, &owned_messages).await?;

            let count = owned_messages.len();

            let mut ids = Vec::with_capacity(count);
            let mut from_accounts = Vec::with_capacity(count);
            let mut to_accounts = Vec::with_capacity(count);
            let mut amounts = Vec::with_capacity(count);
            let mut currencies = Vec::with_capacity(count);
            let mut reference_ids = Vec::with_capacity(count);

            for msg in &owned_messages {
                ids.push(Uuid::new_v4());
                from_accounts.push(msg.request.from_account.clone());
                to_accounts.push(msg.request.to_account.clone());
                amounts.push(msg.request.amount);
                currencies.push(msg.request.currency.clone());
                reference_ids.push(msg.request.reference_id.clone());
            }

            sqlx::query(
                r#"
                INSERT INTO transactions (
                    id, from_account, to_account, amount,
                    currency, status, reference_id, processed_at
                )
                SELECT * FROM UNNEST(
                    $1::uuid[],
                    $2::text[],
                    $3::text[],
                    $4::float8[],
                    $5::text[],
                    ARRAY_FILL('completed'::text, ARRAY[$7::int]),
                    $6::text[],
                    ARRAY_FILL(NOW()::timestamptz, ARRAY[$7::int])
                )
                ON CONFLICT (reference_id) DO NOTHING
                "#
            )
            .bind(&ids)
            .bind(&from_accounts)
            .bind(&to_accounts)
            .bind(&amounts)
            .bind(&currencies)
            .bind(&reference_ids)
            .bind(count as i32)
            .execute(&pool)
            .await
            .map_err(AppError::Database)?;

            Ok::<(), AppError>(())
        }));
    }

    for handle in handles {
        handle
            .await
            .map_err(|e| AppError::Internal(format!("Join error: {}", e)))??;
    }

    Ok(total)
}

async fn apply_balance_updates(
    pool: &sqlx::PgPool,
    messages: &[PendingMessage],
) -> Result<(), AppError> {

    let mut tx = pool.begin().await?;

    for msg in messages {

        // debit sender
        let updated = sqlx::query(
            r#"
            UPDATE users
            SET balance = balance - $1
            WHERE account_number = $2
            AND balance >= $1
            "#,
        )
        .bind(msg.request.amount)
        .bind(&msg.request.from_account)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            tracing::warn!(
                from_account = %msg.request.from_account,
                amount = %msg.request.amount,
                "Insufficient balance — marking transaction as failed"
            );

            sqlx::query(
                r#"
                INSERT INTO transactions (
                    from_account,
                    to_account,
                    amount,
                    currency,
                    status,
                    reference_id,
                    description
                )
                VALUES ($1,$2,$3,$4,'failed',$5,$6)
                ON CONFLICT (reference_id) DO NOTHING
                "#
            )
            .bind(&msg.request.from_account)
            .bind(&msg.request.to_account)
            .bind(&msg.request.amount)
            .bind(&msg.request.currency)
            .bind(&msg.request.reference_id)
            .bind(&msg.request.description)
            .execute(pool)
            .await?;

            continue;
        }

        // credit receiver
        sqlx::query(
            r#"
            UPDATE users
            SET balance = balance + $1
            WHERE account_number = $2
            "#,
        )
        .bind(msg.request.amount)
        .bind(&msg.request.to_account)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    Ok(())
}
