//! RabbitMQ batch consumer for the `transactions` write path.
//!
//! Phase-2 follow-up (Step A in `docs/architecture/cutover-readiness.md`):
//! the consumer was moved out of `crates/app/src/queue/consumer.rs` so the
//! `transactions` crate now owns its full write path
//! (HTTP handler → service → producer → consumer → DB write).
//!
//! Idempotency-key shape (`txn:{shard}:{reference_id}`) and the
//! `transactions.committed` event contract are preserved from the
//! pre-move implementation — v1-published in-flight messages still
//! match v2-reserved keys.

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicQosArguments},
    connection::{Connection, OpenConnectionArguments},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::db::shard::ShardRouter;
use shared_kernel::error::AppError;
use shared_kernel::events::{Event, EventPublisher};
use shared_kernel::queue::producer::parse_amqp_url;

const QUEUE_NAME: &str = "transactions.process";
const CONSUMER_TAG: &str = "peakload-consumer";
const BATCH_SIZE: usize = 50;
const BATCH_FLUSH_MS: u64 = 100;

/// Wire-shape DTO for the queue message. Owned by the consumer
/// because it represents the format the producer writes — the
/// legacy `crate::db::models::CreateTransactionRequest` no longer
/// crosses the crate boundary.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct CreateTransactionRequest {
    from_account: String,
    to_account: String,
    amount: Decimal,
    currency: String,
    reference_id: Option<String>,
    description: Option<String>,
}

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
    amount: Decimal,
    currency: String,
    reference_id: Option<String>,
    description: Option<String>,
    #[serde(default)]
    shard: usize,
    #[serde(default)]
    #[allow(dead_code)]
    request_id: String,
}

/// Start the shard-aware batch consumer.
///
/// Owns the AMQP connection, channel, prefetch tuning, the in-memory
/// batch buffer, and the timer that flushes partial batches every
/// `BATCH_FLUSH_MS`.
///
/// `events` is the cross-module bus from `shared_kernel`. After
/// each successful batch flush we publish one
/// `transactions.committed` event per row processed — the moment
/// `flush_batch_to_shards` returns `Ok(_)` the rows are durably
/// committed in Postgres, which is exactly what `committed` means.
///
/// The `cancel` token enables coordinated graceful shutdown: the
/// flush-timer drains the buffer one last time and exits rather
/// than sleeping forever.
pub async fn start_consumer(
    amqp_url: &str,
    shard_router: ShardRouter,
    events: Arc<dyn EventPublisher>,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, AppError> {
    let (host, port, username, password) = parse_amqp_url(amqp_url)?;

    let args = OpenConnectionArguments::new(&host, port, &username, &password);

    let connection = Connection::open(&args)
        .await
        .map_err(|e| AppError::Internal(format!("Consumer RabbitMQ connection error: {}", e)))?;

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

    // Share the channel so both the consumer callback and the timer flush
    // can ACK messages. The consumer callback only buffers (no individual
    // ACK); both code-paths ACK after a successful DB write.
    let shared_channel = Arc::new(channel);

    let consumer = BatchTransactionConsumer {
        shard_router: shard_router.clone(),
        buffer: Arc::new(Mutex::new(Vec::with_capacity(BATCH_SIZE))),
        channel: shared_channel.clone(),
        events: events.clone(),
    };

    // Spawn flush timer — respects the cancellation token
    let buffer_ref = consumer.buffer.clone();
    let router_ref = consumer.shard_router.clone();
    let timer_channel = shared_channel.clone();
    let timer_events = events.clone();
    let flush_cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(BATCH_FLUSH_MS)) => {
                    let mut buf = buffer_ref.lock().await;
                    if !buf.is_empty() {
                        let batch: Vec<PendingMessage> = buf.drain(..).collect();
                        drop(buf);
                        let max_tag = batch.iter().map(|m| m.delivery_tag).max().unwrap_or(0);
                        match flush_batch_to_shards(&batch, &router_ref).await {
                            Ok(count) => {
                                if let Err(e) = timer_channel
                                    .basic_ack(BasicAckArguments::new(max_tag, true))
                                    .await
                                {
                                    tracing::error!(error = %e, "Timer flush: failed to batch ACK");
                                }
                                metrics::counter!("transactions_processed_total").increment(count as u64);
                                metrics::histogram!("transactions_batch_size").record(count as f64);
                                publish_committed_events(&batch, &timer_events);
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Timer flush error, NACKing");
                                let _ = timer_channel
                                    .basic_nack(BasicNackArguments::new(max_tag, true, true))
                                    .await;
                            }
                        }
                    }
                }
                _ = flush_cancel.cancelled() => {
                    tracing::info!("Consumer flush timer: cancellation received, draining buffer...");
                    let mut buf = buffer_ref.lock().await;
                    if !buf.is_empty() {
                        let batch: Vec<PendingMessage> = buf.drain(..).collect();
                        drop(buf);
                        let max_tag = batch.iter().map(|m| m.delivery_tag).max().unwrap_or(0);
                        match flush_batch_to_shards(&batch, &router_ref).await {
                            Ok(count) => {
                                let _ = timer_channel
                                    .basic_ack(BasicAckArguments::new(max_tag, true))
                                    .await;
                                tracing::info!(count = count, "Final buffer flushed and ACK'd successfully");
                                publish_committed_events(&batch, &timer_events);
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Final flush error during shutdown, NACKing");
                                let _ = timer_channel
                                    .basic_nack(BasicNackArguments::new(max_tag, true, true))
                                    .await;
                            }
                        }
                    }
                    tracing::info!("Consumer flush timer exiting");
                    break;
                }
            }
        }
    });

    let consume_args = BasicConsumeArguments::new(QUEUE_NAME, CONSUMER_TAG);

    let consume_channel_ref = shared_channel.clone();
    consume_channel_ref
        .basic_consume(consumer, consume_args)
        .await
        .map_err(|e| AppError::Internal(format!("Consume error: {}", e)))?;

    tracing::info!(
        queue = QUEUE_NAME,
        batch_size = BATCH_SIZE,
        "RabbitMQ shard-aware batch consumer started"
    );

    let handle = tokio::spawn(async move {
        let _connection = connection;
        let _channel = shared_channel;
        cancel.cancelled().await;
        tracing::info!("Consumer connection holder: cancellation received, closing...");
    });

    Ok(handle)
}

struct BatchTransactionConsumer {
    shard_router: ShardRouter,
    buffer: Arc<Mutex<Vec<PendingMessage>>>,
    /// Shared channel reference for batch ACK from the consume callback.
    channel: Arc<amqprs::channel::Channel>,
    /// Shared-kernel event bus. After a batch successfully flushes to
    /// the database we publish one `transactions.committed` event per
    /// row so subscribers (`notifications`, future `analytics`) can
    /// react without coupling to this module.
    events: Arc<dyn EventPublisher>,
}

#[async_trait]
impl AsyncConsumer for BatchTransactionConsumer {
    async fn consume(
        &mut self,
        _channel: &amqprs::channel::Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let delivery_tag = deliver.delivery_tag();

        let payload: QueuePayload = match serde_json::from_slice(&content) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "Invalid message format, NACKing to DLQ");
                metrics::counter!("dlq_messages_total").increment(1);
                let _ = _channel
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
                    if let Err(e) = self
                        .channel
                        .basic_ack(BasicAckArguments::new(max_tag, true))
                        .await
                    {
                        tracing::error!(error = %e, "Failed to batch ACK");
                    }
                    metrics::counter!("transactions_processed_total").increment(count as u64);
                    metrics::histogram!("transactions_batch_size").record(count as f64);
                    publish_committed_events(&batch, &self.events);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Batch flush error, NACKing");
                    let _ = self
                        .channel
                        .basic_nack(BasicNackArguments::new(max_tag, true, true))
                        .await;
                }
            }
        }
        // No individual ACK here — messages stay unacknowledged
        // until the batch is flushed (either by size threshold above
        // or by the timer task). QoS prefetch limits how many un-ACK'd
        // messages RabbitMQ will deliver.
    }
}

/// Flush a batch to the correct shards using bulk INSERT per shard.
///
/// Groups by sender shard and runs each group via `process_shard_batch`
/// in parallel. The whole sender-shard write path (debit + same-shard
/// credit + transactions row) is one DB transaction so a partial
/// failure cannot leak money. Cross-shard credits run AFTER commit
/// best-effort — the only remaining money-mid-air window is a hard
/// failure on the receiver shard between the sender's commit and the
/// credit, which is logged and counted via
/// `cross_shard_credit_failures_total` for reconciliation.
async fn flush_batch_to_shards(
    batch: &[PendingMessage],
    router: &ShardRouter,
) -> Result<usize, AppError> {
    if batch.is_empty() {
        return Ok(0);
    }

    let mut shard_groups: std::collections::HashMap<usize, Vec<&PendingMessage>> =
        std::collections::HashMap::new();
    for msg in batch {
        shard_groups.entry(msg.shard).or_default().push(msg);
    }

    let total = batch.len();

    let mut handles = Vec::new();
    for (sender_shard, messages) in shard_groups {
        let router = router.clone();
        let pool = router.writer(sender_shard).clone();
        let owned: Vec<PendingMessage> = messages.into_iter().cloned().collect();

        handles.push(tokio::spawn(async move {
            process_shard_batch(&pool, sender_shard, &owned, &router).await
        }));
    }

    for handle in handles {
        handle
            .await
            .map_err(|e| AppError::Internal(format!("Join error: {}", e)))??;
    }

    Ok(total)
}

/// Process all messages whose sender lives on `sender_shard`.
///
/// Atomicity model:
///   * Inside ONE tx on `pool`: idempotency check (SELECT existing
///     reference_ids), debit (UPDATE … WHERE balance >= …),
///     same-shard credit (when receiver lives on `sender_shard`),
///     and the bulk INSERT of completed/failed transaction rows.
///   * Cross-shard credit runs AFTER tx.commit. If it fails the row
///     is already durably committed and the sender already debited;
///     the incident is logged and counted but NOT re-driven via
///     queue NACK (a NACK would re-debit on redelivery — see the
///     idempotency note below — but it would also re-emit the
///     `transactions.committed` event, complicating downstream).
///
/// Idempotency on redelivery:
///   * The SELECT-then-skip path drops messages whose `reference_id`
///     already has a row. Because the SELECT runs INSIDE the same
///     tx as the UPDATE, the only race window is concurrent
///     processing of the *same* delivery on two replicas, which
///     RabbitMQ does not do (a message is delivered to one consumer
///     at a time). NACK+requeue → next delivery sees the existing
///     transactions row → skipped.
async fn process_shard_batch(
    pool: &sqlx::PgPool,
    sender_shard: usize,
    messages: &[PendingMessage],
    router: &ShardRouter,
) -> Result<(), AppError> {
    let mut tx = pool.begin().await?;

    // ─── Idempotency: drop already-processed reference_ids ──
    let ref_ids: Vec<String> = messages
        .iter()
        .filter_map(|m| m.request.reference_id.clone())
        .collect();
    let already_processed: std::collections::HashSet<String> = if ref_ids.is_empty() {
        std::collections::HashSet::new()
    } else {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT reference_id FROM transactions WHERE reference_id = ANY($1)",
        )
        .bind(&ref_ids)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .flatten()
        .collect()
    };

    let mut completed_rows: Vec<&PendingMessage> = Vec::with_capacity(messages.len());
    let mut failed_rows: Vec<&PendingMessage> = Vec::new();
    let mut cross_shard_credits: Vec<(String, Decimal)> = Vec::new();

    for msg in messages {
        if let Some(ref_id) = msg.request.reference_id.as_deref() {
            if already_processed.contains(ref_id) {
                tracing::debug!(
                    reference_id = ref_id,
                    "Skipping already-processed message (idempotent redelivery)"
                );
                continue;
            }
        }

        // Debit (in tx)
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
                "Insufficient balance — recording as failed"
            );
            failed_rows.push(msg);
            continue;
        }

        // Same-shard credit lives in the same tx as the debit so the
        // pair is atomic. Cross-shard credit is queued for after the
        // commit because it talks to a different DB.
        let receiver_shard = ShardRouter::shard_for(&msg.request.to_account);
        if receiver_shard == sender_shard {
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
        } else {
            cross_shard_credits
                .push((msg.request.to_account.clone(), msg.request.amount));
        }

        completed_rows.push(msg);
    }

    // ─── Bulk INSERT completed (in tx) ──────────────────────
    if !completed_rows.is_empty() {
        let count = completed_rows.len();
        let mut ids: Vec<Uuid> = Vec::with_capacity(count);
        let mut from_accounts: Vec<String> = Vec::with_capacity(count);
        let mut to_accounts: Vec<String> = Vec::with_capacity(count);
        let mut amounts: Vec<Decimal> = Vec::with_capacity(count);
        let mut currencies: Vec<String> = Vec::with_capacity(count);
        let mut reference_ids: Vec<Option<String>> = Vec::with_capacity(count);

        for msg in &completed_rows {
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
                $4::numeric[],
                $5::text[],
                ARRAY_FILL('completed'::text, ARRAY[$7::int]),
                $6::text[],
                ARRAY_FILL(NOW()::timestamptz, ARRAY[$7::int])
            )
            ON CONFLICT (reference_id) DO NOTHING
            "#,
        )
        .bind(&ids)
        .bind(&from_accounts)
        .bind(&to_accounts)
        .bind(&amounts)
        .bind(&currencies)
        .bind(&reference_ids)
        .bind(count as i32)
        .execute(&mut *tx)
        .await?;
    }

    // ─── Bulk INSERT failed (in tx) ─────────────────────────
    if !failed_rows.is_empty() {
        let count = failed_rows.len();
        let mut ids: Vec<Uuid> = Vec::with_capacity(count);
        let mut from_accounts: Vec<String> = Vec::with_capacity(count);
        let mut to_accounts: Vec<String> = Vec::with_capacity(count);
        let mut amounts: Vec<Decimal> = Vec::with_capacity(count);
        let mut currencies: Vec<String> = Vec::with_capacity(count);
        let mut reference_ids: Vec<Option<String>> = Vec::with_capacity(count);
        let mut descriptions: Vec<Option<String>> = Vec::with_capacity(count);

        for msg in &failed_rows {
            ids.push(Uuid::new_v4());
            from_accounts.push(msg.request.from_account.clone());
            to_accounts.push(msg.request.to_account.clone());
            amounts.push(msg.request.amount);
            currencies.push(msg.request.currency.clone());
            reference_ids.push(msg.request.reference_id.clone());
            descriptions.push(msg.request.description.clone());
        }

        sqlx::query(
            r#"
            INSERT INTO transactions (
                id, from_account, to_account, amount,
                currency, status, reference_id, description
            )
            SELECT * FROM UNNEST(
                $1::uuid[],
                $2::text[],
                $3::text[],
                $4::numeric[],
                $5::text[],
                ARRAY_FILL('failed'::text, ARRAY[$8::int]),
                $6::text[],
                $7::text[]
            )
            ON CONFLICT (reference_id) DO NOTHING
            "#,
        )
        .bind(&ids)
        .bind(&from_accounts)
        .bind(&to_accounts)
        .bind(&amounts)
        .bind(&currencies)
        .bind(&reference_ids)
        .bind(&descriptions)
        .bind(count as i32)
        .execute(&mut *tx)
        .await?;
    }

    // Atomic: debits + same-shard credits + transaction rows.
    tx.commit().await?;

    // ─── Cross-shard credits (post-commit, best-effort) ─────
    // Sender side is durably committed at this point. If a
    // receiver-shard write fails we surface it via metric +
    // log for reconciliation; we deliberately do NOT return Err
    // because that would NACK+requeue, and the next delivery
    // would skip via the idempotency check above — so the credit
    // would never run at all.
    for (to_acc, amount) in cross_shard_credits {
        let receiver_shard = ShardRouter::shard_for(&to_acc);
        let receiver_pool = router.writer(receiver_shard);
        if let Err(e) = sqlx::query(
            r#"
            UPDATE users
            SET balance = balance + $1
            WHERE account_number = $2
            "#,
        )
        .bind(amount)
        .bind(&to_acc)
        .execute(receiver_pool)
        .await
        {
            tracing::error!(
                error = %e,
                account = %to_acc,
                amount = %amount,
                "CROSS-SHARD CREDIT FAILED post-commit — manual reconciliation required"
            );
            metrics::counter!("cross_shard_credit_failures_total").increment(1);
        }
    }

    Ok(())
}

// ─── Cross-module event publication ─────────────────────────

/// Wire-level payload for the `transactions.committed` event.
///
/// Owned by the publisher (this consumer). Subscribers in other
/// modules deserialise into THEIR own struct — see e.g.
/// `notifications::domain::TransactionCommittedDomainEvent`.
/// Renaming a field here breaks the wire contract; add fields
/// (with `serde(default)`) freely.
#[derive(serde::Serialize)]
struct TransactionCommittedPayload<'a> {
    from_account: &'a str,
    to_account: &'a str,
    amount: String,
    currency: &'a str,
    reference_id: Option<&'a str>,
    shard: usize,
}

/// Publish one `transactions.committed` event per row in `batch`.
///
/// Best-effort — failures here are logged + counted but never
/// propagate, because the row is already durably committed and
/// nothing about the queue ACK should depend on a notification
/// channel being healthy.
fn publish_committed_events(batch: &[PendingMessage], events: &Arc<dyn EventPublisher>) {
    for msg in batch {
        let payload = TransactionCommittedPayload {
            from_account: &msg.request.from_account,
            to_account: &msg.request.to_account,
            amount: msg.request.amount.to_string(),
            currency: &msg.request.currency,
            reference_id: msg.request.reference_id.as_deref(),
            shard: msg.shard,
        };
        let event = match Event::new("transactions.committed", &payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(error = %e, "Failed to build TransactionCommitted event");
                metrics::counter!("events_build_errors_total").increment(1);
                continue;
            }
        };
        if let Err(e) = events.publish(event) {
            tracing::warn!(error = %e, "EventBus publish failed");
            metrics::counter!("events_publish_errors_total").increment(1);
        } else {
            metrics::counter!("events_published_total", "name" => "transactions.committed")
                .increment(1);
        }
    }
}
