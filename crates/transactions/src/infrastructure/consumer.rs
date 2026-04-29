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
//!
//! Bleed-stop bundle (this rewrite): restored in-tx idempotency
//! check, re-derives the destination shard from `from_account`
//! (drops trust in the wire-supplied `shard` field), bulk INSERTs
//! moved INTO the same tx as debit/credit (atomic), `processed_at`
//! set on failed rows, same-shard credit failures roll back via
//! atomic refund within the tx, ACKs are per-tag (`multiple=false`)
//! to prevent cumulative-tag races between concurrent flushes,
//! empty-batch ACK guarded against `delivery_tag=0`, cross-shard
//! credits run in parallel post-commit.
//!
//! Cross-shard credit failure remains best-effort + metric; the
//! outbox-table fix lives in a follow-up bundle.

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
use shared_kernel::events::{Event, EventPublisher, EVENT_TRANSACTIONS_COMMITTED};

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
    /// Sender shard, derived locally from `from_account` rather
    /// than trusting the wire field — the wire `shard` was a
    /// trust boundary leak that let a buggy/malicious producer
    /// route debits to the wrong DB.
    shard: usize,
    /// Request id propagated from the producer for cross-process
    /// trace correlation. Empty when the producer didn't supply
    /// one. Attached to the per-batch span so log lines can be
    /// joined with the originating HTTP request.
    request_id: String,
}

/// Message payload from queue.
///
/// `shard` is no longer read — kept on the wire for backwards
/// compat but the consumer re-derives shard via
/// `ShardRouter::shard_for(&from_account)` so the routing decision
/// is anchored to the same hash function the producer used (and
/// would compute on a fresh re-publish).
#[derive(serde::Deserialize)]
struct QueuePayload {
    from_account: String,
    to_account: String,
    amount: Decimal,
    currency: String,
    reference_id: Option<String>,
    description: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    shard: usize,
    #[serde(default)]
    request_id: String,
}

/// Per-batch ACK/NACK ledger returned by `flush_batch_to_shards`.
///
/// Both `successful_tags` and `failed_tags` are ACKed to the broker
/// (`failed_tags` represent business-state failures — insufficient
/// balance, missing recipient — that are durably persisted as
/// `'failed'` rows; redelivery would not change the outcome).
/// `requeue_tags` cover infrastructure errors that should be
/// re-delivered for retry.
#[derive(Default)]
struct FlushReport {
    successful: Vec<PendingMessage>,
    successful_tags: Vec<u64>,
    failed_tags: Vec<u64>,
    requeue_tags: Vec<u64>,
    /// Poison messages — non-retriable DB-side errors (CHECK
    /// violations, type overflow). NACKed with requeue=false so
    /// the broker routes them to the DLX.
    dlq_tags: Vec<u64>,
}

impl FlushReport {
    fn ack_count(&self) -> usize {
        self.successful_tags.len() + self.failed_tags.len()
    }
}

fn is_poison(err: &AppError) -> bool {
    let msg = err.to_string();
    msg.contains("violates check constraint")
        || msg.contains("numeric field overflow")
        || msg.contains("value too long")
        || msg.contains("invalid input syntax")
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
/// `flush_batch_to_shards` returns the rows are durably committed
/// in Postgres, which is exactly what `committed` means.
///
/// The `cancel` token enables coordinated graceful shutdown: the
/// flush-timer drains the buffer one last time, then the
/// connection holder waits for the timer to exit before closing
/// the AMQP connection (so the in-flight commit/ACK can finish).
pub async fn start_consumer(
    amqp_url: &str,
    shard_router: ShardRouter,
    events: Arc<dyn EventPublisher>,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, AppError> {
    let parts = shared_kernel::queue::producer::parse_amqp_url_full(amqp_url)?;

    let mut args = OpenConnectionArguments::new(&parts.host, parts.port, &parts.username, &parts.password);
    args.virtual_host(&parts.vhost);

    // Cap broker handshake at 10s so a slow/down RabbitMQ does not
    // wedge app startup indefinitely.
    let connection = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        Connection::open(&args),
    )
    .await
    .map_err(|_| AppError::Internal("Consumer RabbitMQ connection open timed out".into()))?
    .map_err(|e| AppError::Internal(format!("Consumer RabbitMQ connection error: {}", e)))?;

    connection
        .register_callback(DefaultConnectionCallback)
        .await
        .map_err(|e| AppError::Internal(format!("Consumer callback error: {}", e)))?;

    let channel = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        connection.open_channel(None),
    )
    .await
    .map_err(|_| AppError::Internal("Consumer open_channel timed out".into()))?
    .map_err(|e| AppError::Internal(format!("Consumer channel error: {}", e)))?;

    channel
        .register_callback(DefaultChannelCallback)
        .await
        .map_err(|e| AppError::Internal(format!("Consumer channel callback error: {}", e)))?;

    channel
        .basic_qos(BasicQosArguments::new(0, BATCH_SIZE as u16, false))
        .await
        .map_err(|e| AppError::Internal(format!("QOS error: {}", e)))?;

    let shared_channel = Arc::new(channel);

    let consumer = BatchTransactionConsumer {
        shard_router: shard_router.clone(),
        buffer: Arc::new(Mutex::new(Vec::with_capacity(BATCH_SIZE))),
        channel: shared_channel.clone(),
        events: events.clone(),
    };

    // Spawn flush timer — respects the cancellation token, drains
    // the buffer one last time on shutdown.
    let buffer_ref = consumer.buffer.clone();
    let router_ref = consumer.shard_router.clone();
    let timer_channel = shared_channel.clone();
    let timer_events = events.clone();
    let flush_cancel = cancel.clone();
    let timer_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(BATCH_FLUSH_MS)) => {
                    let batch = drain_buffer(&buffer_ref).await;
                    if !batch.is_empty() {
                        let report = flush_batch_to_shards(&batch, &router_ref).await;
                        apply_acks(&timer_channel, &report, "timer-flush").await;
                        record_batch_metrics(&report);
                        publish_committed_events(&report.successful, &timer_events);
                    }
                }
                _ = flush_cancel.cancelled() => {
                    tracing::info!("Consumer flush timer: cancellation received, draining buffer...");
                    let batch = drain_buffer(&buffer_ref).await;
                    if !batch.is_empty() {
                        let report = flush_batch_to_shards(&batch, &router_ref).await;
                        apply_acks(&timer_channel, &report, "shutdown-flush").await;
                        record_batch_metrics(&report);
                        publish_committed_events(&report.successful, &timer_events);
                        tracing::info!(
                            acked = report.ack_count(),
                            "Final buffer flushed"
                        );
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

    // Connection holder: on cancel, wait for the timer to drain
    // (which performs the final commit + ACK) BEFORE dropping the
    // connection. Closing the connection while a tx.commit is
    // in-flight would race the broker into thinking the message
    // is unacked and redeliver it — the in-tx idempotency SELECT
    // would catch the duplicate, but the redelivery is wasted
    // work and clutters the dashboard.
    let handle = tokio::spawn(async move {
        let _connection = connection;
        let _channel = shared_channel;
        cancel.cancelled().await;
        tracing::info!("Consumer connection holder: cancellation received, awaiting timer drain...");
        if let Err(e) = timer_handle.await {
            tracing::warn!(error = %e, "Timer task did not exit cleanly");
        }
        tracing::info!("Consumer connection holder: closing connection");
    });

    Ok(handle)
}

async fn drain_buffer(buffer: &Arc<Mutex<Vec<PendingMessage>>>) -> Vec<PendingMessage> {
    let mut buf = buffer.lock().await;
    buf.drain(..).collect()
}

/// Apply ACK/NACK for every tag in the report. Per-tag with
/// `multiple=false` so a failed shard's NACK does not corrupt
/// other shards' already-ACKed messages.
///
/// Empty `tags` are guarded — `basic_ack(0, multiple=true)` would
/// ACK every un-ACKed delivery on the channel, which under the
/// concurrent-flush race produces silent message loss.
async fn apply_acks(
    channel: &Arc<amqprs::channel::Channel>,
    report: &FlushReport,
    context: &'static str,
) {
    for tag in report.successful_tags.iter().chain(report.failed_tags.iter()) {
        if *tag == 0 {
            // Defensive: per AMQP spec, ACK with delivery_tag=0 +
            // multiple=true means "ACK everything". We never use
            // multiple=true now, but a stray 0 would still be a
            // protocol error worth catching.
            tracing::warn!(context, "skipping ACK with delivery_tag=0");
            continue;
        }
        if let Err(e) = channel
            .basic_ack(BasicAckArguments::new(*tag, false))
            .await
        {
            tracing::error!(error = %e, context, tag = *tag, "failed to ACK");
        }
    }
    for tag in &report.requeue_tags {
        if *tag == 0 {
            tracing::warn!(context, "skipping NACK with delivery_tag=0");
            continue;
        }
        if let Err(e) = channel
            .basic_nack(BasicNackArguments::new(*tag, false, true))
            .await
        {
            tracing::error!(error = %e, context, tag = *tag, "failed to NACK");
        }
    }
    // Poison: requeue=false → DLX route per queue declare args.
    for tag in &report.dlq_tags {
        if *tag == 0 {
            continue;
        }
        if let Err(e) = channel
            .basic_nack(BasicNackArguments::new(*tag, false, false))
            .await
        {
            tracing::error!(error = %e, context, tag = *tag, "failed to NACK to DLQ");
        }
    }
}

fn record_batch_metrics(report: &FlushReport) {
    let acked = report.ack_count();
    if acked > 0 {
        metrics::counter!("transactions_processed_total").increment(acked as u64);
        metrics::histogram!("transactions_batch_size").record(acked as f64);
    }
    if !report.failed_tags.is_empty() {
        metrics::counter!("transactions_failed_total")
            .increment(report.failed_tags.len() as u64);
    }
    if !report.requeue_tags.is_empty() {
        metrics::counter!("transactions_requeued_total")
            .increment(report.requeue_tags.len() as u64);
    }
}

struct BatchTransactionConsumer {
    shard_router: ShardRouter,
    buffer: Arc<Mutex<Vec<PendingMessage>>>,
    /// Shared channel reference for ACK from the consume callback.
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
                // requeue=false → routes to DLX configured at queue declare time.
                let _ = _channel
                    .basic_nack(BasicNackArguments::new(delivery_tag, false, false))
                    .await;
                return;
            }
        };

        // Defense-in-depth: the producer always supplies a
        // reference_id (UUID fallback). A NULL on the wire would
        // bypass the `ON CONFLICT (reference_id)` dedupe in the
        // bulk INSERT — multiple `NULL` refs are allowed by the
        // unique constraint — so reject explicitly to DLQ.
        if payload.reference_id.is_none() {
            tracing::error!("Message missing reference_id, NACKing to DLQ");
            metrics::counter!("dlq_messages_total").increment(1);
            let _ = _channel
                .basic_nack(BasicNackArguments::new(delivery_tag, false, false))
                .await;
            return;
        }

        // Re-derive shard locally — the wire field is advisory only.
        let shard = ShardRouter::shard_for(&payload.from_account);
        let request_id = payload.request_id;
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
                request_id,
            });
            should_flush = buf.len() >= BATCH_SIZE;
        }

        if should_flush {
            let batch = drain_buffer(&self.buffer).await;
            // Empty drain is possible: the timer can race in
            // between the lock release above and this drain.
            // Guard against the empty-batch ACK pitfall.
            if batch.is_empty() {
                return;
            }
            let report = flush_batch_to_shards(&batch, &self.shard_router).await;
            apply_acks(&self.channel, &report, "size-flush").await;
            record_batch_metrics(&report);
            publish_committed_events(&report.successful, &self.events);
        }
        // No individual ACK here — messages stay unacknowledged
        // until the batch is flushed (either by size threshold above
        // or by the timer task). QoS prefetch limits how many un-ACK'd
        // messages RabbitMQ will deliver.
    }
}

/// Group `batch` by sender shard, run each shard's batch in
/// parallel, and aggregate ACK/NACK decisions into a `FlushReport`.
///
/// Per-shard task either commits the whole shard's messages
/// atomically (`successful_tags` + `failed_tags`) or aborts the
/// tx on infra error (`requeue_tags`). Because each shard's tx
/// is independent the failure of one shard never affects the
/// other shards' ACKs.
async fn flush_batch_to_shards(
    batch: &[PendingMessage],
    router: &ShardRouter,
) -> FlushReport {
    let mut report = FlushReport::default();
    if batch.is_empty() {
        return report;
    }

    let mut shard_groups: std::collections::HashMap<usize, Vec<PendingMessage>> =
        std::collections::HashMap::new();
    for msg in batch {
        shard_groups
            .entry(msg.shard)
            .or_default()
            .push(msg.clone());
    }

    let mut handles: Vec<(Vec<PendingMessage>, JoinHandle<Result<ShardOutcome, AppError>>)> =
        Vec::with_capacity(shard_groups.len());

    for (sender_shard, messages) in shard_groups {
        let pool = router.writer(sender_shard).clone();
        let router = router.clone();
        let owned = messages.clone();
        let handle = tokio::spawn(async move {
            process_shard_batch(&pool, sender_shard, &owned, &router).await
        });
        handles.push((messages, handle));
    }

    for (messages, handle) in handles {
        match handle.await {
            Ok(Ok(outcome)) => {
                let ShardOutcome { completed_tags, failed_tags } = outcome;
                report.failed_tags.extend(failed_tags);
                // Reconstruct successful PendingMessages for event emission.
                for msg in &messages {
                    if completed_tags.contains(&msg.delivery_tag) {
                        report.successful.push(msg.clone());
                    }
                }
                report.successful_tags.extend(completed_tags);
            }
            Ok(Err(e)) => {
                if is_poison(&e) {
                    tracing::error!(error = %e, "shard batch failed (poison), routing to DLQ");
                    metrics::counter!("dlq_messages_total")
                        .increment(messages.len() as u64);
                    report.dlq_tags.extend(messages.iter().map(|m| m.delivery_tag));
                } else {
                    tracing::error!(error = %e, "shard batch failed, requeuing");
                    report.requeue_tags.extend(messages.iter().map(|m| m.delivery_tag));
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "shard task join error, requeuing");
                report
                    .requeue_tags
                    .extend(messages.iter().map(|m| m.delivery_tag));
            }
        }
    }

    report
}

/// Per-shard outcome after `process_shard_batch` commits.
struct ShardOutcome {
    /// Delivery tags whose `transactions` row was inserted as
    /// `'completed'` (sender debited, recipient credited).
    completed_tags: Vec<u64>,
    /// Delivery tags whose `transactions` row was inserted as
    /// `'failed'` (insufficient balance, missing recipient,
    /// idempotent skip). Both are durably persisted; both ACK.
    failed_tags: Vec<u64>,
}

/// Tx structure (atomic on `pool`):
///   1. SELECT existing reference_ids on this shard → idempotency dedupe set.
///   2. For each message NOT already processed:
///      a. UPDATE balance debit (atomic check-and-decrement).
///      b. If debit matched zero rows: mark message 'failed', continue.
///      c. If receiver lives on this shard: UPDATE balance credit. If
///         credit matched zero rows (recipient missing): refund the
///         sender atomically within this tx, mark message 'failed'.
///      d. Else: queue for post-commit cross-shard credit.
///   3. Bulk INSERT all 'failed' rows (with `processed_at = NOW()`).
///   4. Bulk INSERT all 'completed' rows (with `processed_at = NOW()`).
///   5. tx.commit().
///   6. Cross-shard credits, parallelised, post-commit, best-effort.
///
/// On any DB error before commit the whole batch returns Err and
/// the caller NACKs with requeue=true. Idempotency dedupe ensures
/// redelivery is a no-op on already-processed messages.
async fn process_shard_batch(
    pool: &sqlx::PgPool,
    sender_shard: usize,
    messages: &[PendingMessage],
    router: &ShardRouter,
) -> Result<ShardOutcome, AppError> {
    let mut completed_tags: Vec<u64> = Vec::with_capacity(messages.len());
    let mut failed_tags: Vec<u64> = Vec::new();

    if messages.is_empty() {
        return Ok(ShardOutcome {
            completed_tags,
            failed_tags,
        });
    }

    let mut tx = pool.begin().await?;

    // Dedupe key is (reference_id, from_account) — composite unique
    // matches the schema constraint. Two distinct from_accounts can
    // legitimately reuse a ref_id and both must process.
    let pairs: Vec<(String, String)> = messages
        .iter()
        .filter_map(|m| {
            m.request
                .reference_id
                .as_ref()
                .map(|r| (r.clone(), m.request.from_account.clone()))
        })
        .collect();
    let already_processed: std::collections::HashSet<(String, String)> = if pairs.is_empty() {
        std::collections::HashSet::new()
    } else {
        let ref_ids: Vec<String> = pairs.iter().map(|(r, _)| r.clone()).collect();
        let from_accs: Vec<String> = pairs.iter().map(|(_, f)| f.clone()).collect();
        sqlx::query_as::<_, (String, String)>(
            "SELECT reference_id, from_account FROM transactions \
             WHERE (reference_id, from_account) IN ( \
                 SELECT * FROM UNNEST($1::text[], $2::text[]) \
             )",
        )
        .bind(&ref_ids)
        .bind(&from_accs)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .collect()
    };

    let mut completed_msgs: Vec<&PendingMessage> = Vec::with_capacity(messages.len());
    let mut failed_msgs: Vec<&PendingMessage> = Vec::new();
    // (msg, receiver_shard) — outbox rows queued for cross-shard credit.
    let mut cross_shard_outbox: Vec<(&PendingMessage, usize)> = Vec::new();

    for msg in messages {
        if let Some(ref_id) = msg.request.reference_id.as_deref() {
            let key = (ref_id.to_string(), msg.request.from_account.clone());
            if already_processed.contains(&key) {
                tracing::debug!(
                    reference_id = ref_id,
                    from_account = %msg.request.from_account,
                    "Skipping already-processed message (idempotent redelivery)"
                );
                failed_tags.push(msg.delivery_tag);
                continue;
            }
        }

        // Debit sender: atomic check-and-decrement against an
        // active row. The `status = 'active'` predicate is what
        // closes the bug where the API layer's `verify_from_account`
        // is OFF and an inactive/blocked sender's tx would otherwise
        // be silently debited.
        let updated = sqlx::query(
            "UPDATE users SET balance = balance - $1 \
             WHERE account_number = $2 AND balance >= $1 AND status = 'active'",
        )
        .bind(msg.request.amount)
        .bind(&msg.request.from_account)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            // Insufficient balance OR sender missing — both surface
            // here as a no-op debit. Persist as 'failed'.
            failed_msgs.push(msg);
            continue;
        }

        // Credit recipient. Filter on `status = 'active'` so a
        // blocked/inactive recipient is treated the same as a
        // missing one — refund the sender and mark failed rather
        // than crediting funds to an account that can't transact.
        let receiver_shard = ShardRouter::shard_for(&msg.request.to_account);
        if receiver_shard == sender_shard {
            let credited = sqlx::query(
                "UPDATE users SET balance = balance + $1 \
                 WHERE account_number = $2 AND status = 'active'",
            )
            .bind(msg.request.amount)
            .bind(&msg.request.to_account)
            .execute(&mut *tx)
            .await?;

            if credited.rows_affected() == 0 {
                // Recipient missing or not active on this shard.
                // Compensate the sender's debit atomically within
                // the same tx so money is never lost. Mark
                // message 'failed'.
                tracing::warn!(
                    from = %msg.request.from_account,
                    to = %msg.request.to_account,
                    "same-shard credit hit no row, refunding sender"
                );
                metrics::counter!("same_shard_credit_missing_total").increment(1);
                sqlx::query(
                    "UPDATE users SET balance = balance + $1 WHERE account_number = $2",
                )
                .bind(msg.request.amount)
                .bind(&msg.request.from_account)
                .execute(&mut *tx)
                .await?;
                failed_msgs.push(msg);
                continue;
            }
        } else {
            // Cross-shard credit deferred to outbox row written
            // INSIDE this tx — atomic with the debit.
            cross_shard_outbox.push((msg, receiver_shard));
        }

        completed_msgs.push(msg);
    }

    // Insert outbox rows in same tx as debit.
    if !cross_shard_outbox.is_empty() {
        bulk_insert_outbox(&mut tx, &cross_shard_outbox).await?;
    }

    // ─── Bulk INSERT failed rows (in tx) ────────────────────
    if !failed_msgs.is_empty() {
        bulk_insert_rows(&mut tx, &failed_msgs, "failed").await?;
        for msg in &failed_msgs {
            failed_tags.push(msg.delivery_tag);
        }
    }

    // ─── Bulk INSERT completed rows (in tx) ─────────────────
    if !completed_msgs.is_empty() {
        bulk_insert_rows(&mut tx, &completed_msgs, "completed").await?;
        for msg in &completed_msgs {
            completed_tags.push(msg.delivery_tag);
        }
    }

    tx.commit().await?;

    // Cross-shard credits are now in the outbox table. The
    // outbox processor (`cross_shard_processor.rs`) drains them.
    let _ = router;

    Ok(ShardOutcome {
        completed_tags,
        failed_tags,
    })
}

async fn bulk_insert_outbox(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[(&PendingMessage, usize)],
) -> Result<(), AppError> {
    let n = rows.len();
    let mut ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut from_accounts: Vec<String> = Vec::with_capacity(n);
    let mut to_accounts: Vec<String> = Vec::with_capacity(n);
    let mut to_shards: Vec<i32> = Vec::with_capacity(n);
    let mut amounts: Vec<Decimal> = Vec::with_capacity(n);
    let mut currencies: Vec<String> = Vec::with_capacity(n);
    let mut reference_ids: Vec<String> = Vec::with_capacity(n);
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(n);
    for (msg, to_shard) in rows {
        ids.push(Uuid::new_v4());
        from_accounts.push(msg.request.from_account.clone());
        to_accounts.push(msg.request.to_account.clone());
        to_shards.push(*to_shard as i32);
        amounts.push(msg.request.amount);
        currencies.push(msg.request.currency.clone());
        // Producer guarantees Some(ref) — null-rejected at the
        // wire level. unwrap_or for defense.
        reference_ids.push(msg.request.reference_id.clone().unwrap_or_default());
        descriptions.push(msg.request.description.clone());
    }
    sqlx::query(
        r#"INSERT INTO cross_shard_outbox
           (id, from_account, to_account, to_shard, amount, currency,
            reference_id, description, status)
           SELECT * FROM UNNEST(
               $1::uuid[], $2::text[], $3::text[], $4::int[],
               $5::numeric[], $6::text[], $7::text[], $8::text[],
               ARRAY_FILL('pending'::text, ARRAY[$9::int])
           )"#,
    )
    .bind(&ids)
    .bind(&from_accounts)
    .bind(&to_accounts)
    .bind(&to_shards)
    .bind(&amounts)
    .bind(&currencies)
    .bind(&reference_ids)
    .bind(&descriptions)
    .bind(n as i32)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Bulk INSERT a vec of `PendingMessage`s with the given status.
/// Sets `processed_at = NOW()` for all rows (including 'failed' —
/// terminal state, the timestamp records when the decision was made).
/// `ON CONFLICT (reference_id) DO NOTHING` makes the insert
/// idempotent against the in-tx dedupe SELECT plus broker
/// redelivery between batches.
async fn bulk_insert_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    msgs: &[&PendingMessage],
    status: &'static str,
) -> Result<(), AppError> {
    let n = msgs.len();
    let mut ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut from_accounts: Vec<String> = Vec::with_capacity(n);
    let mut to_accounts: Vec<String> = Vec::with_capacity(n);
    let mut amounts: Vec<Decimal> = Vec::with_capacity(n);
    let mut currencies: Vec<String> = Vec::with_capacity(n);
    let mut reference_ids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(n);

    for msg in msgs {
        ids.push(Uuid::new_v4());
        from_accounts.push(msg.request.from_account.clone());
        to_accounts.push(msg.request.to_account.clone());
        amounts.push(msg.request.amount);
        currencies.push(msg.request.currency.clone());
        reference_ids.push(msg.request.reference_id.clone());
        descriptions.push(msg.request.description.clone());
    }

    sqlx::query(
        r#"INSERT INTO transactions
           (id, from_account, to_account, amount, currency, status, reference_id, description, processed_at)
           SELECT * FROM UNNEST(
               $1::uuid[], $2::text[], $3::text[], $4::numeric[], $5::text[],
               ARRAY_FILL($8::text, ARRAY[$9::int]),
               $6::text[], $7::text[],
               ARRAY_FILL(NOW()::timestamptz, ARRAY[$9::int])
           )
           ON CONFLICT (reference_id, from_account) DO NOTHING"#,
    )
    .bind(&ids)
    .bind(&from_accounts)
    .bind(&to_accounts)
    .bind(&amounts)
    .bind(&currencies)
    .bind(&reference_ids)
    .bind(&descriptions)
    .bind(status)
    .bind(n as i32)
    .execute(&mut **tx)
    .await?;

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

/// Publish one `transactions.committed` event per row in
/// `successful`. Best-effort — failures here are logged + counted
/// but never propagate, because the row is already durably
/// committed and nothing about the queue ACK should depend on a
/// notification channel being healthy.
fn publish_committed_events(successful: &[PendingMessage], events: &Arc<dyn EventPublisher>) {
    for msg in successful {
        let payload = TransactionCommittedPayload {
            from_account: &msg.request.from_account,
            to_account: &msg.request.to_account,
            amount: msg.request.amount.to_string(),
            currency: &msg.request.currency,
            reference_id: msg.request.reference_id.as_deref(),
            shard: msg.shard,
        };
        let event = match Event::new(EVENT_TRANSACTIONS_COMMITTED, &payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    request_id = %msg.request_id,
                    "Failed to build TransactionCommitted event"
                );
                metrics::counter!("events_build_errors_total").increment(1);
                continue;
            }
        };
        if let Err(e) = events.publish(event) {
            tracing::warn!(
                error = %e,
                request_id = %msg.request_id,
                "EventBus publish failed"
            );
            metrics::counter!("events_publish_errors_total").increment(1);
        } else {
            metrics::counter!("events_published_total", "name" => EVENT_TRANSACTIONS_COMMITTED)
                .increment(1);
        }
    }
}
