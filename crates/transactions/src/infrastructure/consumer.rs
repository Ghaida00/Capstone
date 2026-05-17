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
    callbacks::DefaultConnectionCallback,
    channel::{BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicQosArguments},
    connection::{Connection, OpenConnectionArguments},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use shared_kernel::queue::callback::ConsumerChannelCallback;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use shared_kernel::db::shard::ShardRouter;
use shared_kernel::error::AppError;
use shared_kernel::events::{Event, EventPublisher, EVENT_TRANSACTIONS_COMMITTED};

const QUEUE_NAME: &str = "transactions.process";
const CONSUMER_TAG: &str = "peakload-consumer";
const BATCH_SIZE: usize = 100;
/// Maximum time a partial batch waits in the buffer before being
/// flushed to PG. Caps the average status-reflection latency at
/// `BATCH_FLUSH_MS / 2` for any single message that doesn't fill
/// a full batch on its own. The value is the dominant tunable for
/// the 202-to-`completed` window.
const BATCH_FLUSH_MS: u64 = 50;

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
    /// Request id propagated from the producer. Surfaced as a
    /// field on event-publish warn lines for cross-process trace
    /// correlation. Empty when the producer didn't supply one.
    request_id: String,
    /// Server-assigned `transactions.id` for this row. Filled in
    /// by `bulk_claim_slots` when the row is first claimed in
    /// this batch. Used to build the `transactions.committed`
    /// event payload so subscribers (cache invalidator) can DEL
    /// the per-id cache key. `None` for messages that lost the
    /// claim race (idempotent redeliveries) — those never emit
    /// an event.
    id: Option<Uuid>,
}

/// Message payload from queue.
///
/// Consumer re-derives shard via `ShardRouter::shard_for(&from_account)`
/// so routing is anchored to the same hash function the producer used.
/// Serde tolerates extra fields, so old producers that still send
/// `shard` continue to deserialize fine.
#[derive(serde::Deserialize)]
struct QueuePayload {
    from_account: String,
    to_account: String,
    amount: Decimal,
    currency: String,
    reference_id: Option<String>,
    description: Option<String>,
    #[serde(default)]
    request_id: String,
    /// W3C `traceparent` also rides the JSON payload (it crosses the
    /// storage hop there). The active parent context is reconstructed
    /// from the AMQP headers in `consume`, so this field is accepted
    /// for wire-shape completeness but not read directly.
    #[serde(default)]
    #[allow(dead_code)]
    traceparent: Option<String>,
}

/// Per-batch ACK/NACK ledger returned by `flush_batch_to_shards`.
///
/// `successful_tags`, `failed_tags`, and `skipped_tags` are all
/// ACKed to the broker:
///
/// * `successful_tags` — durable `'completed'` row in transactions.
/// * `failed_tags` — durable `'failed'` row (business-state failure:
///   insufficient balance, missing recipient on the same shard).
/// * `skipped_tags` — idempotent redelivery, the row already existed
///   from a prior batch's commit. No DB write happened in this batch;
///   counting these as "failed" pollutes the dashboards.
///
/// `requeue_tags` cover infrastructure errors that should be
/// re-delivered for retry.
#[derive(Default)]
struct FlushReport {
    successful: Vec<PendingMessage>,
    successful_tags: Vec<u64>,
    failed_tags: Vec<u64>,
    skipped_tags: Vec<u64>,
    requeue_tags: Vec<u64>,
    /// Poison messages — non-retriable DB-side errors (CHECK
    /// violations, type overflow). NACKed with requeue=false so
    /// the broker routes them to the DLX. Populated only for
    /// messages that individually failed during the per-message
    /// poison-fallback path, NOT for whole-batch poison errors.
    dlq_tags: Vec<u64>,
}

impl FlushReport {
    fn ack_count(&self) -> usize {
        self.successful_tags.len() + self.failed_tags.len() + self.skipped_tags.len()
    }
}

/// Distinguish non-retriable DB errors (route to DLQ) from
/// transient infra errors (requeue).
///
/// Matches on SQLSTATE codes — stable across Postgres versions
/// and independent of `LC_MESSAGES`. Codes covered here:
///   * `23514` — check_violation (e.g. amount > 0 violated)
///   * `22003` — numeric_value_out_of_range (DECIMAL overflow)
///   * `22001` — string_data_right_truncation (VARCHAR too long)
///   * `22P02` — invalid_text_representation
///   * `23502` — not_null_violation
///   * `22008` — datetime_field_overflow
///   * `23505` — unique_violation (e.g. intra-batch duplicate of
///     `(reference_id, from_account)`)
///   * `21000` — cardinality_violation (`INSERT ... ON CONFLICT
///     DO NOTHING` over UNNEST hitting the same conflict key
///     twice in one statement)
fn is_poison(err: &AppError) -> bool {
    if let AppError::Database(db_err) = err {
        if let Some(pg_err) = db_err.as_database_error() {
            if let Some(code) = pg_err.code() {
                return matches!(
                    code.as_ref(),
                    "23514" | "22003" | "22001" | "22P02" | "23502" | "22008" | "23505" | "21000"
                );
            }
        }
    }
    false
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

    let mut args =
        OpenConnectionArguments::new(&parts.host, parts.port, &parts.username, &parts.password);
    args.virtual_host(&parts.vhost);

    // Cap broker handshake at 10s so a slow/down RabbitMQ does not
    // wedge app startup indefinitely.
    let connection =
        tokio::time::timeout(std::time::Duration::from_secs(10), Connection::open(&args))
            .await
            .map_err(|_| AppError::Internal("Consumer RabbitMQ connection open timed out".into()))?
            .map_err(|e| {
                AppError::Internal(format!("Consumer RabbitMQ connection error: {}", e))
            })?;

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
        .register_callback(ConsumerChannelCallback::new(cancel.clone()))
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
        tracing::info!(
            "Consumer connection holder: cancellation received, awaiting timer drain..."
        );
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
    for tag in report
        .successful_tags
        .iter()
        .chain(report.failed_tags.iter())
        .chain(report.skipped_tags.iter())
    {
        if *tag == 0 {
            // Defensive: per AMQP spec, ACK with delivery_tag=0 +
            // multiple=true means "ACK everything". We never use
            // multiple=true now, but a stray 0 would still be a
            // protocol error worth catching.
            tracing::warn!(context, "skipping ACK with delivery_tag=0");
            continue;
        }
        if let Err(e) = channel.basic_ack(BasicAckArguments::new(*tag, false)).await {
            tracing::error!(error = %e, context, tag = *tag, "failed to ACK");
            metrics::counter!("amqp_ack_failures_total", "kind" => "ack").increment(1);
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
            metrics::counter!("amqp_ack_failures_total", "kind" => "nack_requeue").increment(1);
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
            metrics::counter!("amqp_ack_failures_total", "kind" => "nack_dlq").increment(1);
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
        metrics::counter!("transactions_failed_total").increment(report.failed_tags.len() as u64);
    }
    if !report.skipped_tags.is_empty() {
        // Idempotent redeliveries — separate counter so the
        // failure dashboard isn't polluted by broker retries.
        metrics::counter!("transactions_idempotent_skips_total")
            .increment(report.skipped_tags.len() as u64);
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
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let delivery_tag = deliver.delivery_tag();

        // Reconstruct the originating HTTP request's trace context
        // from the AMQP headers (set by `publish_traced`) so this
        // span is parented under it — one trace spans HTTP -> storage
        // -> worker -> AMQP -> consumer. Absent header => a fresh
        // (root) context, preserving prior behaviour.
        let parent_cx = basic_properties
            .headers()
            .map(shared_kernel::queue::trace_propagation::extract_parent_context)
            .unwrap_or_default();
        let span = tracing::info_span!(
            "amqp.consume",
            messaging.system = "rabbitmq",
            messaging.destination = %deliver.routing_key(),
            request_id = tracing::field::Empty,
        );
        let _ = span.set_parent(parent_cx);
        let _span_guard = span.enter();

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

        tracing::Span::current().record("request_id", payload.request_id.as_str());

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
        // Bound to this consumer's own router so a config change
        // elsewhere can't re-route mid-flight.
        let shard = self.shard_router.shard_for_account(&payload.from_account);
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
                id: None,
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
            // Spawn the flush so this `consume()` returns quickly
            // and amqprs can dispatch the next delivery. The
            // AsyncConsumer trait serializes calls per consumer,
            // so awaiting flush_batch_to_shards + apply_acks here
            // would wedge the AMQP read loop for the entire
            // DB-write + ACK round-trip. Prefetch caps in-flight
            // flushes — broker won't deliver past `BATCH_SIZE`
            // unacked messages until apply_acks runs.
            let router = self.shard_router.clone();
            let channel = self.channel.clone();
            let events = self.events.clone();
            tokio::spawn(async move {
                let report = flush_batch_to_shards(&batch, &router).await;
                apply_acks(&channel, &report, "size-flush").await;
                record_batch_metrics(&report);
                publish_committed_events(&report.successful, &events);
            });
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
async fn flush_batch_to_shards(batch: &[PendingMessage], router: &ShardRouter) -> FlushReport {
    let mut report = FlushReport::default();
    if batch.is_empty() {
        return report;
    }

    let mut shard_groups: std::collections::HashMap<usize, Vec<PendingMessage>> =
        std::collections::HashMap::new();
    for msg in batch {
        shard_groups.entry(msg.shard).or_default().push(msg.clone());
    }

    type ShardHandle = (
        usize,
        Vec<PendingMessage>,
        JoinHandle<Result<ShardOutcome, AppError>>,
    );
    let mut handles: Vec<ShardHandle> = Vec::with_capacity(shard_groups.len());

    for (sender_shard, messages) in shard_groups {
        let pool = router.writer(sender_shard).clone();
        let router_cl = router.clone();
        let owned = messages.clone();
        let handle = tokio::spawn(async move {
            process_shard_batch(&pool, sender_shard, &owned, &router_cl).await
        });
        handles.push((sender_shard, messages, handle));
    }

    for (sender_shard, messages, handle) in handles {
        match handle.await {
            Ok(Ok(outcome)) => {
                let ShardOutcome {
                    completed,
                    failed_tags,
                    skipped_tags,
                } = outcome;
                report
                    .successful_tags
                    .extend(completed.iter().map(|m| m.delivery_tag));
                report.successful.extend(completed);
                report.failed_tags.extend(failed_tags);
                report.skipped_tags.extend(skipped_tags);
            }
            Ok(Err(e)) if is_poison(&e) => {
                tracing::error!(
                    error = %e,
                    shard = sender_shard,
                    batch_size = messages.len(),
                    "shard batch failed (poison) — falling back to per-message retry"
                );
                metrics::counter!("transactions_poison_fallback_total").increment(1);
                let pool = router.writer(sender_shard).clone();
                let split =
                    process_messages_individually(&pool, sender_shard, &messages, router).await;
                report
                    .successful_tags
                    .extend(split.completed.iter().map(|m| m.delivery_tag));
                report.successful.extend(split.completed);
                report.failed_tags.extend(split.failed_tags);
                report.skipped_tags.extend(split.skipped_tags);
                if !split.dlq_tags.is_empty() {
                    metrics::counter!("dlq_messages_total").increment(split.dlq_tags.len() as u64);
                }
                report.dlq_tags.extend(split.dlq_tags);
                report.requeue_tags.extend(split.requeue_tags);
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, shard = sender_shard, "shard batch failed, requeuing");
                report
                    .requeue_tags
                    .extend(messages.iter().map(|m| m.delivery_tag));
            }
            Err(e) => {
                tracing::error!(error = %e, shard = sender_shard, "shard task join error, requeuing");
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
    /// PendingMessages whose `transactions` row was inserted as
    /// `'completed'` (sender debited, recipient credited or
    /// cross-shard outbox queued). Carries the assigned id so
    /// the caller can build the committed event payload.
    completed: Vec<PendingMessage>,
    /// Delivery tags whose `transactions` row was inserted as
    /// `'failed'` (insufficient balance, missing recipient on
    /// the same shard).
    failed_tags: Vec<u64>,
    /// Delivery tags whose `(reference_id, from_account)` slot
    /// was already taken by a prior batch's commit — i.e.
    /// idempotent redelivery. ACKed but no DB write happened
    /// in this batch.
    skipped_tags: Vec<u64>,
}

/// Tx structure (atomic on `pool`):
///
/// 1. **Claim slots** — bulk INSERT a placeholder row (status='pending',
///    processed_at=NOW()) for every message, `ON CONFLICT (reference_id,
///    from_account) DO NOTHING RETURNING id, reference_id, from_account`.
///    Messages whose key was already inserted by a prior batch (or a
///    concurrent batch racing this one) lose the claim and are reported
///    as `skipped` — they ARE NOT debited again.
/// 2. For each message that won the claim:
///    a. UPDATE balance debit (atomic check-and-decrement).
///    b. If debit matched zero rows: mark message 'failed', continue.
///    c. If receiver lives on this shard: UPDATE balance credit. If
///    credit matched zero rows (recipient missing): refund the
///    sender atomically within this tx, mark message 'failed'.
///    d. Else: queue an outbox row for the cross-shard processor.
/// 3. Bulk INSERT outbox rows (in tx).
/// 4. Bulk UPDATE the placeholder transactions rows to terminal
///    ('completed' or 'failed') status.
/// 5. tx.commit().
///
/// Why insert-first rather than the previous SELECT-then-UPDATE:
/// the dedupe SELECT runs at READ COMMITTED snapshot time, so two
/// concurrent batches racing on the same `(reference_id, from_account)`
/// could both see "no row exists", both proceed to debit, and only
/// one INSERT survive `ON CONFLICT DO NOTHING`. The losing batch's
/// debit became silent money loss. INSERT-first turns the
/// `(reference_id, from_account)` UNIQUE constraint into the
/// serialisation point: only the writer that successfully inserts
/// the placeholder owns the slot for the rest of the tx.
///
/// On any DB error before commit the whole batch returns Err. The
/// caller decides per-error class whether to requeue or fall back
/// to a per-message retry that isolates the bad row.
async fn process_shard_batch(
    pool: &sqlx::PgPool,
    sender_shard: usize,
    messages: &[PendingMessage],
    router: &ShardRouter,
) -> Result<ShardOutcome, AppError> {
    let mut completed: Vec<PendingMessage> = Vec::with_capacity(messages.len());
    let mut failed_tags: Vec<u64> = Vec::new();
    let mut skipped_tags: Vec<u64> = Vec::new();

    if messages.is_empty() {
        return Ok(ShardOutcome {
            completed,
            failed_tags,
            skipped_tags,
        });
    }

    let mut tx = pool.begin().await?;

    // Claim the (reference_id, from_account) slot up front. The
    // returned map covers ONLY the messages whose row was actually
    // inserted; everything else lost the claim race and is an
    // idempotent skip.
    let claimed: std::collections::HashMap<(String, String), Uuid> =
        bulk_claim_slots(&mut tx, messages).await?;

    let mut completed_msgs: Vec<PendingMessage> = Vec::with_capacity(messages.len());
    let mut failed_msgs: Vec<PendingMessage> = Vec::new();
    // Cross-shard rows: sender-side audit row stays at 'processing'
    // until `cross_shard_processor` confirms the credit landed on
    // the receiver. Marking these 'completed' at sender commit time
    // would surface a "completed" status to clients during the
    // 250 ms+ window before the cross-shard apply runs, even though
    // the recipient has not been credited yet.
    let mut processing_msgs: Vec<PendingMessage> = Vec::new();
    // (msg, receiver_shard) — outbox rows queued for cross-shard credit.
    let mut cross_shard_outbox: Vec<(PendingMessage, usize)> = Vec::new();
    // Intra-batch dedupe: ON CONFLICT DO NOTHING returns one
    // placeholder per key, so without this two messages sharing
    // (ref_id, from) would both look up the same id and double-debit.
    let mut consumed: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();

    for msg in messages {
        let ref_id = match msg.request.reference_id.as_deref() {
            Some(r) => r.to_string(),
            // The `consume` callback rejects messages with a NULL
            // reference_id to DLQ before they ever land in the
            // buffer; defense-in-depth — skip this row.
            None => {
                skipped_tags.push(msg.delivery_tag);
                continue;
            }
        };
        let key = (ref_id, msg.request.from_account.clone());
        let assigned_id = match claimed.get(&key) {
            Some(id) => *id,
            None => {
                tracing::debug!(
                    reference_id = %key.0,
                    from_account = %key.1,
                    "Skipping already-claimed message (idempotent redelivery)"
                );
                skipped_tags.push(msg.delivery_tag);
                continue;
            }
        };
        if !consumed.insert(key.clone()) {
            tracing::debug!(
                reference_id = %key.0,
                from_account = %key.1,
                "Skipping intra-batch duplicate"
            );
            metrics::counter!("transactions_intra_batch_dupes_total").increment(1);
            skipped_tags.push(msg.delivery_tag);
            continue;
        }

        let mut owned = msg.clone();
        owned.id = Some(assigned_id);

        // Debit sender: atomic check-and-decrement against an
        // active row. The `status = 'active'` predicate is what
        // closes the bug where the API layer's `verify_from_account`
        // is OFF and an inactive/blocked sender's tx would otherwise
        // be silently debited.
        let updated = sqlx::query(
            "UPDATE users SET balance = balance - $1 \
             WHERE account_number = $2 AND balance >= $1 AND status = 'active'",
        )
        .bind(owned.request.amount)
        .bind(&owned.request.from_account)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            // Insufficient balance OR sender missing — both surface
            // here as a no-op debit. Persist as 'failed'.
            failed_msgs.push(owned);
            continue;
        }

        // Credit recipient. Filter on `status = 'active'` so a
        // blocked/inactive recipient is treated the same as a
        // missing one — refund the sender and mark failed rather
        // than crediting funds to an account that can't transact.
        let receiver_shard = router.shard_for_account(&owned.request.to_account);
        if receiver_shard == sender_shard {
            let credited = sqlx::query(
                "UPDATE users SET balance = balance + $1 \
                 WHERE account_number = $2 AND status = 'active'",
            )
            .bind(owned.request.amount)
            .bind(&owned.request.to_account)
            .execute(&mut *tx)
            .await?;

            if credited.rows_affected() == 0 {
                // Recipient missing or not active on this shard.
                // Compensate the sender's debit atomically within
                // the same tx so money is never lost. Mark
                // message 'failed'.
                tracing::warn!(
                    from = %owned.request.from_account,
                    to = %owned.request.to_account,
                    "same-shard credit hit no row, refunding sender"
                );
                metrics::counter!("same_shard_credit_missing_total").increment(1);
                sqlx::query("UPDATE users SET balance = balance + $1 WHERE account_number = $2")
                    .bind(owned.request.amount)
                    .bind(&owned.request.from_account)
                    .execute(&mut *tx)
                    .await?;
                failed_msgs.push(owned);
                continue;
            }
            completed_msgs.push(owned);
        } else {
            // Cross-shard credit deferred to outbox row written
            // INSIDE this tx — atomic with the debit. The sender
            // audit row commits as 'processing' and is flipped to
            // 'completed' by `cross_shard_processor` only after the
            // receiver shard's credit lands.
            cross_shard_outbox.push((owned.clone(), receiver_shard));
            processing_msgs.push(owned);
        }
    }

    // Insert outbox rows in same tx as debit.
    if !cross_shard_outbox.is_empty() {
        let outbox_refs: Vec<(&PendingMessage, usize)> =
            cross_shard_outbox.iter().map(|(m, s)| (m, *s)).collect();
        bulk_insert_outbox(&mut tx, &outbox_refs).await?;
    }

    // ─── Promote placeholder rows from 'pending' ────────────
    // Same-shard outcomes are terminal here ('completed' /
    // 'failed'). Cross-shard rows go to the non-terminal
    // 'processing' state — `cross_shard_processor` flips them to
    // 'completed' once the receiver-shard credit lands, or to
    // 'reversed' on the recipient-missing refund path.
    if !failed_msgs.is_empty() {
        bulk_update_status(&mut tx, &failed_msgs, "failed").await?;
        for msg in &failed_msgs {
            failed_tags.push(msg.delivery_tag);
        }
    }

    if !completed_msgs.is_empty() {
        bulk_update_status(&mut tx, &completed_msgs, "completed").await?;
        completed.extend(completed_msgs);
    }

    if !processing_msgs.is_empty() {
        bulk_update_status(&mut tx, &processing_msgs, "processing").await?;
        // Both same-shard 'completed' and cross-shard 'processing'
        // rows are durably committed at this point. Publish the
        // `transactions.committed` event for both so the cache
        // invalidator DELs the per-id / per-status keys; clients
        // who poll status during the cross-shard window then read
        // the truthful 'processing' state from the DB instead of a
        // stale cached value.
        completed.extend(processing_msgs);
    }

    tx.commit().await?;

    Ok(ShardOutcome {
        completed,
        failed_tags,
        skipped_tags,
    })
}

/// Per-message poison-fallback path. Invoked when a whole batch's
/// commit returned a poison-class error (CHECK violation, type
/// overflow, etc.) — instead of routing all 50 to the DLQ as the
/// previous code did, retry each message in its own transaction so
/// the bad row is isolated and the rest land in the DB cleanly.
async fn process_messages_individually(
    pool: &sqlx::PgPool,
    sender_shard: usize,
    messages: &[PendingMessage],
    router: &ShardRouter,
) -> ShardSplit {
    let mut split = ShardSplit::default();
    for msg in messages {
        let single = std::slice::from_ref(msg).to_vec();
        match process_shard_batch(pool, sender_shard, &single, router).await {
            Ok(out) => {
                split.completed.extend(out.completed);
                split.failed_tags.extend(out.failed_tags);
                split.skipped_tags.extend(out.skipped_tags);
            }
            Err(e) if is_poison(&e) => {
                tracing::error!(
                    error = %e,
                    delivery_tag = msg.delivery_tag,
                    reference_id = %msg.request.reference_id.as_deref().unwrap_or(""),
                    "individual message is poison, routing to DLQ"
                );
                split.dlq_tags.push(msg.delivery_tag);
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    delivery_tag = msg.delivery_tag,
                    "individual message infra error, requeuing"
                );
                split.requeue_tags.push(msg.delivery_tag);
            }
        }
    }
    split
}

#[derive(Default)]
struct ShardSplit {
    completed: Vec<PendingMessage>,
    failed_tags: Vec<u64>,
    skipped_tags: Vec<u64>,
    dlq_tags: Vec<u64>,
    requeue_tags: Vec<u64>,
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

/// Bulk INSERT placeholder rows for the batch and return the set
/// of `(reference_id, from_account) → id` whose row this batch
/// actually claimed. Conflicts are silent — the conflicting key
/// belongs to a prior batch's commit and is the caller's signal
/// to treat the message as an idempotent skip.
///
/// The placeholder is inserted with `status = 'pending'` and
/// `processed_at = NOW()`. The schema CHECK allows 'pending', and
/// the row is promoted to a terminal status by `bulk_update_status`
/// before tx commit, so external readers (READ COMMITTED snapshot)
/// only ever observe the terminal state.
async fn bulk_claim_slots(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    msgs: &[PendingMessage],
) -> Result<std::collections::HashMap<(String, String), Uuid>, AppError> {
    let n = msgs.len();
    let mut ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut from_accounts: Vec<String> = Vec::with_capacity(n);
    let mut to_accounts: Vec<String> = Vec::with_capacity(n);
    let mut amounts: Vec<Decimal> = Vec::with_capacity(n);
    let mut currencies: Vec<String> = Vec::with_capacity(n);
    let mut reference_ids: Vec<String> = Vec::with_capacity(n);
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(n);

    for msg in msgs {
        // Producer always supplies a reference_id (UUID fallback);
        // the consume callback rejects NULLs to DLQ before they
        // reach the buffer. Fall back to empty string here only
        // for the type-system — the conflict on (NULL, from) would
        // never match the message's logical key anyway.
        let ref_id = msg.request.reference_id.clone().unwrap_or_default();
        ids.push(Uuid::new_v4());
        from_accounts.push(msg.request.from_account.clone());
        to_accounts.push(msg.request.to_account.clone());
        amounts.push(msg.request.amount);
        currencies.push(msg.request.currency.clone());
        reference_ids.push(ref_id);
        descriptions.push(msg.request.description.clone());
    }

    let returned: Vec<(Uuid, Option<String>, String)> = sqlx::query_as(
        r#"INSERT INTO transactions
           (id, from_account, to_account, amount, currency, status, reference_id, description, processed_at)
           SELECT * FROM UNNEST(
               $1::uuid[], $2::text[], $3::text[], $4::numeric[], $5::text[],
               ARRAY_FILL('pending'::text, ARRAY[$8::int]),
               $6::text[], $7::text[],
               ARRAY_FILL(NOW()::timestamptz, ARRAY[$8::int])
           )
           ON CONFLICT (reference_id, from_account) DO NOTHING
           RETURNING id, reference_id, from_account"#,
    )
    .bind(&ids)
    .bind(&from_accounts)
    .bind(&to_accounts)
    .bind(&amounts)
    .bind(&currencies)
    .bind(&reference_ids)
    .bind(&descriptions)
    .bind(n as i32)
    .fetch_all(&mut **tx)
    .await?;

    Ok(returned
        .into_iter()
        .filter_map(|(id, ref_id, from_acc)| ref_id.map(|r| ((r, from_acc), id)))
        .collect())
}

/// Promote the placeholder rows claimed by `bulk_claim_slots` to
/// the given terminal status. Both `processed_at` and `updated_at`
/// are refreshed to `NOW()` so the timestamp on the terminal row
/// reflects when the status was finalised, not when the placeholder
/// was first inserted.
async fn bulk_update_status(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    msgs: &[PendingMessage],
    status: &'static str,
) -> Result<(), AppError> {
    let n = msgs.len();
    let mut reference_ids: Vec<String> = Vec::with_capacity(n);
    let mut from_accounts: Vec<String> = Vec::with_capacity(n);
    for msg in msgs {
        reference_ids.push(msg.request.reference_id.clone().unwrap_or_default());
        from_accounts.push(msg.request.from_account.clone());
    }
    sqlx::query(
        r#"UPDATE transactions
           SET status = $3, processed_at = NOW(), updated_at = NOW()
           WHERE (reference_id, from_account) IN (
               SELECT * FROM UNNEST($1::text[], $2::text[])
           )"#,
    )
    .bind(&reference_ids)
    .bind(&from_accounts)
    .bind(status)
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
///
/// `id` was added so the cache invalidator can DEL the per-id
/// cache key (`txn:{id}`) populated by the `get_by_id` handler.
/// Older subscribers without the field continue to work — they
/// just ignore the new key.
#[derive(serde::Serialize)]
struct TransactionCommittedPayload<'a> {
    id: Option<Uuid>,
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
            id: msg.id,
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
