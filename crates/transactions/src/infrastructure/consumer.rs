//! RabbitMQ batch consumer for the `transactions` write path.
//!
//! Owns the consumer side of `HTTP handler → service → producer
//! → consumer → DB write`. The consumer pulls messages from
//! `transactions.process`, buffers them up to `BATCH_SIZE`, and
//! flushes either when the buffer fills or when `BATCH_FLUSH_MS`
//! elapses — whichever comes first.
//!
//! Idempotency key shape: `txn:{shard}:{reference_id}`. The
//! `transactions.committed` event contract is the same shape the
//! notifications module consumes via the shared-kernel event bus.
//!
//! Money-safety invariants:
//!
//!   * The shard for a message is derived from `from_account`
//!     locally; the wire-supplied `shard` field (if any) is
//!     ignored — preventing a buggy or malicious producer from
//!     routing a debit to the wrong shard.
//!   * Idempotency check, debit, credit, and audit-row insert
//!     all run inside ONE per-shard transaction. A same-shard
//!     credit failure rolls back the debit atomically.
//!   * Failed rows carry `processed_at` so dashboards can
//!     measure end-to-end-to-terminal latency.
//!   * ACKs are per-tag (`multiple=false`) — cumulative-tag
//!     races between concurrent flushes can't lose a tag.
//!   * Empty-batch ACK is guarded against `delivery_tag=0`
//!     (an AMQP protocol error).
//!   * Cross-shard credits run in parallel post-commit and use
//!     the durable-outbox pattern (`cross_shard_outbox` table)
//!     for retries; see `cross_shard_processor.rs`.

use amqprs::{
    callbacks::DefaultConnectionCallback,
    channel::{BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicQosArguments},
    connection::{Connection, OpenConnectionArguments},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use shared_kernel::queue::callback::ConsumerChannelCallback;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use shared_kernel::db::shard::ShardRouter;
use shared_kernel::error::AppError;
use shared_kernel::events::{Event, EventPublisher, EVENT_TRANSACTIONS_COMMITTED};

const QUEUE_NAME: &str = "transactions.process";
const CONSUMER_TAG: &str = "peakload-consumer";
const BATCH_SIZE: usize = 100;
/// Maximum time a partial batch waits in the buffer with no new
/// arrival before the timer flushes it. Under sustained load the
/// idle window never opens (messages arrive every few ms), so the
/// size-flush at `BATCH_SIZE` dominates — bigger batches, higher
/// throughput. When inflow pauses, the last partial batch flushes
/// after this window, bounding straggler status-reflection latency
/// at roughly `IDLE_FLUSH_MS` for the slowest message.
///
/// Why this is larger than the previous fixed `BATCH_FLUSH_MS = 50`:
/// at 50ms the timer fired before the buffer could accumulate (~7
/// messages per consumer × 50ms), so per-shard batches averaged 2.1
/// instead of the configured cap of 100, and per-tx overhead was
/// amortised over almost nothing. See the 2026-05-21 throughput
/// design doc.
const IDLE_FLUSH_MS: u64 = 250;
/// How often the timer wakes up to evaluate `should_idle_flush`.
/// Decoupled from `IDLE_FLUSH_MS` so cancellation still drains
/// promptly (the `select!` polls cancellation on the same cadence).
const CHECK_INTERVAL_MS: u64 = 50;

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

    let last_arrival_ms = Arc::new(AtomicU64::new(0));
    let consumer = BatchTransactionConsumer {
        shard_router: shard_router.clone(),
        buffer: Arc::new(Mutex::new(Vec::with_capacity(BATCH_SIZE))),
        channel: shared_channel.clone(),
        events: events.clone(),
        spawned: Arc::new(Mutex::new(JoinSet::new())),
        last_arrival_ms: last_arrival_ms.clone(),
    };

    // Spawn flush timer — respects the cancellation token, drains
    // the buffer one last time on shutdown.
    let buffer_ref = consumer.buffer.clone();
    let router_ref = consumer.shard_router.clone();
    let timer_channel = shared_channel.clone();
    let timer_events = events.clone();
    let flush_cancel = cancel.clone();
    let timer_last_arrival = last_arrival_ms.clone();
    let timer_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(CHECK_INTERVAL_MS)) => {
                    // Debounce: only flush when the buffer has been
                    // idle for IDLE_FLUSH_MS. Under sustained load
                    // arrivals keep landing inside the window and the
                    // size-flush in `consume()` wins; we never enter
                    // this branch's drain. During quiet periods the
                    // last partial batch ships after the idle window.
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    let buf_len = buffer_ref.lock().await.len();
                    let last = timer_last_arrival.load(Ordering::Relaxed);
                    if should_idle_flush(buf_len, last, now_ms, IDLE_FLUSH_MS) {
                        let batch = drain_buffer(&buffer_ref).await;
                        if !batch.is_empty() {
                            let report = flush_batch_to_shards(&batch, &router_ref).await;
                            apply_acks(&timer_channel, &report, "idle-flush").await;
                            record_batch_metrics(&report);
                            publish_committed_events(&report.successful, &timer_events);
                        }
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

/// Decide whether the timer should flush a partial batch.
///
/// Returns true when the buffer has at least one message AND no new
/// arrival has landed for `idle_threshold_ms`. Under sustained load
/// the idle window never opens (messages arrive every few ms), so
/// the size-flush at `BATCH_SIZE` dominates and batches reach their
/// configured cap. When inflow stops or pauses, the last partial
/// batch flushes after a bounded idle window — bounding straggler
/// latency without capping throughput.
///
/// `saturating_sub` defends against `last_arrival_ms > now_ms`
/// (initial zero state, NTP step, clock skew) — that case is
/// treated as "just arrived", not "very old".
fn should_idle_flush(
    buf_len: usize,
    last_arrival_ms: u64,
    now_ms: u64,
    idle_threshold_ms: u64,
) -> bool {
    buf_len > 0 && now_ms.saturating_sub(last_arrival_ms) >= idle_threshold_ms
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
        // transactions_batch_size moved to flush_batch_to_shards
        // (O-10): it now carries a `shard` label and is recorded
        // per shard, so a summed/aggregate distribution is still
        // recoverable in PromQL (`histogram_quantile` without the
        // shard label) while a per-shard P95 also becomes plottable.
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
    /// A-3: holds the JoinHandles of size-flush tasks the consumer
    /// fires off. `consume()` cannot await flush directly (amqprs
    /// serialises the per-consumer call, so awaiting would wedge
    /// the AMQP read loop) — but dropping the JoinHandle silently
    /// loses panic observability and gives the orchestrator no
    /// way to know about leaked work. The JoinSet keeps panics
    /// visible (`background_task_panics_total`) and lets shutdown
    /// drain by `join_all`-ing.
    spawned: Arc<Mutex<JoinSet<()>>>,
    /// Wall-clock millis of the most recent buffer push. Read by
    /// the flush-timer to decide whether the buffer has been idle
    /// long enough to flush a partial batch (`should_idle_flush`).
    /// Updated in `consume()` after each push. Relaxed ordering is
    /// fine: the timer's idle-check tolerates a slightly stale read
    /// (it will just re-check on the next tick).
    last_arrival_ms: Arc<AtomicU64>,
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
        // Stamp arrival time AFTER the buffer push so the timer's
        // idle-check (which also drains under the buffer mutex) sees
        // a consistent "buffer has content AND was recently touched"
        // window. Relaxed is sufficient — the timer tolerates stale
        // reads, it will retry on the next tick.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_arrival_ms.store(now_ms, Ordering::Relaxed);

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
            // A-3: track every size-flush spawn in the JoinSet so a
            // panic is observable (counter + log) rather than
            // silently dropped by the default panic hook. Drain
            // completed tasks each call so the JoinSet does not
            // grow unbounded.
            let mut spawned = self.spawned.lock().await;
            spawned.spawn(async move {
                let report = flush_batch_to_shards(&batch, &router).await;
                apply_acks(&channel, &report, "size-flush").await;
                record_batch_metrics(&report);
                publish_committed_events(&report.successful, &events);
            });
            while let Some(res) = spawned.try_join_next() {
                if let Err(e) = res {
                    if e.is_panic() {
                        metrics::counter!(
                            "background_task_panics_total",
                            "task" => "consumer_size_flush"
                        )
                        .increment(1);
                        tracing::error!(
                            error = ?e,
                            "consumer size-flush task panicked (A-3 made this observable)"
                        );
                    }
                }
            }
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
                // O-10: per-shard batch-size distribution. Recorded
                // here (not in record_batch_metrics) because the
                // aggregated FlushReport has already merged every
                // shard's outcome — the `shard` label can only be
                // attached while the per-shard ShardOutcome is still
                // in hand. Ack count mirrors FlushReport::ack_count
                // (completed + failed + skipped are all ACKed).
                let shard_acked = completed.len() + failed_tags.len() + skipped_tags.len();
                if shard_acked > 0 {
                    metrics::histogram!(
                        "transactions_batch_size",
                        "shard" => sender_shard.to_string()
                    )
                    .record(shard_acked as f64);
                }
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
                // O-10: poison-fallback path still ACKs the
                // completed/failed/skipped split — record its
                // per-shard size too so the histogram is not
                // silently missing the fallback batches.
                let split_acked =
                    split.completed.len() + split.failed_tags.len() + split.skipped_tags.len();
                if split_acked > 0 {
                    metrics::histogram!(
                        "transactions_batch_size",
                        "shard" => sender_shard.to_string()
                    )
                    .record(split_acked as f64);
                }
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

/// Apply one shard's batch via the `apply_transactions_batch`
/// PL/pgSQL function — a single round-trip that runs the per-row
/// claim / debit / credit / refund / outbox sequence server-side.
///
/// The function returns one `(idx, outcome, assigned_id)` row per
/// input message; this fn marshals the batch into the function's
/// array arguments and maps the outcomes onto `ShardOutcome`.
/// `idx` is 1-based (PL/pgSQL `FOR i IN 1..n`).
///
/// Outcome semantics, preserved from the previous per-statement
/// Rust loop: a debit checks the running balance (two debits from
/// one `from_account` in a batch see each other's effects); a
/// same-shard credit miss refunds the sender atomically; a
/// cross-shard row queues a `cross_shard_outbox` row in the same
/// transaction and leaves the sender row 'processing'.
///
/// On any DB error — including a poison row that aborts the
/// function and rolls back the tx — the whole batch returns Err;
/// the caller (`flush_batch_to_shards`) falls back to a per-message
/// retry that isolates the bad row.
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

    // Marshal the batch into the function's array arguments. UUIDs
    // for transactions.id and cross_shard_outbox.id are generated
    // here so the function needs no pgcrypto / gen_random_uuid.
    // Messages with a NULL reference_id are rejected to DLQ by the
    // consume() callback before reaching this loop; defense-in-depth
    // skips any that slip through, and the kept subset stays
    // index-aligned with `owned` below.
    let n = messages.len();
    let mut ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut outbox_ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut from_accounts: Vec<String> = Vec::with_capacity(n);
    let mut to_accounts: Vec<String> = Vec::with_capacity(n);
    let mut amounts: Vec<Decimal> = Vec::with_capacity(n);
    let mut currencies: Vec<String> = Vec::with_capacity(n);
    let mut reference_ids: Vec<String> = Vec::with_capacity(n);
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(n);
    let mut receiver_shards: Vec<i32> = Vec::with_capacity(n);
    let mut owned: Vec<&PendingMessage> = Vec::with_capacity(n);

    for msg in messages {
        let ref_id = match msg.request.reference_id.as_deref() {
            Some(r) => r.to_string(),
            None => {
                skipped_tags.push(msg.delivery_tag);
                continue;
            }
        };
        ids.push(Uuid::new_v4());
        outbox_ids.push(Uuid::new_v4());
        from_accounts.push(msg.request.from_account.clone());
        to_accounts.push(msg.request.to_account.clone());
        amounts.push(msg.request.amount);
        currencies.push(msg.request.currency.clone());
        reference_ids.push(ref_id);
        descriptions.push(msg.request.description.clone());
        receiver_shards.push(router.shard_for_account(&msg.request.to_account) as i32);
        owned.push(msg);
    }

    if owned.is_empty() {
        return Ok(ShardOutcome {
            completed,
            failed_tags,
            skipped_tags,
        });
    }

    // Single round-trip: the function loops server-side and returns
    // one row per input. It runs in the connection's implicit
    // transaction; a poison row aborts it, sqlx surfaces the error,
    // and `flush_batch_to_shards` routes the whole batch into the
    // per-message poison-fallback (`process_messages_individually`).
    let outcomes: Vec<(i32, String, Option<Uuid>)> = sqlx::query_as(
        r#"SELECT idx, outcome, assigned_id
           FROM apply_transactions_batch(
               $1::uuid[], $2::uuid[], $3::text[], $4::text[],
               $5::numeric[], $6::text[], $7::text[], $8::text[],
               $9::int[], $10::int
           )"#,
    )
    .bind(&ids)
    .bind(&outbox_ids)
    .bind(&from_accounts)
    .bind(&to_accounts)
    .bind(&amounts)
    .bind(&currencies)
    .bind(&reference_ids)
    .bind(&descriptions)
    .bind(&receiver_shards)
    .bind(sender_shard as i32)
    .fetch_all(pool)
    .await?;

    for (idx_one_based, outcome, assigned_id) in outcomes {
        // PL/pgSQL `FOR i IN 1..n` yields idx starting at 1.
        let i = match (idx_one_based as usize).checked_sub(1) {
            Some(i) if i < owned.len() => i,
            _ => {
                tracing::error!(
                    idx = idx_one_based,
                    "apply_transactions_batch returned out-of-range idx"
                );
                continue;
            }
        };
        let src = owned[i];
        match outcome.as_str() {
            // Same-shard 'completed' and cross-shard 'processing'
            // are both durably committed and both ACK at the
            // consumer. publish_committed_events emits one
            // transactions.committed per row so the cache
            // invalidator DELs stale keys; the cross-shard
            // processor emits a second one when it flips
            // 'processing' -> 'completed'.
            "completed" | "processing" => {
                let mut m = src.clone();
                m.id = assigned_id;
                completed.push(m);
            }
            "failed" => failed_tags.push(src.delivery_tag),
            "skipped" => skipped_tags.push(src.delivery_tag),
            other => {
                tracing::error!(
                    outcome = other,
                    "apply_transactions_batch returned unknown outcome — treating as skipped"
                );
                skipped_tags.push(src.delivery_tag);
            }
        }
    }

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

#[cfg(test)]
mod tests {
    use super::should_idle_flush;

    #[test]
    fn idle_flush_yes_when_buffer_nonempty_and_idle_threshold_exceeded() {
        // last arrival at t=0, now at t=300, threshold 250 → idle
        assert!(should_idle_flush(3, 0, 300, 250));
    }

    #[test]
    fn idle_flush_no_when_buffer_empty() {
        // Nothing to flush even if idle for a long time.
        assert!(!should_idle_flush(0, 0, 1000, 250));
    }

    #[test]
    fn idle_flush_no_when_arrival_is_recent() {
        // 100ms since last arrival, threshold 250ms → keep accumulating.
        assert!(!should_idle_flush(5, 200, 300, 250));
    }

    #[test]
    fn idle_flush_yes_at_exactly_the_threshold() {
        // Boundary: now - last == threshold → flush (>=, not >).
        assert!(should_idle_flush(1, 0, 250, 250));
    }

    #[test]
    fn idle_flush_handles_clock_running_backwards() {
        // last_arrival_ms > now_ms (clock skew, NTP step, or initial state).
        // saturating_sub returns 0 → not idle → keep accumulating.
        assert!(!should_idle_flush(5, 1000, 500, 250));
    }
}
