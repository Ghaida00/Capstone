//! Drains `cross_shard_outbox` rows on each shard.
//!
//! Two paths per row, gated by `refund_required`:
//!   * `refund_required = false` (default) — apply the credit on
//!     the receiver shard atomically (dedupe via
//!     `cross_shard_outbox_applied`), insert the receiver-side
//!     `transactions` audit row, then mark the sender-side outbox
//!     row 'completed'.
//!   * `refund_required = true` — the prior apply attempt found
//!     the recipient missing/inactive on the receiver shard. The
//!     sender already debited at consumer-tx time, so without a
//!     compensating refund the funds disappear silently. This pass
//!     refunds the sender atomically (UPDATE balance + flip the
//!     sender-side audit row to 'reversed'), then marks the outbox
//!     row 'completed'.
//!
//! Idempotency:
//!   * Apply path — receiver-side `cross_shard_outbox_applied`
//!     primary key on `(sender_shard, outbox_id)` makes re-runs a
//!     no-op.
//!   * Refund path — the audit-row update is conditioned on
//!     `status != 'reversed'`, and the balance UPDATE is gated on
//!     that update having matched, so a redelivered refund is
//!     also a no-op.
//!
//! When a row exhausts `MAX_ATTEMPTS` it terminal-marks 'failed'
//! on the credit path so it drops out of the working set, and
//! (R-8) the sender audit row transitions 'processing' → 'failed'
//! with `failure_reason` populated. The refund path never
//! terminal-fails by design — the sender is debited and only a
//! successful refund restores correctness; it instead holds
//! `attempts` at the cap and defers `lease_until` by
//! `REFUND_STUCK_BACKOFF_SECS` so the row stays claimable on a
//! sane cadence rather than at every poll tick.

use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use rust_decimal::Decimal;
use sqlx::FromRow;
use std::collections::{BTreeMap, HashSet};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::db::shard::ShardRouter;

const POLL_INTERVAL_MS: u64 = 250;
const BATCH_LIMIT: i64 = 100;
/// Default number of credit chunks applied concurrently within a single
/// `drain_shard` call (tunable via CROSS_SHARD_APPLY_CONCURRENCY). The
/// batched apply commits one chunk-transaction at a time, so a serial
/// drain tick's wall-time is the SUM of its chunk-commit fsyncs; fanning
/// the chunks out lets those fsyncs pipeline (and group-commit on the PG
/// side), which is what lifts drain throughput above the cross-shard
/// arrival rate. Each in-flight chunk holds at most one receiver-pool
/// connection (during apply) then one sender-pool connection (during the
/// bookkeeping), so 8 keeps peak usage well inside DB_WRITE_POOL_SIZE=40
/// with headroom for the local consumer + POST handlers on the same node.
const APPLY_CONCURRENCY_DEFAULT: usize = 8;
const MAX_ATTEMPTS: i32 = 10;
/// Lease window after a row is claimed. If the holder crashes,
/// another instance re-claims it after this expiry.
const LEASE_SECS: i64 = 60;
/// Backoff lease for the refund stuck-state path. Past
/// `MAX_ATTEMPTS` a refund row stays claimable forever (we never
/// terminal-fail it — sender is debited), so without this lease
/// the row would be re-claimed every `POLL_INTERVAL_MS` and
/// hammer a struggling downstream shard. Five minutes is short
/// enough that auto-recovery from a transient downstream outage
/// catches up quickly, long enough that an extended outage does
/// not produce per-tick failure spam.
const REFUND_STUCK_BACKOFF_SECS: i64 = 300;
/// How often the processor sweeps settled rows out of the outbox
/// tables. Settled rows are pure history — the credit already landed
/// and the receiver-side `cross_shard_outbox_applied` dedupe row (NOT
/// the outbox row) is what guards against a double-credit — so keeping
/// them only bloats the table, its page-cache footprint, and
/// autovacuum's workload without bound. Sweeping once a minute holds
/// both tables at a bounded steady-state size under sustained load.
const PRUNE_INTERVAL_SECS: u64 = 60;
/// Default age a `completed` outbox row must reach before it is
/// eligible for deletion (tunable via CROSS_SHARD_OUTBOX_RETENTION_SECS).
/// Vastly longer than a saga takes (sub-second), so a row is only ever
/// deleted long after its work finished. `failed` rows are deliberately
/// NOT swept — they require manual reconciliation (see
/// `handle_attempt_error`), so ops must still be able to query them.
const OUTBOX_RETENTION_SECS_DEFAULT: i64 = 300;
/// Floor for how long a `cross_shard_outbox_applied` dedupe row is kept.
/// That row guards the receiver `users.balance` UPDATE against the SAME
/// outbox row being applied twice (a lease re-claim race). Once the
/// corresponding outbox row is gone that race is impossible, so the only
/// correctness requirement is applied-retention >= outbox-retention; the
/// 1h floor is pure defensive margin and the rows are tiny (~30 bytes).
const APPLIED_RETENTION_FLOOR_SECS: i64 = 3600;
/// Max rows deleted per DELETE statement. Keeps each statement well
/// under the DB `statement_timeout` (2s) and bounds its lock footprint,
/// so a large one-time backlog drains across several sweeps instead of
/// one table-locking mega-delete that the timeout would abort anyway.
const PRUNE_BATCH: i64 = 10_000;
/// Max DELETE batches per table per sweep. Caps per-sweep work at
/// `PRUNE_BATCH * PRUNE_MAX_BATCHES` rows so a huge backlog is bounded
/// per tick while still draining far faster than rows arrive.
const PRUNE_MAX_BATCHES: usize = 50;
#[derive(Clone, FromRow)]
struct OutboxRow {
    id: Uuid,
    from_account: String,
    to_account: String,
    to_shard: i32,
    amount: Decimal,
    currency: String,
    reference_id: String,
    description: Option<String>,
    attempts: i32,
    refund_required: bool,
}

/// Outcome of a receiver-side apply attempt.
enum ApplyOutcome {
    /// Credit successfully applied this attempt.
    Applied,
    /// Dedupe row already present — a previous attempt won the
    /// race. Caller treats this as completed (no extra action).
    AlreadyApplied,
    /// Receiver row missing or inactive. The dedupe row IS
    /// inserted (so the receiver-side audit is durable), but the
    /// sender's debit has not yet been compensated. Caller flips
    /// `refund_required` so the next pass takes the refund path.
    RecipientMissing,
}

pub fn spawn_cross_shard_processor(
    shards: ShardRouter,
    events: Arc<dyn shared_kernel::events::EventPublisher>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Experiment knobs (env-tunable so the chunk size can be swept
        // without a rebuild). CROSS_SHARD_CLAIM_LIMIT caps rows claimed per
        // drain; CROSS_SHARD_APPLY_CHUNK caps rows per credit commit —
        // smaller shortens the lock-hold on `users` rows (less contention
        // with the local apply_transactions_batch), larger means fewer
        // commits. Defaults preserve prior behaviour.
        let claim_limit: i64 = std::env::var("CROSS_SHARD_CLAIM_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(BATCH_LIMIT);
        let apply_chunk: usize = std::env::var("CROSS_SHARD_APPLY_CHUNK")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(claim_limit as usize);
        let apply_concurrency: usize = std::env::var("CROSS_SHARD_APPLY_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(APPLY_CONCURRENCY_DEFAULT);
        // Retention window for swept `completed` outbox rows. The applied
        // dedupe rows must outlive the outbox rows they guard, so the
        // applied window is floored at APPLIED_RETENTION_FLOOR_SECS and can
        // never drop below the outbox window.
        let outbox_retention_secs: i64 = std::env::var("CROSS_SHARD_OUTBOX_RETENTION_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(OUTBOX_RETENTION_SECS_DEFAULT);
        let applied_retention_secs = outbox_retention_secs.max(APPLIED_RETENTION_FLOOR_SECS);
        tracing::info!(
            claim_limit,
            apply_chunk,
            apply_concurrency,
            outbox_retention_secs,
            applied_retention_secs,
            prune_interval_secs = PRUNE_INTERVAL_SECS,
            "cross-shard outbox processor started"
        );
        let mut ticker = tokio::time::interval(Duration::from_millis(POLL_INTERVAL_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Independent, slow cadence for the retention sweep — decoupled
        // from the 250ms drain so pruning never competes with a drain tick.
        let mut prune_ticker = tokio::time::interval(Duration::from_secs(PRUNE_INTERVAL_SECS));
        prune_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = prune_ticker.tick() => {
                    // Borrow `shards` directly — the sweep runs inline (not
                    // spawned), so no clone is needed and it settles before
                    // the next tick is serviced.
                    prune_settled(&shards, outbox_retention_secs, applied_retention_secs).await;
                }
                _ = ticker.tick() => {
                    // Drain each shard concurrently within the tick.
                    // The shards are fully independent (distinct sender
                    // pool, distinct claimed rows via FOR UPDATE SKIP
                    // LOCKED). Joining before returning preserves the
                    // per-tick semantics of the previous sequential
                    // loop — the next tick does not start before every
                    // shard's drain has settled.
                    let mut shard_handles = Vec::with_capacity(shards.num_shards());
                    for sender_shard in 0..shards.num_shards() {
                        let shards_cl = shards.clone();
                        let events_cl = events.clone();
                        shard_handles.push(tokio::spawn(async move {
                            if let Err(e) = drain_shard(&shards_cl, sender_shard, &events_cl, claim_limit, apply_chunk, apply_concurrency).await {
                                tracing::warn!(
                                    shard = sender_shard,
                                    error = %e,
                                    "outbox drain error"
                                );
                            }
                        }));
                    }
                    for h in shard_handles {
                        // Best-effort join — a panicked task is a bug
                        // surfaced by the existing panic hook /
                        // counter; we don't want one shard's panic to
                        // wedge the tick loop.
                        let _ = h.await;
                    }
                }
            }
        }
        tracing::info!("cross-shard outbox processor exiting");
    })
}

/// Publish a `transactions.committed` event for a cross-shard
/// sender row whose credit just landed, so the cache invalidator
/// DELs the stale `tx_status:` Redis entry immediately instead of
/// letting the client wait out the 5 s cache TTL. `id` is None —
/// the cross-shard processor does not hold the sender row's
/// `transactions.id`; the invalidator keys the `tx_status:` DEL on
/// `reference_id`, which is what matters here. Best-effort —
/// `publish_one_committed_event` never propagates failures.
fn publish_sender_completed_event(
    events: &Arc<dyn shared_kernel::events::EventPublisher>,
    sender_shard: usize,
    row: &OutboxRow,
) {
    super::consumer::publish_one_committed_event(
        events,
        super::consumer::TransactionCommittedPayload {
            id: None,
            from_account: &row.from_account,
            to_account: &row.to_account,
            amount: row.amount.to_string(),
            currency: &row.currency,
            reference_id: Some(row.reference_id.as_str()),
            shard: sender_shard,
        },
        "",
    );
}

async fn drain_shard(
    shards: &ShardRouter,
    sender_shard: usize,
    events: &Arc<dyn shared_kernel::events::EventPublisher>,
    claim_limit: i64,
    apply_chunk: usize,
    apply_concurrency: usize,
) -> Result<(), sqlx::Error> {
    let sender_pool = shards.writer(sender_shard).clone();
    // Atomic lease-claim. SKIP LOCKED keeps two app instances
    // from grabbing the same row; lease_until gates re-claim if
    // the holder crashes mid-process (LEASE_SECS expiry).
    let rows: Vec<OutboxRow> = sqlx::query_as(
        r#"
        UPDATE cross_shard_outbox
        SET lease_until = NOW() + (INTERVAL '1 second' * $3),
            updated_at = NOW()
        WHERE id IN (
            SELECT id FROM cross_shard_outbox
            WHERE status = 'pending'
              AND attempts < $1
              AND (lease_until IS NULL OR lease_until < NOW())
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $2
        )
        RETURNING id, from_account, to_account, to_shard, amount, currency,
                  reference_id, description, attempts, refund_required
        "#,
    )
    .bind(MAX_ATTEMPTS)
    .bind(claim_limit)
    .bind(LEASE_SECS)
    .fetch_all(&sender_pool)
    .await?;

    if rows.is_empty() {
        return Ok(());
    }

    // refund_required rows take the per-row compensation path (rare — it
    // only fires when a prior credit attempt found the recipient
    // missing). Credit rows take the batched fast path below.
    let (refund_rows, credit_rows): (Vec<OutboxRow>, Vec<OutboxRow>) =
        rows.into_iter().partition(|r| r.refund_required);

    for row in &refund_rows {
        process_one_row(&sender_pool, shards, sender_shard, events, row).await;
    }

    // Group credit rows by receiver shard so each receiver's credits apply
    // against one pool. In a 2-shard topology every row in a sender shard's
    // outbox targets the same receiver, so this is usually a single group;
    // each group is then split into chunks applied concurrently below.
    let mut groups: BTreeMap<i32, Vec<OutboxRow>> = BTreeMap::new();
    for row in credit_rows {
        groups.entry(row.to_shard).or_default().push(row);
    }

    for (to_shard, group) in groups {
        let receiver_shard = to_shard as usize;
        if receiver_shard >= shards.num_shards() {
            for row in &group {
                best_effort(
                    "mark_failed (invalid to_shard)",
                    row.id,
                    mark_failed(&sender_pool, row.id, "invalid to_shard").await,
                );
            }
            continue;
        }

        let receiver_pool = shards.writer(receiver_shard);
        // Apply the group as CROSS_SHARD_APPLY_CHUNK-sized commits, fanned
        // out CROSS_SHARD_APPLY_CONCURRENCY-wide. Batching bounds the commit
        // COUNT (one commit per chunk, not per row); concurrency keeps those
        // chunk-commits in flight together instead of serialising, so a drain
        // tick is gated by the slowest few commits rather than their sum. It
        // also supplies the >= commit_siblings concurrent commits that the
        // configured group-commit (commit_delay) needs before it engages, and
        // — under a durable synchronous_commit — lets their fsyncs coalesce.
        // Chunk size still trades commit-amortisation against `users`
        // lock-hold; the two knobs are swept together.
        let chunks: Vec<Vec<OutboxRow>> = group.chunks(apply_chunk).map(|c| c.to_vec()).collect();
        stream::iter(chunks)
            .for_each_concurrent(apply_concurrency, |chunk_rows| {
                // Clone the cheap Arc-backed handles into each task. Claimed
                // rows are disjoint (FOR UPDATE SKIP LOCKED) so chunks never
                // contend on the same outbox row, and the receiver dedupe PK
                // still absorbs any duplicate apply.
                let sender_pool = sender_pool.clone();
                let receiver_pool = receiver_pool.clone();
                let events = events.clone();
                let shards = shards.clone();
                async move {
                    process_credit_chunk(
                        &sender_pool,
                        &receiver_pool,
                        sender_shard,
                        &events,
                        &chunk_rows,
                        &shards,
                    )
                    .await;
                }
            })
            .await;
    }

    Ok(())
}

/// Apply one batched credit chunk and reconcile its sender-side
/// bookkeeping. A free function so `drain_shard` can drive chunks
/// concurrently via `for_each_concurrent`. Invariants: the sender audit
/// row is flipped to 'completed' BEFORE the outbox row is marked completed
/// (a mid-chunk crash then re-claims the row rather than stranding the
/// sender audit in 'processing'); recipient-missing rows route to the
/// refund path; and a failed chunk falls back to per-row processing, where
/// the dedupe PK makes any re-apply a no-op.
async fn process_credit_chunk(
    sender_pool: &sqlx::PgPool,
    receiver_pool: &sqlx::PgPool,
    sender_shard: usize,
    events: &Arc<dyn shared_kernel::events::EventPublisher>,
    chunk_rows: &[OutboxRow],
    shards: &ShardRouter,
) {
    match apply_credits_batch(receiver_pool, sender_shard, chunk_rows).await {
        Ok(outcome) => {
            // Rows whose recipient was missing/inactive go to the refund
            // path; everyone else (freshly applied OR already applied on a
            // prior attempt) is a completed credit.
            let (missing, completed): (Vec<&OutboxRow>, Vec<&OutboxRow>) = chunk_rows
                .iter()
                .partition(|r| outcome.missing.contains(&r.id));

            if !completed.is_empty() {
                let applied = completed.len().saturating_sub(outcome.redundant);
                metrics::counter!("cross_shard_credit_applied_total").increment(applied as u64);
                metrics::counter!("cross_shard_credit_redundant_total")
                    .increment(outcome.redundant as u64);
                // Flip the sender audit rows FIRST, and only mark the outbox
                // rows completed if that succeeded — otherwise leave them
                // pending so the next tick re-claims and retries (the dedupe
                // PK makes the re-apply a no-op), rather than stranding the
                // sender rows in 'processing'.
                match mark_sender_completed_many(sender_pool, &completed).await {
                    Ok(()) => {
                        if let Err(e) = mark_outbox_completed_many(sender_pool, &completed).await {
                            tracing::warn!(op = "mark_outbox_completed_many", n = completed.len(), error = %e, "outbox batch bookkeeping failed");
                        }
                        for &row in &completed {
                            publish_sender_completed_event(events, sender_shard, row);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(op = "mark_sender_completed_many", n = completed.len(), error = %e, "sender completion failed; leaving outbox rows pending for retry");
                    }
                }
            }

            if !missing.is_empty() {
                metrics::counter!("cross_shard_credit_recipient_missing_total")
                    .increment(missing.len() as u64);
                for &row in &missing {
                    tracing::warn!(outbox_id = %row.id, from = %row.from_account, to = %row.to_account, "cross-shard recipient missing — flagging for sender refund");
                }
                if let Err(e) = mark_refund_required_many(sender_pool, &missing).await {
                    tracing::warn!(op = "mark_refund_required_many", n = missing.len(), error = %e, "outbox batch bookkeeping failed");
                }
            }
        }
        Err(e) => {
            // The chunk is atomic, so a failure means nothing in it was
            // applied. Fall back to the per-row path so each row's attempt
            // count / terminal-fail / recipient-missing accounting stays
            // exactly as before; the dedupe PK makes any later re-apply a
            // no-op.
            tracing::warn!(shard = sender_shard, rows = chunk_rows.len(), error = %e, "cross-shard credit chunk failed; falling back to per-row");
            for row in chunk_rows {
                process_one_row(sender_pool, shards, sender_shard, events, row).await;
            }
        }
    }
}

/// Sweep settled rows out of both cross-shard tables on every shard.
///
/// Deletes (a) `completed` outbox rows older than `outbox_retention_secs`
/// and (b) `cross_shard_outbox_applied` dedupe rows older than
/// `applied_retention_secs` (always >= the outbox window, so an applied
/// row never outlives-by-less the outbox row it guards). `failed` outbox
/// rows are left for manual reconciliation.
///
/// Best-effort: a batch failure is logged and retried on the next sweep
/// — pruning must never wedge the drain loop. Runs on every shard from
/// every app instance; the DELETEs are naturally idempotent so concurrent
/// sweepers just split the work.
async fn prune_settled(
    shards: &ShardRouter,
    outbox_retention_secs: i64,
    applied_retention_secs: i64,
) {
    for shard in 0..shards.num_shards() {
        let pool = shards.writer(shard);
        // Subselect-by-PK (`id`) keeps the bounded delete planner-friendly.
        let outbox_deleted = prune_in_batches(
            pool,
            "DELETE FROM cross_shard_outbox \
             WHERE id IN ( \
                 SELECT id FROM cross_shard_outbox \
                 WHERE status = 'completed' \
                   AND completed_at < NOW() - (INTERVAL '1 second' * $1) \
                 LIMIT $2 \
             )",
            outbox_retention_secs,
        )
        .await;
        // `cross_shard_outbox_applied` has a composite PK, so bound the
        // batch by physical `ctid` instead — the standard idiom for a
        // LIMIT-capped delete on a table without a single-column key.
        let applied_deleted = prune_in_batches(
            pool,
            "DELETE FROM cross_shard_outbox_applied \
             WHERE ctid IN ( \
                 SELECT ctid FROM cross_shard_outbox_applied \
                 WHERE applied_at < NOW() - (INTERVAL '1 second' * $1) \
                 LIMIT $2 \
             )",
            applied_retention_secs,
        )
        .await;
        if outbox_deleted > 0 || applied_deleted > 0 {
            metrics::counter!("cross_shard_outbox_pruned_total").increment(outbox_deleted);
            metrics::counter!("cross_shard_applied_pruned_total").increment(applied_deleted);
            tracing::debug!(
                shard,
                outbox_deleted,
                applied_deleted,
                "cross-shard retention sweep removed settled rows"
            );
        }
    }
}

/// Run a bounded DELETE repeatedly until it stops finding rows or hits
/// `PRUNE_MAX_BATCHES`, returning the total deleted. Each statement
/// deletes at most `PRUNE_BATCH` rows — keeping it well under
/// `statement_timeout` and bounding the lock-hold — while the loop lets a
/// large one-time backlog drain across this sweep without ever issuing an
/// unbounded mega-delete. `sql` must bind $1 = retention secs, $2 = limit.
async fn prune_in_batches(pool: &sqlx::PgPool, sql: &str, retention_secs: i64) -> u64 {
    let mut total = 0u64;
    for _ in 0..PRUNE_MAX_BATCHES {
        match sqlx::query(sql)
            .bind(retention_secs)
            .bind(PRUNE_BATCH)
            .execute(pool)
            .await
        {
            Ok(r) => {
                let n = r.rows_affected();
                total += n;
                // A short batch means the cutoff is drained for now.
                if (n as i64) < PRUNE_BATCH {
                    break;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "cross-shard retention sweep batch failed");
                break;
            }
        }
    }
    total
}

/// Per-row processing — the original path. Still used for (a) the rare
/// `refund_required` compensation rows and (b) the fallback when a
/// batched credit transaction fails, where precise per-row attempt /
/// terminal-fail accounting matters.
async fn process_one_row(
    sender_pool: &sqlx::PgPool,
    shards: &ShardRouter,
    sender_shard: usize,
    events: &Arc<dyn shared_kernel::events::EventPublisher>,
    row: &OutboxRow,
) {
    let receiver_shard = row.to_shard as usize;
    if receiver_shard >= shards.num_shards() {
        best_effort(
            "mark_failed (invalid to_shard)",
            row.id,
            mark_failed(sender_pool, row.id, "invalid to_shard").await,
        );
        return;
    }

    if row.refund_required {
        match refund_sender(sender_pool, row).await {
            Ok(()) => {
                metrics::counter!("cross_shard_refund_applied_total").increment(1);
                best_effort(
                    "mark_completed",
                    row.id,
                    mark_completed(sender_pool, row.id).await,
                );
            }
            Err(e) => {
                handle_attempt_error(sender_pool, row, e.to_string(), "refund").await;
            }
        }
        return;
    }

    let receiver_pool = shards.writer(receiver_shard);
    match apply_on_receiver(receiver_pool, sender_shard, row).await {
        Ok(ApplyOutcome::Applied) => {
            metrics::counter!("cross_shard_credit_applied_total").increment(1);
            best_effort(
                "mark_sender_completed",
                row.id,
                mark_sender_completed(sender_pool, &row.reference_id, &row.from_account).await,
            );
            publish_sender_completed_event(events, sender_shard, row);
            best_effort(
                "mark_completed",
                row.id,
                mark_completed(sender_pool, row.id).await,
            );
        }
        Ok(ApplyOutcome::AlreadyApplied) => {
            metrics::counter!("cross_shard_credit_redundant_total").increment(1);
            best_effort(
                "mark_sender_completed",
                row.id,
                mark_sender_completed(sender_pool, &row.reference_id, &row.from_account).await,
            );
            publish_sender_completed_event(events, sender_shard, row);
            best_effort(
                "mark_completed",
                row.id,
                mark_completed(sender_pool, row.id).await,
            );
        }
        Ok(ApplyOutcome::RecipientMissing) => {
            metrics::counter!("cross_shard_credit_recipient_missing_total").increment(1);
            tracing::warn!(outbox_id = %row.id, from = %row.from_account, to = %row.to_account, "cross-shard recipient missing — flagging for sender refund");
            best_effort(
                "mark_refund_required",
                row.id,
                mark_refund_required(sender_pool, row.id).await,
            );
        }
        Err(e) => {
            handle_attempt_error(sender_pool, row, e.to_string(), "credit").await;
        }
    }
}

/// Outcome of a batched credit apply.
struct BatchOutcome {
    /// Outbox ids whose recipient was missing/inactive — caller flips
    /// these to `refund_required`.
    missing: HashSet<Uuid>,
    /// Count of dedupe-skips (credit already applied on a prior
    /// attempt) — completed, but tracked separately for metrics.
    redundant: usize,
}

/// Apply a batch of cross-shard credits to one receiver shard inside a
/// SINGLE transaction (one commit). Reuses the exact per-row statements
/// of `apply_on_receiver`, so idempotency (the
/// `cross_shard_outbox_applied` dedupe PK) and recipient-missing
/// detection are unchanged — only the commit boundary moved out of the
/// row loop. The batch commits or rolls back atomically; on rollback the
/// caller re-runs the group per-row and the dedupe makes any re-apply a
/// no-op.
async fn apply_credits_batch(
    pool: &sqlx::PgPool,
    sender_shard: usize,
    rows: &[OutboxRow],
) -> Result<BatchOutcome, sqlx::Error> {
    let mut missing = HashSet::new();
    let mut redundant = 0usize;
    let mut tx = pool.begin().await?;

    for row in rows {
        let deduped = sqlx::query(
            "INSERT INTO cross_shard_outbox_applied (sender_shard, outbox_id) \
             VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(sender_shard as i32)
        .bind(row.id)
        .execute(&mut *tx)
        .await?;
        if deduped.rows_affected() == 0 {
            // Already applied on a prior attempt — credit already landed.
            redundant += 1;
            continue;
        }

        let credited = sqlx::query(
            "UPDATE users SET balance = balance + $1 \
             WHERE account_number = $2 AND status = 'active'",
        )
        .bind(row.amount)
        .bind(&row.to_account)
        .execute(&mut *tx)
        .await?;

        let status = if credited.rows_affected() == 0 {
            missing.insert(row.id);
            "failed"
        } else {
            "completed"
        };

        sqlx::query(
            r#"INSERT INTO transactions
               (id, from_account, to_account, amount, currency, status,
                reference_id, description, processed_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
               ON CONFLICT (reference_id, from_account) DO NOTHING"#,
        )
        .bind(Uuid::new_v4())
        .bind(&row.from_account)
        .bind(&row.to_account)
        .bind(row.amount)
        .bind(&row.currency)
        .bind(status)
        .bind(&row.reference_id)
        .bind(&row.description)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(BatchOutcome { missing, redundant })
}

/// Batched `mark_completed` for sender-side outbox rows whose credit
/// landed. The `status='pending'` guard mirrors the single-row helper.
async fn mark_outbox_completed_many(
    pool: &sqlx::PgPool,
    rows: &[&OutboxRow],
) -> Result<(), sqlx::Error> {
    let ids: Vec<Uuid> = rows.iter().map(|r| r.id).collect();
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET status = 'completed', completed_at = NOW(), updated_at = NOW() \
         WHERE id = ANY($1) AND status = 'pending'",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(())
}

/// Batched `mark_sender_completed` — flips sender-side `transactions`
/// audit rows 'processing' → 'completed', matched on `(reference_id,
/// from_account)` and guarded on `status='processing'` so terminal rows
/// are untouched.
async fn mark_sender_completed_many(
    pool: &sqlx::PgPool,
    rows: &[&OutboxRow],
) -> Result<(), sqlx::Error> {
    let refs: Vec<String> = rows.iter().map(|r| r.reference_id.clone()).collect();
    let froms: Vec<String> = rows.iter().map(|r| r.from_account.clone()).collect();
    sqlx::query(
        "UPDATE transactions t \
         SET status = 'completed', processed_at = NOW(), updated_at = NOW() \
         FROM UNNEST($1::text[], $2::text[]) AS x(reference_id, from_account) \
         WHERE t.reference_id = x.reference_id \
           AND t.from_account = x.from_account \
           AND t.status = 'processing'",
    )
    .bind(refs)
    .bind(froms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Batched `mark_refund_required` — clears the lease so the next tick
/// re-claims each row onto the refund path immediately.
async fn mark_refund_required_many(
    pool: &sqlx::PgPool,
    rows: &[&OutboxRow],
) -> Result<(), sqlx::Error> {
    let ids: Vec<Uuid> = rows.iter().map(|r| r.id).collect();
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET refund_required = TRUE, lease_until = NULL, updated_at = NOW() \
         WHERE id = ANY($1)",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(())
}

/// Common path for "step failed, decide whether to retry or
/// terminally fail". Step semantics differ:
///
/// * `step = "credit"` — bumps `attempts`; once `attempts + 1 >=
///   MAX_ATTEMPTS` the outbox row is marked 'failed' AND the
///   sender's `transactions` audit row is transitioned from
///   'processing' to 'failed' with `failure_reason` populated
///   from `last_error`. The customer's GET endpoint therefore
///   sees a real terminal state instead of indefinite
///   'processing'; ops triage queries on `status = 'failed' AND
///   failure_reason LIKE 'max attempts at credit:%'` (see the
///   admin endpoint and the cross-shard-outbox-reconciliation
///   runbook for the standard procedure).
/// * `step = "refund"` — NEVER terminal-fails. The sender is
///   already debited and only a successful refund can make them
///   whole; marking the outbox 'failed' here would strand funds
///   with the audit row stuck at 'processing' forever. Past the
///   `MAX_ATTEMPTS` threshold we hold `attempts` at the cap and
///   defer the lease by `REFUND_STUCK_BACKOFF_SECS` so re-claim
///   happens at a sane cadence rather than every poll tick. The
///   `cross_shard_refund_stuck_total` counter therefore reads as
///   "refund retry attempts while stuck" (12/hour/row at the
///   default backoff), not as a count of distinct stuck rows.
///   The audit row legitimately stays at 'processing' here —
///   the transaction is still in flight from the customer's
///   perspective until the compensating refund lands.
async fn handle_attempt_error(
    pool: &sqlx::PgPool,
    row: &OutboxRow,
    err: String,
    step: &'static str,
) {
    let next_attempt = row.attempts + 1;
    metrics::counter!("cross_shard_step_failures_total", "step" => step).increment(1);

    if next_attempt >= MAX_ATTEMPTS && step == "refund" {
        tracing::error!(
            outbox_id = %row.id,
            attempts = row.attempts,
            step,
            backoff_secs = REFUND_STUCK_BACKOFF_SECS,
            error = %err,
            "cross-shard REFUND exceeded retry budget — sender debited, no compensation yet. Deferring re-claim; manual ops triage required."
        );
        metrics::counter!("cross_shard_refund_stuck_total").increment(1);
        // Hold attempts at the cap (don't bump) so the
        // `attempts < MAX_ATTEMPTS` filter in the claim query
        // still selects this row, and defer the next claim by
        // REFUND_STUCK_BACKOFF_SECS.
        best_effort(
            "defer_lease (refund stuck)",
            row.id,
            defer_lease(pool, row.id, &err, REFUND_STUCK_BACKOFF_SECS).await,
        );
        return;
    }

    if next_attempt >= MAX_ATTEMPTS {
        tracing::error!(
            outbox_id = %row.id,
            attempts = next_attempt,
            step,
            error = %err,
            "cross-shard step exhausted retry budget — marking outbox failed (manual reconciliation required)"
        );
        // Bump first so the attempts column reflects reality, then
        // terminal-fail. Both errors are best-effort logged; if
        // either DB write fails the row stays 'pending' and the
        // next drain re-evaluates with the new attempt count.
        best_effort(
            "bump_attempt (terminal)",
            row.id,
            bump_attempt(pool, row.id, &err).await,
        );
        let reason = format!("max attempts at {}: {}", step, err);
        best_effort(
            "mark_failed (max attempts)",
            row.id,
            mark_failed(pool, row.id, &reason).await,
        );
        // R-8: terminal credit failures must also transition the
        // sender audit row from 'processing' to 'failed' so the
        // customer-visible state stops claiming "in flight". Only
        // the credit path takes this branch — the refund path
        // returns above via the defer-lease arm and deliberately
        // leaves the audit row at 'processing'.
        if step == "credit" {
            best_effort(
                "mark_sender_failed (max attempts)",
                row.id,
                mark_sender_failed(pool, &row.reference_id, &row.from_account, &reason).await,
            );
        }
        metrics::counter!("cross_shard_outbox_terminal_failures_total", "step" => step)
            .increment(1);
    } else {
        tracing::warn!(
            outbox_id = %row.id,
            attempts = next_attempt,
            step,
            error = %err,
            "cross-shard step failed, will retry"
        );
        best_effort(
            "bump_attempt",
            row.id,
            bump_attempt(pool, row.id, &err).await,
        );
    }
}

/// Records the last error and pushes the lease forward by
/// `defer_secs` seconds without bumping `attempts`. Used by the
/// refund stuck-state path: the row stays eligible for re-claim
/// indefinitely, but the next claim is gated on the lease
/// expiring, so a stuck downstream is hit at most once per
/// `defer_secs` rather than once per poll tick.
async fn defer_lease(
    pool: &sqlx::PgPool,
    id: Uuid,
    last_error: &str,
    defer_secs: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET last_error = $2, \
             lease_until = NOW() + (INTERVAL '1 second' * $3), \
             updated_at = NOW() \
         WHERE id = $1",
    )
    .bind(id)
    .bind(last_error)
    .bind(defer_secs)
    .execute(pool)
    .await?;
    Ok(())
}

fn best_effort(op: &'static str, id: Uuid, res: Result<(), sqlx::Error>) {
    if let Err(e) = res {
        tracing::warn!(outbox_id = %id, op, error = %e, "outbox bookkeeping failed");
    }
}

async fn apply_on_receiver(
    pool: &sqlx::PgPool,
    sender_shard: usize,
    row: &OutboxRow,
) -> Result<ApplyOutcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    // Dedupe row — insert succeeds only on first apply.
    let inserted = sqlx::query(
        "INSERT INTO cross_shard_outbox_applied (sender_shard, outbox_id) \
         VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(sender_shard as i32)
    .bind(row.id)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        // Already applied — commit the empty tx and signal idempotent skip.
        tx.commit().await?;
        return Ok(ApplyOutcome::AlreadyApplied);
    }

    let credited = sqlx::query(
        "UPDATE users SET balance = balance + $1 \
         WHERE account_number = $2 AND status = 'active'",
    )
    .bind(row.amount)
    .bind(&row.to_account)
    .execute(&mut *tx)
    .await?;

    if credited.rows_affected() == 0 {
        // Recipient missing/inactive on the receiver shard. Record
        // a receiver-side audit row as 'failed' so the receiver's
        // history reflects the attempt, commit the dedupe row so
        // the apply step itself is idempotent, then signal
        // RecipientMissing so the caller flips refund_required on
        // the sender-side outbox row.
        sqlx::query(
            r#"INSERT INTO transactions
               (id, from_account, to_account, amount, currency, status,
                reference_id, description, processed_at)
               VALUES ($1, $2, $3, $4, $5, 'failed', $6, $7, NOW())
               ON CONFLICT (reference_id, from_account) DO NOTHING"#,
        )
        .bind(Uuid::new_v4())
        .bind(&row.from_account)
        .bind(&row.to_account)
        .bind(row.amount)
        .bind(&row.currency)
        .bind(&row.reference_id)
        .bind(&row.description)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(ApplyOutcome::RecipientMissing);
    }

    sqlx::query(
        r#"INSERT INTO transactions
           (id, from_account, to_account, amount, currency, status,
            reference_id, description, processed_at)
           VALUES ($1, $2, $3, $4, $5, 'completed', $6, $7, NOW())
           ON CONFLICT (reference_id, from_account) DO NOTHING"#,
    )
    .bind(Uuid::new_v4())
    .bind(&row.from_account)
    .bind(&row.to_account)
    .bind(row.amount)
    .bind(&row.currency)
    .bind(&row.reference_id)
    .bind(&row.description)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(ApplyOutcome::Applied)
}

/// Atomic compensating refund on the sender shard for a cross-shard
/// transaction whose recipient turned out to be missing/inactive.
///
/// Single CTE so balance and audit-row update commit together:
///   * audit row flips 'completed' → 'reversed' (gated on
///     `status != 'reversed'` so a redelivered refund is a no-op),
///   * balance UPDATE only fires when the audit update matched a
///     row, so we never refund twice.
///
/// Returns Ok(()) regardless of whether the refund actually moved
/// money — a no-op result means a previous attempt already
/// reversed the row.
async fn refund_sender(pool: &sqlx::PgPool, row: &OutboxRow) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        WITH audit AS (
            UPDATE transactions
            SET status = 'reversed', processed_at = NOW(), updated_at = NOW()
            WHERE reference_id = $1
              AND from_account = $2
              AND status <> 'reversed'
            RETURNING id
        )
        UPDATE users
        SET balance = balance + $3
        WHERE account_number = $2
          AND EXISTS (SELECT 1 FROM audit)
        "#,
    )
    .bind(&row.reference_id)
    .bind(&row.from_account)
    .bind(row.amount)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn mark_refund_required(pool: &sqlx::PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    // Clear the lease so the next tick can immediately re-claim
    // and run the refund path without waiting LEASE_SECS.
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET refund_required = TRUE, lease_until = NULL, updated_at = NOW() \
         WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_completed(pool: &sqlx::PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    // Guard on status='pending' so a racing instance doesn't stomp
    // a row that was already terminal-marked elsewhere.
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET status = 'completed', completed_at = NOW(), updated_at = NOW() \
         WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Flip the sender-side `transactions` audit row from
/// 'processing' to 'completed' once the cross-shard credit has
/// landed on the receiver. Guarded on `status = 'processing'` so
/// the update is a no-op against a row that is already terminal
/// — either a redelivered apply, or the refund path having
/// already moved the row to 'reversed'.
async fn mark_sender_completed(
    pool: &sqlx::PgPool,
    reference_id: &str,
    from_account: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE transactions \
         SET status = 'completed', processed_at = NOW(), updated_at = NOW() \
         WHERE reference_id = $1 AND from_account = $2 AND status = 'processing'",
    )
    .bind(reference_id)
    .bind(from_account)
    .execute(pool)
    .await?;
    Ok(())
}

/// Flip the sender-side `transactions` audit row from
/// 'processing' to 'failed' once the cross-shard credit has
/// exhausted its retry budget. Guarded on `status = 'processing'`
/// so a redelivered terminal-fail does not stomp a row that some
/// other code path already moved to a terminal state. `processed_at`
/// is set so the dashboard "time in 'processing'" metric stops
/// climbing for this row.
async fn mark_sender_failed(
    pool: &sqlx::PgPool,
    reference_id: &str,
    from_account: &str,
    reason: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE transactions \
         SET status = 'failed', failure_reason = $3, processed_at = NOW(), updated_at = NOW() \
         WHERE reference_id = $1 AND from_account = $2 AND status = 'processing'",
    )
    .bind(reference_id)
    .bind(from_account)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

async fn bump_attempt(pool: &sqlx::PgPool, id: Uuid, last_error: &str) -> Result<(), sqlx::Error> {
    // Clear lease so the row is reclaimable on the next tick.
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET attempts = attempts + 1, last_error = $2, \
             lease_until = NULL, updated_at = NOW() \
         WHERE id = $1",
    )
    .bind(id)
    .bind(last_error)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_failed(pool: &sqlx::PgPool, id: Uuid, reason: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET status = 'failed', last_error = $2, updated_at = NOW() \
         WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}
