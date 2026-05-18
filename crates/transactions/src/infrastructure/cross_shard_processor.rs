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
//! When a row exhausts `MAX_ATTEMPTS` it is marked 'failed' so it
//! drops out of the working set; previously it stuck at status
//! 'pending' forever and the recipient never got their money
//! (and, after this rewrite, the sender never got their refund).

use std::time::Duration;

use rust_decimal::Decimal;
use sqlx::FromRow;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::db::shard::ShardRouter;

const POLL_INTERVAL_MS: u64 = 250;
const BATCH_LIMIT: i64 = 100;
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

#[derive(FromRow)]
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
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::info!("cross-shard outbox processor started");
        let mut ticker = tokio::time::interval(Duration::from_millis(POLL_INTERVAL_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    for sender_shard in 0..shards.num_shards() {
                        if let Err(e) = drain_shard(&shards, sender_shard).await {
                            tracing::warn!(shard = sender_shard, error = %e, "outbox drain error");
                        }
                    }
                }
            }
        }
        tracing::info!("cross-shard outbox processor exiting");
    })
}

async fn drain_shard(shards: &ShardRouter, sender_shard: usize) -> Result<(), sqlx::Error> {
    let sender_pool = shards.writer(sender_shard);
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
    .bind(BATCH_LIMIT)
    .bind(LEASE_SECS)
    .fetch_all(sender_pool)
    .await?;

    for row in rows {
        // Each row's processing is independent — log+continue on
        // bookkeeping errors instead of `?`-ing out of the loop.
        // A single sender-pool hiccup used to abort the rest of
        // the drain and starve every other row this tick.
        let receiver_shard = row.to_shard as usize;
        if receiver_shard >= shards.num_shards() {
            best_effort(
                "mark_failed (invalid to_shard)",
                row.id,
                mark_failed(sender_pool, row.id, "invalid to_shard").await,
            );
            continue;
        }

        if row.refund_required {
            // Receiver-side credit could not land on a real account.
            // Refund the sender atomically + mark sender audit row
            // 'reversed'. The CTE makes the whole compensation
            // idempotent against redelivery / retry.
            match refund_sender(sender_pool, &row).await {
                Ok(()) => {
                    metrics::counter!("cross_shard_refund_applied_total").increment(1);
                    best_effort(
                        "mark_completed",
                        row.id,
                        mark_completed(sender_pool, row.id).await,
                    );
                }
                Err(e) => {
                    handle_attempt_error(sender_pool, &row, e.to_string(), "refund").await;
                }
            }
            continue;
        }

        let receiver_pool = shards.writer(receiver_shard);
        match apply_on_receiver(receiver_pool, sender_shard, &row).await {
            Ok(ApplyOutcome::Applied) => {
                metrics::counter!("cross_shard_credit_applied_total").increment(1);
                best_effort(
                    "mark_sender_completed",
                    row.id,
                    mark_sender_completed(sender_pool, &row.reference_id, &row.from_account).await,
                );
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
                best_effort(
                    "mark_completed",
                    row.id,
                    mark_completed(sender_pool, row.id).await,
                );
            }
            Ok(ApplyOutcome::RecipientMissing) => {
                metrics::counter!("cross_shard_credit_recipient_missing_total").increment(1);
                tracing::warn!(
                    outbox_id = %row.id,
                    from = %row.from_account,
                    to = %row.to_account,
                    "cross-shard recipient missing — flagging for sender refund"
                );
                best_effort(
                    "mark_refund_required",
                    row.id,
                    mark_refund_required(sender_pool, row.id).await,
                );
            }
            Err(e) => {
                handle_attempt_error(sender_pool, &row, e.to_string(), "credit").await;
            }
        }
    }
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
