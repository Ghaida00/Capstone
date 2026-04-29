//! Drains `cross_shard_outbox` rows on each shard, applies the
//! credit on the receiver shard atomically (dedupe via
//! `cross_shard_outbox_applied`), inserts the receiver-side
//! `transactions` audit row, then marks the sender-side outbox
//! row 'completed'.
//!
//! Idempotent: re-runs of the same outbox row are no-ops because
//! the receiver-side INSERT into `cross_shard_outbox_applied` has
//! a primary key on (sender_shard, outbox_id).

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
    let rows: Vec<OutboxRow> = sqlx::query_as(
        r#"
        SELECT id, from_account, to_account, to_shard, amount, currency,
               reference_id, description, attempts
        FROM cross_shard_outbox
        WHERE status = 'pending' AND attempts < $1
        ORDER BY created_at
        LIMIT $2
        "#,
    )
    .bind(MAX_ATTEMPTS)
    .bind(BATCH_LIMIT)
    .fetch_all(sender_pool)
    .await?;

    for row in rows {
        let receiver_shard = row.to_shard as usize;
        if receiver_shard >= shards.num_shards() {
            mark_failed(sender_pool, row.id, "invalid to_shard").await?;
            continue;
        }
        let receiver_pool = shards.writer(receiver_shard);
        match apply_on_receiver(receiver_pool, sender_shard, &row).await {
            Ok(applied) => {
                mark_completed(sender_pool, row.id).await?;
                if applied {
                    metrics::counter!("cross_shard_credit_applied_total").increment(1);
                } else {
                    // Already applied on a previous attempt — outbox row
                    // was 'pending' due to a sender-side update failure.
                    metrics::counter!("cross_shard_credit_redundant_total").increment(1);
                }
            }
            Err(e) => {
                let msg = e.to_string();
                bump_attempt(sender_pool, row.id, &msg).await?;
                metrics::counter!("cross_shard_credit_failures_total").increment(1);
                tracing::warn!(
                    outbox_id = %row.id,
                    attempts = row.attempts + 1,
                    error = %msg,
                    "cross-shard credit attempt failed"
                );
            }
        }
    }
    Ok(())
}

async fn apply_on_receiver(
    pool: &sqlx::PgPool,
    sender_shard: usize,
    row: &OutboxRow,
) -> Result<bool, sqlx::Error> {
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
        return Ok(false);
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
        // Recipient missing/inactive — record audit row as 'failed'
        // so the receiver's history reflects the attempt.
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
        metrics::counter!("cross_shard_credit_recipient_missing_total").increment(1);
        return Ok(true);
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
    Ok(true)
}

async fn mark_completed(pool: &sqlx::PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET status = 'completed', completed_at = NOW(), updated_at = NOW() \
         WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn bump_attempt(pool: &sqlx::PgPool, id: Uuid, last_error: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE cross_shard_outbox \
         SET attempts = attempts + 1, last_error = $2, updated_at = NOW() \
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
         WHERE id = $1",
    )
    .bind(id)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}
