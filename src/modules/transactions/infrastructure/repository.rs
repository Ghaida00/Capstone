//! sqlx-backed implementations of the domain traits.
//!
//! Two adapters live here:
//!   * `SqlxTransactionRepository` — read-side fan-out across
//!     shards for `find_by_id`, `list`, and
//!     `find_status_by_reference`.
//!   * `SqlxIdempotencyWriter`     — the create-time idempotency
//!     dance that spans Postgres + Redis.

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::FromRow;
use uuid::Uuid;

use crate::cache::redis::RedisCache;
use crate::db::shard::ShardRouter;

use super::super::domain::{
    IdempotencyAwareWriter, ReserveOutcome, Transaction, TransactionRepository, TransactionStatus,
};
use super::super::ports::{ListFilter, TransactionId};

// ─── Read-side repository ───────────────────────────────────

#[derive(FromRow)]
struct TransactionRowSlim {
    id: Uuid,
    from_account: String,
    to_account: String,
    amount: Decimal,
    currency: String,
    status: String,
    reference_id: Option<String>,
    description: Option<String>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
    processed_at: Option<chrono::DateTime<Utc>>,
}

impl From<TransactionRowSlim> for Transaction {
    fn from(r: TransactionRowSlim) -> Self {
        Transaction {
            id: r.id,
            from_account: r.from_account,
            to_account: r.to_account,
            amount: r.amount,
            currency: r.currency,
            status: r.status,
            reference_id: r.reference_id,
            description: r.description,
            created_at: r.created_at,
            updated_at: r.updated_at,
            processed_at: r.processed_at,
        }
    }
}

#[derive(FromRow)]
struct StatusRowSlim {
    reference_id: Option<String>,
    status: String,
    processed_at: Option<chrono::DateTime<Utc>>,
}

pub(crate) struct SqlxTransactionRepository {
    shards: ShardRouter,
}

impl SqlxTransactionRepository {
    pub(crate) fn new(shards: ShardRouter) -> Self {
        Self { shards }
    }
}

#[async_trait]
impl TransactionRepository for SqlxTransactionRepository {
    async fn find_by_id(&self, id: TransactionId) -> Result<Option<Transaction>, String> {
        // Cross-shard fan-out. Same pattern as the legacy
        // handler: spawn one query per shard, return the first
        // hit. Misses are silent so a query timeout on one shard
        // does not prevent another from answering.
        let mut handles = Vec::with_capacity(self.shards.num_shards());
        for shard_idx in 0..self.shards.num_shards() {
            let pool = self.shards.reader(shard_idx).clone();
            let id = id.as_uuid();
            handles.push(tokio::spawn(async move {
                sqlx::query_as::<_, TransactionRowSlim>(
                    "SELECT id, from_account, to_account, amount, currency, status, reference_id, description, created_at, updated_at, processed_at FROM transactions WHERE id = $1",
                )
                .bind(id)
                .fetch_optional(&pool)
                .await
            }));
        }
        for h in handles {
            if let Ok(Ok(Some(row))) = h.await {
                return Ok(Some(row.into()));
            }
        }
        Ok(None)
    }

    async fn list(&self, filter: &ListFilter) -> Result<Vec<Transaction>, String> {
        let limit = filter.limit.min(100) as i64;
        let cursor = filter.before;

        let mut handles = Vec::with_capacity(self.shards.num_shards());
        for shard_idx in 0..self.shards.num_shards() {
            let pool = self.shards.reader(shard_idx).clone();
            handles.push(tokio::spawn(async move {
                match cursor {
                    Some(before) => {
                        sqlx::query_as::<_, TransactionRowSlim>(
                            "SELECT id, from_account, to_account, amount, currency, status, reference_id, description, created_at, updated_at, processed_at FROM transactions WHERE created_at < $1 ORDER BY created_at DESC LIMIT $2",
                        )
                        .bind(before)
                        .bind(limit)
                        .fetch_all(&pool)
                        .await
                    }
                    None => {
                        sqlx::query_as::<_, TransactionRowSlim>(
                            "SELECT id, from_account, to_account, amount, currency, status, reference_id, description, created_at, updated_at, processed_at FROM transactions ORDER BY created_at DESC LIMIT $1",
                        )
                        .bind(limit)
                        .fetch_all(&pool)
                        .await
                    }
                }
            }));
        }

        let mut rows: Vec<Transaction> = Vec::new();
        for h in handles {
            if let Ok(Ok(rs)) = h.await {
                rows.extend(rs.into_iter().map(Into::into));
            }
        }
        rows.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn find_status_by_reference(
        &self,
        reference_id: &str,
    ) -> Result<Option<TransactionStatus>, String> {
        let mut handles = Vec::with_capacity(self.shards.num_shards());
        for shard_idx in 0..self.shards.num_shards() {
            let pool = self.shards.reader(shard_idx).clone();
            let ref_id = reference_id.to_owned();
            handles.push(tokio::spawn(async move {
                sqlx::query_as::<_, StatusRowSlim>(
                    "SELECT reference_id, status, processed_at FROM transactions WHERE reference_id = $1",
                )
                .bind(ref_id)
                .fetch_optional(&pool)
                .await
            }));
        }
        for h in handles {
            if let Ok(Ok(Some(row))) = h.await {
                return Ok(Some(TransactionStatus {
                    reference_id: row.reference_id.unwrap_or_default(),
                    status: row.status,
                    processed_at: row.processed_at,
                }));
            }
        }
        Ok(None)
    }
}

// ─── Idempotency writer ─────────────────────────────────────

#[derive(FromRow)]
struct IdempotencyRowSlim {
    request_hash: String,
    status: String,
    response_payload: Option<serde_json::Value>,
    expires_at: chrono::DateTime<Utc>,
}

pub(crate) struct SqlxIdempotencyWriter {
    shards: ShardRouter,
    cache: RedisCache,
}

impl SqlxIdempotencyWriter {
    pub(crate) fn new(shards: ShardRouter, cache: RedisCache) -> Self {
        Self { shards, cache }
    }
}

#[async_trait]
impl IdempotencyAwareWriter for SqlxIdempotencyWriter {
    async fn reserve(
        &self,
        shard: usize,
        idempotency_key: &str,
        request_hash: &str,
        accepted_payload: &serde_json::Value,
    ) -> Result<ReserveOutcome, String> {
        let writer = self.shards.writer(shard);

        // Fast-path: Redis cache for already-accepted responses.
        // Mirrors legacy behaviour and short-circuits the DB
        // before we touch idempotency_keys.
        if let Ok(Some(cached)) = self.cache.get::<serde_json::Value>(idempotency_key).await {
            return Ok(ReserveOutcome::Replay(cached));
        }

        // Try to claim the row.
        let inserted = sqlx::query(
            r#"
            INSERT INTO idempotency_keys (
                idempotency_key, request_hash, status, response_payload, expires_at
            )
            VALUES ($1, $2, 'processing', $3, NOW() + INTERVAL '24 hours')
            ON CONFLICT (idempotency_key) DO NOTHING
            "#,
        )
        .bind(idempotency_key)
        .bind(request_hash)
        .bind(accepted_payload)
        .execute(writer)
        .await
        .map_err(|e| e.to_string())?;

        if inserted.rows_affected() > 0 {
            // First-mover. Cache the accepted payload at the
            // legacy 24h TTL so duplicate v1/v2 replays hit
            // Redis directly.
            let _ = self.cache.set(idempotency_key, accepted_payload, 86400).await;
            return Ok(ReserveOutcome::Reserved);
        }

        // Row already exists — examine it.
        let existing: Option<IdempotencyRowSlim> = sqlx::query_as(
            "SELECT request_hash, status, response_payload, expires_at FROM idempotency_keys WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_optional(writer)
        .await
        .map_err(|e| e.to_string())?;

        let Some(existing) = existing else {
            // Race: row vanished between INSERT and SELECT.
            // Treat as a fresh reservation; legacy code did the
            // same.
            return Ok(ReserveOutcome::Reserved);
        };

        if existing.request_hash != request_hash {
            return Ok(ReserveOutcome::HashConflict);
        }

        // Same hash. Decide replay vs revive.
        if matches!(existing.status.as_str(), "processing" | "completed" | "pending") {
            let payload = existing
                .response_payload
                .unwrap_or_else(|| accepted_payload.clone());
            let _ = self.cache.set(idempotency_key, &payload, 86400).await;
            return Ok(ReserveOutcome::Replay(payload));
        }

        if existing.status == "failed" || existing.expires_at <= Utc::now() {
            // Revive failed or expired reservation. Best-effort:
            // if the UPDATE matches no rows another worker won
            // the race; fall through to replay anyway.
            let _ = sqlx::query(
                r#"
                UPDATE idempotency_keys
                SET status = 'processing',
                    response_payload = $2,
                    expires_at = NOW() + INTERVAL '24 hours',
                    updated_at = NOW()
                WHERE idempotency_key = $1
                  AND (status = 'failed' OR expires_at <= NOW())
                "#,
            )
            .bind(idempotency_key)
            .bind(accepted_payload)
            .execute(writer)
            .await;

            let _ = self.cache.set(idempotency_key, accepted_payload, 86400).await;
            return Ok(ReserveOutcome::Reserved);
        }

        // Unknown state — treat as replay (safest).
        let _ = self.cache.set(idempotency_key, accepted_payload, 86400).await;
        Ok(ReserveOutcome::Replay(accepted_payload.clone()))
    }
}
