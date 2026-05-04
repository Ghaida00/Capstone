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
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;

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
        // Cross-shard fan-out: spawn one query per shard, await
        // ALL of them. Surfacing infra errors (one shard down) is
        // important — silently swallowing them would return 404
        // for rows that genuinely live on the unavailable shard.
        // Awaiting all handles also avoids the leak the
        // first-hit-wins variant produced.
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
        let mut found: Option<Transaction> = None;
        let mut last_err: Option<String> = None;
        for h in handles {
            match h.await {
                Ok(Ok(Some(row))) => {
                    if found.is_none() {
                        found = Some(row.into());
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => last_err = Some(e.to_string()),
                Err(e) => last_err = Some(format!("join: {}", e)),
            }
        }
        if found.is_some() {
            // At least one shard answered with the row. A partial
            // failure on another shard is logged but doesn't mask
            // the hit.
            if let Some(err) = last_err {
                tracing::warn!(err = %err, "find_by_id: partial shard failure");
            }
            return Ok(found);
        }
        if let Some(err) = last_err {
            return Err(err);
        }
        Ok(None)
    }

    async fn list(&self, filter: &ListFilter) -> Result<Vec<Transaction>, String> {
        let limit = filter.limit.min(100) as i64;
        // Per-shard fetch budget = `limit + ceil(limit / num_shards)`,
        // floored at `limit + 2`.
        //
        // Each shard returns its top `per_shard_limit` rows; the
        // caller merges and truncates the union to `limit`. The
        // slack term absorbs ordinary inter-shard skew so the
        // merge step does not drop a row whose `created_at` lands
        // older than another shard's tail. Pages that bunch
        // entirely on one shard can still lose a tail row at the
        // page boundary; the next page's cursor recovers it on
        // the following call.
        //
        // Invariant: `per_shard_limit >= limit` so a single-shard
        // result set is never under-fetched.
        let num_shards = self.shards.num_shards() as i64;
        let per_shard_slack = (limit / num_shards.max(1)).max(2);
        let per_shard_limit = limit.saturating_add(per_shard_slack);
        let cursor = filter.before;
        let cursor_id = filter.before_id;

        let mut handles = Vec::with_capacity(self.shards.num_shards());
        for shard_idx in 0..self.shards.num_shards() {
            let pool = self.shards.reader(shard_idx).clone();
            handles.push(tokio::spawn(async move {
                match (cursor, cursor_id) {
                    // Tuple cursor — stable across rows with equal
                    // `created_at`. ORDER BY mirrors the cursor
                    // expression so the merge-and-truncate step in
                    // the caller produces the same total order each
                    // page.
                    (Some(before), Some(before_id)) => {
                        sqlx::query_as::<_, TransactionRowSlim>(
                            "SELECT id, from_account, to_account, amount, currency, status, reference_id, description, created_at, updated_at, processed_at \
                             FROM transactions \
                             WHERE (created_at, id) < ($1, $2) \
                             ORDER BY created_at DESC, id DESC LIMIT $3",
                        )
                        .bind(before)
                        .bind(before_id)
                        .bind(per_shard_limit)
                        .fetch_all(&pool)
                        .await
                    }
                    (Some(before), None) => {
                        sqlx::query_as::<_, TransactionRowSlim>(
                            "SELECT id, from_account, to_account, amount, currency, status, reference_id, description, created_at, updated_at, processed_at FROM transactions WHERE created_at < $1 ORDER BY created_at DESC, id DESC LIMIT $2",
                        )
                        .bind(before)
                        .bind(per_shard_limit)
                        .fetch_all(&pool)
                        .await
                    }
                    (None, _) => {
                        sqlx::query_as::<_, TransactionRowSlim>(
                            "SELECT id, from_account, to_account, amount, currency, status, reference_id, description, created_at, updated_at, processed_at FROM transactions ORDER BY created_at DESC, id DESC LIMIT $1",
                        )
                        .bind(per_shard_limit)
                        .fetch_all(&pool)
                        .await
                    }
                }
            }));
        }

        let mut rows: Vec<Transaction> = Vec::new();
        let mut last_err: Option<String> = None;
        for h in handles {
            match h.await {
                Ok(Ok(rs)) => rows.extend(rs.into_iter().map(Into::into)),
                Ok(Err(e)) => last_err = Some(e.to_string()),
                Err(e) => last_err = Some(format!("join: {}", e)),
            }
        }
        // Surface infra errors only when zero shards answered —
        // a partial result is still useful and the error is logged.
        if rows.is_empty() {
            if let Some(err) = last_err {
                return Err(err);
            }
        } else if let Some(err) = last_err {
            tracing::warn!(err = %err, "list: partial shard failure");
        }
        // Stable order: created_at DESC, id DESC (matches per-shard
        // ORDER BY so merging is consistent).
        rows.sort_by(|a, b| b.created_at.cmp(&a.created_at).then_with(|| b.id.cmp(&a.id)));
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn find_status_by_reference(
        &self,
        reference_id: &str,
    ) -> Result<Option<TransactionStatus>, String> {
        // Cross-shard transactions can produce TWO rows for the
        // same reference_id — one on the sender shard (consumer
        // bulk-insert) and one on the receiver shard (cross-shard
        // processor audit row). Status may diverge: e.g. sender
        // 'reversed' / receiver 'failed' for the recipient-missing
        // path. Fetch up to one row per shard with `fetch_all`
        // bounded to 1, collect across shards, then pick the most
        // recently processed row so the user always sees the
        // freshest decision. Earlier code used `fetch_optional`
        // and returned the first shard's response nondeterministically.
        let mut handles = Vec::with_capacity(self.shards.num_shards());
        for shard_idx in 0..self.shards.num_shards() {
            let pool = self.shards.reader(shard_idx).clone();
            let ref_id = reference_id.to_owned();
            handles.push(tokio::spawn(async move {
                sqlx::query_as::<_, StatusRowSlim>(
                    "SELECT reference_id, status, processed_at FROM transactions WHERE reference_id = $1 ORDER BY processed_at DESC NULLS LAST LIMIT 1",
                )
                .bind(ref_id)
                .fetch_optional(&pool)
                .await
            }));
        }
        let mut candidates: Vec<TransactionStatus> = Vec::new();
        let mut last_err: Option<String> = None;
        for h in handles {
            match h.await {
                Ok(Ok(Some(row))) => {
                    candidates.push(TransactionStatus {
                        reference_id: row.reference_id.unwrap_or_default(),
                        status: row.status,
                        processed_at: row.processed_at,
                    });
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => last_err = Some(e.to_string()),
                Err(e) => last_err = Some(format!("join: {}", e)),
            }
        }
        if !candidates.is_empty() {
            if let Some(err) = &last_err {
                tracing::warn!(err = %err, "find_status_by_reference: partial shard failure");
            }
            // Prefer the row with the latest processed_at; ties
            // broken by status priority (reversed > failed >
            // completed > processing > pending) so the most
            // informative terminal state wins deterministically.
            candidates.sort_by(|a, b| {
                b.processed_at
                    .cmp(&a.processed_at)
                    .then_with(|| status_priority(&b.status).cmp(&status_priority(&a.status)))
            });
            return Ok(candidates.into_iter().next());
        }
        if let Some(err) = last_err {
            return Err(err);
        }
        Ok(None)
    }
}

fn status_priority(s: &str) -> u8 {
    match s {
        "reversed" => 5,
        "failed" => 4,
        "completed" => 3,
        "processing" => 2,
        "pending" => 1,
        _ => 0,
    }
}

// ─── Idempotency writer ─────────────────────────────────────

#[derive(FromRow)]
struct IdempotencyRowSlim {
    request_hash: String,
    /// Decoded but never branched on: `idempotency_keys.status` is
    /// always `'processing'` once the producer has reserved (the
    /// consumer never writes back to this table). Kept on the row
    /// for observability dumps and to keep the SELECT projection
    /// stable across migrations.
    
    status: String,
    response_payload: Option<serde_json::Value>,
    /// SQL-side `NOW() >= expires_at` is the only thing the
    /// caller branches on; the raw timestamp stays for logs.
    /// Comparing `expires_at` against `Utc::now()` in the
    /// application would risk app-vs-DB clock skew producing
    /// premature/late revives.
    
    expires_at: chrono::DateTime<Utc>,
    /// SQL-side `NOW() >= expires_at` — sidesteps app/DB clock drift.
    expired: bool,
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

const IDEMPOTENCY_TTL_SECS: u64 = 86_400;

/// Cached idempotency entry. Stores the request_hash alongside the
/// accepted response so the cache fast-path can verify equality
/// before replaying — the hash-blind cache GET previously here let
/// a duplicate idempotency_key with a DIFFERENT payload return the
/// original "accepted" response without ever reaching the queue,
/// silently masking a payload-conflict the DB path would have
/// surfaced as `HashConflict`.
#[derive(Debug, Serialize, Deserialize)]
struct IdempotencyCacheEntry {
    request_hash: String,
    payload: serde_json::Value,
}

#[async_trait]
impl IdempotencyAwareWriter for SqlxIdempotencyWriter {
    async fn reserve(
        &self,
        shard: usize,
        idempotency_key: &str,
        request_hash: &str,
        accepted_payload: &serde_json::Value,
        outbox_payload: &serde_json::Value,
    ) -> Result<ReserveOutcome, String> {
        let writer = self.shards.writer(shard);

        // Fast-path: Redis cache for already-accepted responses.
        // The cached entry embeds the original `request_hash` so a
        // duplicate `idempotency_key` carrying a different payload
        // falls through to the DB and surfaces `HashConflict`
        // instead of silently replaying.
        match self
            .cache
            .get::<IdempotencyCacheEntry>(idempotency_key)
            .await
        {
            Ok(Some(cached)) => {
                if cached.request_hash == request_hash {
                    metrics::counter!("idempotency_hits_total").increment(1);
                    return Ok(ReserveOutcome::Replay(cached.payload));
                }
                let _ = self.cache.delete(idempotency_key).await;
                metrics::counter!("idempotency_cache_hash_mismatch_total").increment(1);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, key = %idempotency_key, "idempotency cache get failed");
            }
        }

        // Atomic first-mover claim. Writes both the response and
        // the outbox payload in the same row so commit makes the
        // queue message durable before this function returns.
        let inserted = sqlx::query(
            r#"
            INSERT INTO idempotency_keys (
                idempotency_key, request_hash, status,
                response_payload, outbox_payload,
                expires_at, published
            )
            VALUES ($1, $2, 'processing', $3, $4,
                    NOW() + INTERVAL '24 hours', false)
            ON CONFLICT (idempotency_key) DO NOTHING
            "#,
        )
        .bind(idempotency_key)
        .bind(request_hash)
        .bind(accepted_payload)
        .bind(outbox_payload)
        .execute(writer)
        .await
        .map_err(|e| e.to_string())?;

        if inserted.rows_affected() > 0 {
            let entry = IdempotencyCacheEntry {
                request_hash: request_hash.to_string(),
                payload: accepted_payload.clone(),
            };
            let _ = self
                .cache
                .set(idempotency_key, &entry, IDEMPOTENCY_TTL_SECS)
                .await;
            return Ok(ReserveOutcome::Reserved);
        }

        // Conflict path. SQL-side `expired` projection sidesteps
        // app-vs-DB clock drift.
        let existing: Option<IdempotencyRowSlim> = sqlx::query_as(
            r#"
            SELECT request_hash,
                   status,
                   response_payload,
                   expires_at,
                   (NOW() >= expires_at) AS expired
            FROM idempotency_keys
            WHERE idempotency_key = $1
            "#,
        )
        .bind(idempotency_key)
        .fetch_optional(writer)
        .await
        .map_err(|e| e.to_string())?;

        let Some(existing) = existing else {
            // Row vanished between INSERT and SELECT (cleanup race).
            // Surface as Infra so the caller retries fresh; treating
            // this as a clean reservation would risk two concurrent
            // callers both writing outbox rows for the same logical
            // transaction.
            return Err("idempotency row vanished after INSERT conflict".to_string());
        };

        if existing.request_hash != request_hash {
            return Ok(ReserveOutcome::HashConflict);
        }

        if !existing.expired {
            // Same key, same hash, still live. Replay the stored
            // accepted payload. The outbox row already exists and
            // either has been or will be published by the worker.
            let payload = existing
                .response_payload
                .unwrap_or_else(|| accepted_payload.clone());
            let entry = IdempotencyCacheEntry {
                request_hash: request_hash.to_string(),
                payload: payload.clone(),
            };
            let _ = self
                .cache
                .set(idempotency_key, &entry, IDEMPOTENCY_TTL_SECS)
                .await;
            metrics::counter!("idempotency_hits_total").increment(1);
            return Ok(ReserveOutcome::Replay(payload));
        }

        // Expired. Revive in-place: rewrite the outbox payload and
        // mark `published=false` so the worker re-emits the current
        // caller's intent. The condition keeps the UPDATE atomic
        // against a concurrent revive winner.
        let revived = sqlx::query(
            r#"
            UPDATE idempotency_keys
            SET status = 'processing',
                response_payload = $2,
                outbox_payload = $3,
                published = false,
                published_at = NULL,
                expires_at = NOW() + INTERVAL '24 hours',
                updated_at = NOW()
            WHERE idempotency_key = $1
              AND NOW() >= expires_at
            "#,
        )
        .bind(idempotency_key)
        .bind(accepted_payload)
        .bind(outbox_payload)
        .execute(writer)
        .await
        .map_err(|e| e.to_string())?;

        if revived.rows_affected() > 0 {
            let entry = IdempotencyCacheEntry {
                request_hash: request_hash.to_string(),
                payload: accepted_payload.clone(),
            };
            let _ = self
                .cache
                .set(idempotency_key, &entry, IDEMPOTENCY_TTL_SECS)
                .await;
            return Ok(ReserveOutcome::Reserved);
        }

        // Lost the revive race. Re-fetch and replay the winner's
        // payload — caching ours would mask theirs.
        let winner: Option<IdempotencyRowSlim> = sqlx::query_as(
            r#"
            SELECT request_hash,
                   status,
                   response_payload,
                   expires_at,
                   (NOW() >= expires_at) AS expired
            FROM idempotency_keys
            WHERE idempotency_key = $1
            "#,
        )
        .bind(idempotency_key)
        .fetch_optional(writer)
        .await
        .map_err(|e| e.to_string())?;
        let payload = winner
            .and_then(|w| w.response_payload)
            .unwrap_or_else(|| accepted_payload.clone());
        metrics::counter!("idempotency_hits_total").increment(1);
        Ok(ReserveOutcome::Replay(payload))
    }
}
