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
use std::time::Duration;
use uuid::Uuid;

/// Per-shard wall-clock budget for the `find_by_id` fan-out. Single-row
/// PK lookups are sub-10 ms on healthy shards; 500 ms gives ~50× buffer
/// while bounding the tail when one shard is degraded so the call does
/// not pin the API-timeout (D-4).
const FIND_BY_ID_PER_SHARD_TIMEOUT: Duration = Duration::from_millis(500);

use shared_kernel::cache::redis::RedisCache;
use shared_kernel::db::shard::ShardRouter;

use super::super::domain::{
    IdempotencyAwareWriter, RepoError, ReserveOutcome, Transaction, TransactionRepository,
    TransactionStatus,
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
    async fn find_by_id(&self, id: TransactionId) -> Result<Option<Transaction>, RepoError> {
        // Cross-shard fan-out: spawn one query per shard, each wrapped
        // in `tokio::time::timeout(FIND_BY_ID_PER_SHARD_TIMEOUT, _)`
        // so a degraded shard turns into a per-shard error rather than
        // pinning the API-wide wall-clock (D-4). Surfacing infra errors
        // (one shard down) still matters — silently swallowing them
        // would return 404 for rows that genuinely live on the
        // unavailable shard. Awaiting all handles still applies; it
        // avoids the leak the first-hit-wins variant produced.
        let mut handles = Vec::with_capacity(self.shards.num_shards());
        for shard_idx in 0..self.shards.num_shards() {
            let pool = self.shards.reader(shard_idx).clone();
            let id = id.as_uuid();
            handles.push(tokio::spawn(async move {
                let res = tokio::time::timeout(FIND_BY_ID_PER_SHARD_TIMEOUT, async {
                    sqlx::query_as::<_, TransactionRowSlim>(
                        "SELECT id, from_account, to_account, amount, currency, status, reference_id, description, created_at, updated_at, processed_at FROM transactions WHERE id = $1",
                    )
                    .bind(id)
                    .fetch_optional(&pool)
                    .await
                })
                .await;
                (shard_idx, res)
            }));
        }
        let mut found: Option<Transaction> = None;
        let mut last_err: Option<String> = None;
        for h in handles {
            match h.await {
                Ok((_, Ok(Ok(Some(row))))) => {
                    if found.is_none() {
                        found = Some(row.into());
                    }
                }
                Ok((_, Ok(Ok(None)))) => {}
                Ok((_, Ok(Err(e)))) => last_err = Some(e.to_string()),
                Ok((shard_idx, Err(_elapsed))) => {
                    metrics::counter!(
                        "transactions_find_by_id_shard_timeout_total",
                        "shard" => shard_idx.to_string()
                    )
                    .increment(1);
                    last_err = Some(format!(
                        "shard {} exceeded {}ms find_by_id budget",
                        shard_idx,
                        FIND_BY_ID_PER_SHARD_TIMEOUT.as_millis()
                    ));
                }
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
            return Err(RepoError::Other(err));
        }
        Ok(None)
    }

    async fn list(&self, filter: &ListFilter) -> Result<Vec<Transaction>, RepoError> {
        let limit = filter.limit.min(100) as i64;
        // Two-phase cross-shard pagination. Each shard fetches its top
        // `limit + 1` rows. The `(limit+1)`th row (if present) is a
        // "tail probe" — the OLDEST row on that shard's slice that we
        // would have dropped on truncate. Across shards, the MAX of
        // those probes is the *safe cursor*: the oldest `(created_at,
        // id)` for which every shard's slice still has full coverage
        // newer than it. Any row in the merged page OLDER than the
        // safe cursor is in a range where at least one shard's
        // coverage is incomplete — drop it so the client's next-page
        // cursor (derived from the last returned row) is safe.
        //
        // Replaces the prior slack-based heuristic (`per_shard_limit =
        // limit + ceil(limit/N)`) that silently dropped tail rows
        // when a page bunched lopsidedly (D-3). Shards that exhaust
        // (return fewer than `limit + 1` rows) do not contribute a
        // probe — they cannot have more rows than they returned.
        let per_shard_limit = limit.saturating_add(1);
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

        let mut per_shard_slices: Vec<Vec<TransactionRowSlim>> =
            Vec::with_capacity(self.shards.num_shards());
        let mut last_err: Option<String> = None;
        for h in handles {
            match h.await {
                Ok(Ok(rs)) => per_shard_slices.push(rs),
                Ok(Err(e)) => last_err = Some(e.to_string()),
                Err(e) => last_err = Some(format!("join: {}", e)),
            }
        }

        // MAX (created_at, id) over the (limit+1)th row of each shard
        // that returned a full `limit + 1`. Tuple Ord is lexicographic
        // — matches our DESC sort key, so larger = newer.
        let safe_cursor: Option<(chrono::DateTime<Utc>, Uuid)> = per_shard_slices
            .iter()
            .filter_map(|slice| slice.get(limit as usize))
            .map(|row| (row.created_at, row.id))
            .max();

        // Each slice contributes its first `limit` rows (drop the
        // `+1` probe row). The probe was a tail signal, not page data.
        let mut rows: Vec<Transaction> = per_shard_slices
            .into_iter()
            .flat_map(|slice| {
                slice
                    .into_iter()
                    .take(limit as usize)
                    .map(Into::into)
                    .collect::<Vec<Transaction>>()
            })
            .collect();

        // Surface infra errors only when zero shards answered —
        // a partial result is still useful and the error is logged.
        if rows.is_empty() {
            if let Some(err) = last_err {
                return Err(RepoError::Other(err));
            }
        } else if let Some(err) = last_err {
            tracing::warn!(err = %err, "list: partial shard failure");
        }

        // Stable order: created_at DESC, id DESC (matches per-shard
        // ORDER BY so merging is consistent).
        rows.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        rows.truncate(limit as usize);

        // Drop rows older than the safe cursor — at least one shard
        // has un-fetched coverage there. When `safe_cursor` is None
        // (no shard returned a full `limit + 1`), the global result
        // fits within `limit` and no rows are unsafe.
        if let Some((sc_at, sc_id)) = safe_cursor {
            let pre_drop = rows.len();
            rows.retain(|r| (r.created_at, r.id) > (sc_at, sc_id));
            let dropped = pre_drop - rows.len();
            if dropped > 0 {
                metrics::counter!("transactions_list_tail_skew_dropped_total")
                    .increment(dropped as u64);
            }
        }

        Ok(rows)
    }

    async fn find_status_by_reference(
        &self,
        reference_id: &str,
    ) -> Result<Option<TransactionStatus>, RepoError> {
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
            return Err(RepoError::Other(err));
        }
        Ok(None)
    }

    async fn idempotency_exists_for_reference(
        &self,
        reference_id: &str,
    ) -> Result<bool, RepoError> {
        // The `idempotency_key` column is the unique
        // `txn:<shard_idx>:<reference_id>` composite. We don't
        // know which from_account drove the request, so the
        // exact key on each shard is the one whose prefix
        // matches that shard's own index. First-hit wins —
        // duplicates across shards would require the same
        // reference_id to have been used with from_accounts on
        // both shards, which is a degenerate case.
        let mut handles = Vec::with_capacity(self.shards.num_shards());
        for shard_idx in 0..self.shards.num_shards() {
            let pool = self.shards.reader(shard_idx).clone();
            let key = format!("txn:{}:{}", shard_idx, reference_id);
            handles.push(tokio::spawn(async move {
                sqlx::query_scalar::<_, i32>(
                    "SELECT 1 FROM idempotency_keys WHERE idempotency_key = $1",
                )
                .bind(key)
                .fetch_optional(&pool)
                .await
            }));
        }
        let mut last_err: Option<String> = None;
        for h in handles {
            match h.await {
                Ok(Ok(Some(_))) => return Ok(true),
                Ok(Ok(None)) => {}
                Ok(Err(e)) => last_err = Some(e.to_string()),
                Err(e) => last_err = Some(format!("join: {}", e)),
            }
        }
        // No shard hit. If any shard erred, surface the error so
        // a partial outage doesn't masquerade as a clean miss
        // (which would mean a 404 to the caller for a reference
        // we couldn't actually check).
        if let Some(err) = last_err {
            return Err(RepoError::Other(err));
        }
        Ok(false)
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
    response_payload: Option<serde_json::Value>,
    /// SQL-side `NOW() >= expires_at` — sidesteps app/DB clock drift.
    /// Comparing the raw timestamp against `Utc::now()` in the
    /// application would risk app-vs-DB clock skew producing
    /// premature/late revives.
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

impl SqlxIdempotencyWriter {
    /// Detach the cache populate so the hot path returns one Redis
    /// round-trip earlier. A dropped SET is not a correctness
    /// issue: the next replay falls through to the DB SELECT and
    /// repopulates from there.
    ///
    /// A-3: lower-stakes than the consumer size-flush spawn (no
    /// ACK durability hinges on it), so the lighter wrapper is
    /// sufficient — a `tracing::Instrument` span carries task
    /// identity through panics, and the attempt counter makes the
    /// path observable on the cache-write panel even when every
    /// SET succeeds. JoinSet wrap unnecessary because the result
    /// of the spawn does not influence any external state the
    /// caller awaits.
    fn spawn_cache_set(cache: RedisCache, key: String, entry: IdempotencyCacheEntry) {
        use tracing::Instrument;
        tokio::spawn(
            async move {
                metrics::counter!("idempotency_cache_set_attempts_total").increment(1);
                let _ = cache.set(&key, &entry, IDEMPOTENCY_TTL_SECS).await;
            }
            .instrument(tracing::info_span!("idempotency_cache_set")),
        );
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
        outbox_payload: &serde_json::Value,
    ) -> Result<ReserveOutcome, RepoError> {
        let writer = self.shards.writer(shard);

        // Optimistic INSERT first. The fresh path is ~95% of
        // steady-state traffic, so skipping the pre-INSERT cache
        // GET removes a Redis round-trip from the hot path. The
        // conflict path (~5%) probes the cache below to short-
        // circuit replay detection before the DB SELECT.
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
        .await?;

        if inserted.rows_affected() > 0 {
            Self::spawn_cache_set(
                self.cache.clone(),
                idempotency_key.to_string(),
                IdempotencyCacheEntry {
                    request_hash: request_hash.to_string(),
                    payload: accepted_payload.clone(),
                },
            );
            return Ok(ReserveOutcome::Reserved);
        }

        // Conflict path. Probe the cache to short-circuit a Replay
        // before paying the DB SELECT. The cached entry embeds the
        // original `request_hash` so a duplicate `idempotency_key`
        // carrying a different payload falls through to the DB and
        // surfaces `HashConflict` instead of silently replaying.
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

        // SQL-side `expired` projection sidesteps app-vs-DB clock
        // drift.
        let existing: Option<IdempotencyRowSlim> = sqlx::query_as(
            r#"
            SELECT request_hash,
                   response_payload,
                   (NOW() >= expires_at) AS expired
            FROM idempotency_keys
            WHERE idempotency_key = $1
            "#,
        )
        .bind(idempotency_key)
        .fetch_optional(writer)
        .await?;

        let Some(existing) = existing else {
            // Row vanished between INSERT and SELECT (cleanup race).
            // Surface as Infra so the caller retries fresh; treating
            // this as a clean reservation would risk two concurrent
            // callers both writing outbox rows for the same logical
            // transaction.
            return Err(RepoError::Other(
                "idempotency row vanished after INSERT conflict".to_string(),
            ));
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
            Self::spawn_cache_set(
                self.cache.clone(),
                idempotency_key.to_string(),
                IdempotencyCacheEntry {
                    request_hash: request_hash.to_string(),
                    payload: payload.clone(),
                },
            );
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
        .await?;

        if revived.rows_affected() > 0 {
            Self::spawn_cache_set(
                self.cache.clone(),
                idempotency_key.to_string(),
                IdempotencyCacheEntry {
                    request_hash: request_hash.to_string(),
                    payload: accepted_payload.clone(),
                },
            );
            return Ok(ReserveOutcome::Reserved);
        }

        // Lost the revive race. Re-fetch and replay the winner's
        // payload — caching ours would mask theirs.
        let winner: Option<IdempotencyRowSlim> = sqlx::query_as(
            r#"
            SELECT request_hash,
                   response_payload,
                   (NOW() >= expires_at) AS expired
            FROM idempotency_keys
            WHERE idempotency_key = $1
            "#,
        )
        .bind(idempotency_key)
        .fetch_optional(writer)
        .await?;
        let payload = winner
            .and_then(|w| w.response_payload)
            .unwrap_or_else(|| accepted_payload.clone());
        metrics::counter!("idempotency_hits_total").increment(1);
        Ok(ReserveOutcome::Replay(payload))
    }

    async fn reservation_exists_for_reference(
        &self,
        _reference_id: &str,
        _num_shards: usize,
    ) -> Result<bool, RepoError> {
        // Pure-PG backend never writes to the Redis idempotency
        // namespace. The PG-side check in
        // `TransactionRepository::idempotency_exists_for_reference`
        // is already authoritative for this backend.
        Ok(false)
    }
}
