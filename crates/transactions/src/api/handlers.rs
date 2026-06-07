//! HTTP handlers for `/api/v2/transactions/*`.
//!
//! Each handler is thin: extract → port call → map.
//! `BalanceResponse`-style wire compatibility: every JSON shape
//! in this file is intentionally byte-identical to the legacy
//! handlers under `src/api/handlers.rs`.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use shared_kernel::error::{AppError, AppResult};
use shared_kernel::responses::ApiResponse;

use super::super::infrastructure::TransactionsDeps;
use super::super::ports::{
    CreateTransactionInput, ListFilter, TransactionAccepted, TransactionError, TransactionId,
    TransactionStatusView, TransactionView,
};

// ─── Wire DTOs ──────────────────────────────────────────────
// Shape is preserved from the pre-cull v1 endpoint so clients
// see no JSON diff across the cull.

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    pub from_account: String,
    pub to_account: String,
    pub amount: Decimal,
    #[serde(default = "default_currency")]
    pub currency: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
}

fn default_currency() -> String {
    "IDR".into()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct AcceptedResponse {
    pub reference_id: String,
    pub status: String,
    pub message: String,
}

impl From<TransactionAccepted> for AcceptedResponse {
    fn from(a: TransactionAccepted) -> Self {
        Self {
            reference_id: a.reference_id,
            status: a.status,
            message: a.message,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TransactionResponseV2 {
    pub id: Uuid,
    pub from_account: String,
    pub to_account: String,
    pub amount: String,
    pub currency: String,
    pub status: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

impl From<TransactionView> for TransactionResponseV2 {
    fn from(v: TransactionView) -> Self {
        Self {
            id: v.id,
            from_account: v.from_account,
            to_account: v.to_account,
            amount: v.amount,
            currency: v.currency,
            status: v.status,
            reference_id: v.reference_id,
            description: v.description,
            created_at: v.created_at,
            updated_at: v.updated_at,
            processed_at: v.processed_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct StatusResponseV2 {
    pub reference_id: String,
    pub status: String,
    pub processed_at: Option<DateTime<Utc>>,
}

impl From<TransactionStatusView> for StatusResponseV2 {
    fn from(v: TransactionStatusView) -> Self {
        Self {
            reference_id: v.reference_id,
            status: v.status,
            processed_at: v.processed_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListParams {
    pub limit: Option<u32>,
    pub before: Option<String>,
    /// Optional second half of the keyset cursor. Pair with
    /// `before` to disambiguate rows that share a `created_at`
    /// timestamp — without it, ties at a page boundary either
    /// drop or duplicate rows depending on which row was the
    /// last on the previous page.
    pub before_id: Option<Uuid>,
    /// Accepted on the wire only so we can return a clear error
    /// when present. The endpoint is cursor-paginated (`before`)
    /// and never honoured `offset` correctly across shards —
    /// silently ignoring it would have clients page over the same
    /// rows forever.
    pub offset: Option<u32>,
}

// ─── Error mapping ─────────────────────────────────────────

impl From<TransactionError> for AppError {
    fn from(err: TransactionError) -> Self {
        match err {
            TransactionError::NotFound(m) => {
                AppError::NotFound(format!("Transaction {} not found", m))
            }
            TransactionError::Validation(m) => AppError::BadRequest(m),
            TransactionError::IdempotencyConflict(m) => AppError::BadRequest(m),
            TransactionError::Infra(m) => AppError::Internal(m),
        }
    }
}

// ─── Handlers ──────────────────────────────────────────────

pub(crate) async fn create(
    State(deps): State<TransactionsDeps>,
    headers: HeaderMap,
    Json(req): Json<CreateRequest>,
) -> AppResult<(StatusCode, Json<ApiResponse<AcceptedResponse>>)> {
    // Pull the request id from the standard tracing header so the
    // span on the queue side can be joined with the originating
    // HTTP request. Earlier versions hard-coded `None` and the
    // consumer log lines were orphaned from the API log lines.
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let input = CreateTransactionInput {
        from_account: req.from_account,
        to_account: req.to_account,
        amount: req.amount,
        currency: req.currency,
        reference_id: req.reference_id,
        description: req.description,
        request_id,
        traceparent: shared_kernel::queue::trace_propagation::current_traceparent(),
    };
    let accepted = deps.service.create(input).await?;
    metrics::counter!("transactions_created_total").increment(1);
    Ok((
        StatusCode::ACCEPTED,
        Json(ApiResponse::success(accepted.into())),
    ))
}

pub(crate) async fn get_by_id(
    State(deps): State<TransactionsDeps>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<ApiResponse<TransactionResponseV2>>> {
    // Cache parity with legacy: same key, same TTL.
    let cache_key = format!(
        "{}:txn:{}",
        shared_kernel::cache::redis::CACHE_KEY_VERSION,
        id
    );
    if let Some(cached) = deps.cache.get::<TransactionResponseV2>(&cache_key).await? {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }
    metrics::counter!("cache_misses_total").increment(1);

    let view = deps.service.get_by_id(TransactionId(id)).await?;
    let resp: TransactionResponseV2 = view.into();
    // Short TTL bounds worst-case staleness if the cache
    // invalidator subscriber misses an event.
    let _ = deps.cache.set(&cache_key, &resp, 30).await;
    Ok(Json(ApiResponse::success(resp)))
}

pub(crate) async fn list(
    State(deps): State<TransactionsDeps>,
    Query(params): Query<ListParams>,
) -> AppResult<Json<ApiResponse<Vec<TransactionResponseV2>>>> {
    if params.offset.is_some() {
        return Err(AppError::BadRequest(
            "offset is not supported; use the `before` cursor for pagination".into(),
        ));
    }
    let limit = params.limit.unwrap_or(20).min(100);
    let cursor: Option<DateTime<Utc>> = params.before.as_deref().and_then(|s| {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });
    let cursor_id = params.before_id;

    // Cursor timestamp is floored to `CACHE_BUCKET_SECS` before
    // going into the cache key. All requests whose `before`
    // cursor falls in the same bucket share a cache entry. This
    // caps staleness at the bucket size and lets near-simultaneous
    // pagers reuse one another's results. `cursor_id` is left
    // un-bucketed: it is already discrete and acts as the
    // tie-breaker for rows sharing a `created_at`, so two callers
    // with the same time bucket but different ids legitimately
    // want different pages.
    const CACHE_BUCKET_SECS: i64 = 5;
    let cursor_bucket = cursor.map(|c| {
        // `div_euclid` floors toward negative infinity so all
        // cursors in `[bucket, bucket+CACHE_BUCKET_SECS)` map to
        // the same key, including the pre-epoch case.
        let ts = c.timestamp();
        ts.div_euclid(CACHE_BUCKET_SECS) * CACHE_BUCKET_SECS
    });

    let cache_key = format!(
        "{}:txn_list:{}:{}:{}",
        shared_kernel::cache::redis::CACHE_KEY_VERSION,
        limit,
        cursor_bucket.map_or("latest".to_string(), |b| b.to_string()),
        cursor_id.map_or("none".to_string(), |id| id.to_string()),
    );
    if let Ok(Some(cached)) = deps
        .cache
        .get::<Vec<TransactionResponseV2>>(&cache_key)
        .await
    {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }
    metrics::counter!("cache_misses_total").increment(1);

    let views = deps
        .service
        .list(ListFilter {
            limit,
            before: cursor,
            before_id: cursor_id,
        })
        .await?;
    let resp: Vec<TransactionResponseV2> = views.into_iter().map(Into::into).collect();
    // 30 s amortises the cross-shard fan-out across the bucket
    // window. Worst-case staleness for a caller is
    // `CACHE_BUCKET_SECS + TTL` — the bucket bounds how out-of-date
    // the cached page can be relative to the caller's cursor, the
    // TTL bounds how long a single cached entry lives.
    let _ = deps.cache.set(&cache_key, &resp, 30).await;
    Ok(Json(ApiResponse::success(resp)))
}

pub(crate) async fn get_status(
    State(deps): State<TransactionsDeps>,
    Path(reference_id): Path<String>,
) -> AppResult<Json<ApiResponse<StatusResponseV2>>> {
    let cache_key = format!(
        "{}:tx_status:{}",
        shared_kernel::cache::redis::CACHE_KEY_VERSION,
        reference_id
    );
    if let Some(cached) = deps.cache.get::<StatusResponseV2>(&cache_key).await? {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }
    metrics::counter!("cache_misses_total").increment(1);

    let view = deps.service.get_status_by_reference(&reference_id).await?;
    let resp: StatusResponseV2 = view.into();
    // Cache ONLY terminal statuses. 'processing'/'pending' are transient:
    // a poll that reads the in-flight status can re-populate the cache
    // just after the commit-time invalidator DELs it, pinning the client
    // to the stale value for the full TTL. For a cross-shard tx — which
    // sits in 'processing' for ~1s while the outbox drains — that stale
    // window dominates the observed end-to-end latency even though the row
    // is already completed in the DB. Terminal statuses are immutable, so
    // caching them is safe and still absorbs the bulk of post-completion
    // lookups; transient polls fall through to an indexed replica read,
    // which is cheap. Whitelist (not blacklist) so an unrecognised status
    // defaults to not-cached.
    if matches!(resp.status.as_str(), "completed" | "failed" | "reversed") {
        let _ = deps.cache.set(&cache_key, &resp, 5).await;
    }
    Ok(Json(ApiResponse::success(resp)))
}
