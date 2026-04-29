//! HTTP handlers for `/api/v2/transactions/*`.
//!
//! Each handler is thin: extract → port call → map.
//! `BalanceResponse`-style wire compatibility: every JSON shape
//! in this file is intentionally byte-identical to the legacy
//! handlers under `src/api/handlers.rs`.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
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
    #[allow(dead_code)]
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
    Json(req): Json<CreateRequest>,
) -> AppResult<(StatusCode, Json<ApiResponse<AcceptedResponse>>)> {
    let input = CreateTransactionInput {
        from_account: req.from_account,
        to_account: req.to_account,
        amount: req.amount,
        currency: req.currency,
        reference_id: req.reference_id,
        description: req.description,
        request_id: None,
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
    let cache_key = format!("txn:{}", id);
    if let Some(cached) = deps.cache.get::<TransactionResponseV2>(&cache_key).await? {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }
    metrics::counter!("cache_misses_total").increment(1);

    let view = deps.service.get_by_id(TransactionId(id)).await?;
    let resp: TransactionResponseV2 = view.into();
    let _ = deps.cache.set(&cache_key, &resp, 300).await;
    Ok(Json(ApiResponse::success(resp)))
}

pub(crate) async fn list(
    State(deps): State<TransactionsDeps>,
    Query(params): Query<ListParams>,
) -> AppResult<Json<ApiResponse<Vec<TransactionResponseV2>>>> {
    let limit = params.limit.unwrap_or(20).min(100);
    let cursor: Option<DateTime<Utc>> = params.before.as_deref().and_then(|s| {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    let cache_key = format!(
        "txn_list:{}:{}",
        limit,
        cursor.map_or("latest".to_string(), |c| c.timestamp_millis().to_string())
    );
    if let Ok(Some(cached)) = deps
        .cache
        .get::<Vec<TransactionResponseV2>>(&cache_key)
        .await
    {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }

    let views = deps
        .service
        .list(ListFilter {
            limit,
            before: cursor,
        })
        .await?;
    let resp: Vec<TransactionResponseV2> = views.into_iter().map(Into::into).collect();
    // 1 s was effectively no caching at our request rate. 10 s
    // bounded staleness is acceptable for a list endpoint —
    // each VU's cursor still pages through the same window
    // because the cache key embeds the cursor.
    let _ = deps.cache.set(&cache_key, &resp, 10).await;
    Ok(Json(ApiResponse::success(resp)))
}

pub(crate) async fn get_status(
    State(deps): State<TransactionsDeps>,
    Path(reference_id): Path<String>,
) -> AppResult<Json<ApiResponse<StatusResponseV2>>> {
    let cache_key = format!("tx_status:{}", reference_id);
    if let Some(cached) = deps.cache.get::<StatusResponseV2>(&cache_key).await? {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }
    metrics::counter!("cache_misses_total").increment(1);

    let view = deps.service.get_status_by_reference(&reference_id).await?;
    let resp: StatusResponseV2 = view.into();
    let _ = deps.cache.set(&cache_key, &resp, 60).await;
    Ok(Json(ApiResponse::success(resp)))
}
