use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension,
    Json,
};
use uuid::Uuid;

use crate::api::responses::{ApiResponse, HealthResponse, HealthServices};
use crate::db::models::{CreateTransactionRequest, TransactionResponse, TransactionRow};
use crate::db::shard::ShardRouter;
use crate::error::{AppError, AppResult};
use crate::middleware::request_id::RequestId;
use crate::AppState;

// ─── Transaction Handlers ──────────────────────────────────────

/// POST /api/v1/transactions
/// Shard-aware: routes to correct shard based on from_account.
pub async fn create_transaction(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    Json(req): Json<CreateTransactionRequest>,
) -> AppResult<(StatusCode, Json<ApiResponse<TransactionCreatedResponse>>)> {
    // Validate input
    if req.amount <= 0.0 {
        return Err(AppError::BadRequest("Amount must be positive".into()));
    }
    if req.from_account.is_empty() || req.to_account.is_empty() {
        return Err(AppError::BadRequest("Account IDs must not be empty".into()));
    }
    if req.from_account == req.to_account {
        return Err(AppError::BadRequest(
            "Source and destination accounts must be different".into(),
        ));
    }

    let reference_id = req.reference_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());

    // ─── Idempotency Check (Redis) ─────────────────────────
    let idempotency_key = format!("idempotency:{}", reference_id);
    if let Ok(Some(cached_response)) = state
        .cache
        .get::<TransactionCreatedResponse>(&idempotency_key)
        .await
    {
        metrics::counter!("idempotency_hits_total").increment(1);
        return Ok((
            StatusCode::ACCEPTED,
            Json(ApiResponse::success(cached_response)),
        ));
    }

    // Extract request ID for tracing
    let req_id = request_id.map(|r| r.0 .0.clone()).unwrap_or_default();

    // Determine shard for this transaction
    let shard = ShardRouter::shard_for(&req.from_account);

    // Create the message to enqueue (include shard info)
    let queue_message = serde_json::json!({
        "from_account": req.from_account,
        "to_account": req.to_account,
        "amount": req.amount,
        "currency": req.currency,
        "reference_id": reference_id,
        "description": req.description,
        "request_id": req_id,
        "shard": shard,
    });

    let response = TransactionCreatedResponse {
        reference_id: reference_id.clone(),
        status: "accepted".to_string(),
        message: format!("Transaction queued for processing (shard {})", shard),
    };

    // Execute RabbitMQ publish and Redis cache set concurrently
    let publish_future = state.queue_producer.publish(&queue_message);
    let cache_future = state.cache.set(&idempotency_key, &response, 86400);

    let (publish_res, cache_res) = tokio::join!(publish_future, cache_future);

    // Propagate RabbitMQ publish error if it fails
    publish_res?;

    // Log cache error but don't fail the transaction
    if let Err(e) = cache_res {
        tracing::warn!(error = %e, "Failed to set idempotency cache");
    }

    metrics::counter!("transactions_created_total").increment(1);

    Ok((
        StatusCode::ACCEPTED,
        Json(ApiResponse::success(response)),
    ))
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TransactionCreatedResponse {
    pub reference_id: String,
    pub status: String,
    pub message: String,
}

/// GET /api/v1/transactions/:id
/// Cross-shard: queries all shards since we don't know which shard has the ID.
pub async fn get_transaction(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<ApiResponse<TransactionResponse>>> {
    let cache_key = format!("txn:{}", id);

    // 1. Try cache first
    if let Some(cached) = state.cache.get::<TransactionResponse>(&cache_key).await? {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }

    metrics::counter!("cache_misses_total").increment(1);

    // 2. Query all shards in parallel
    let mut handles = Vec::new();
    for shard_idx in 0..state.shard_router.num_shards() {
        let pool = state.shard_router.reader(shard_idx).clone();
        let id = id;
        handles.push(tokio::spawn(async move {
            sqlx::query_as::<_, TransactionRow>("SELECT * FROM transactions WHERE id = $1")
                .bind(id)
                .fetch_optional(&pool)
                .await
        }));
    }

    for handle in handles {
        if let Ok(Ok(Some(row))) = handle.await {
            let response: TransactionResponse = row.into();
            let _ = state.cache.set(&cache_key, &response, 300).await;
            return Ok(Json(ApiResponse::success(response)));
        }
    }

    Err(AppError::NotFound(format!("Transaction {} not found", id)))
}

/// GET /api/v1/transactions
/// Cross-shard: queries all shards and merges results.
/// Cached with 1s TTL.
pub async fn list_transactions(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<ListParams>,
) -> AppResult<Json<ApiResponse<Vec<TransactionResponse>>>> {
    let limit = params.limit.unwrap_or(20).min(100) as i64;
    let offset = params.offset.unwrap_or(0) as i64;

    // 1s cache for list endpoint
    let cache_key = format!("txn_list:{}:{}", limit, offset);
    if let Ok(Some(cached)) = state.cache.get::<Vec<TransactionResponse>>(&cache_key).await {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }

    // Query all shards in parallel
    let mut handles = Vec::new();
    for shard_idx in 0..state.shard_router.num_shards() {
        let pool = state.shard_router.reader(shard_idx).clone();
        let limit = limit;
        let offset = offset;
        handles.push(tokio::spawn(async move {
            sqlx::query_as::<_, TransactionRow>(
                "SELECT * FROM transactions ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(&pool)
            .await
        }));
    }

    let mut all_rows: Vec<TransactionRow> = Vec::new();
    for handle in handles {
        if let Ok(Ok(rows)) = handle.await {
            all_rows.extend(rows);
        }
    }

    // Sort merged results by created_at DESC, take limit
    all_rows.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    all_rows.truncate(limit as usize);

    let responses: Vec<TransactionResponse> = all_rows.into_iter().map(Into::into).collect();

    // Cache for 1 second
    let _ = state.cache.set(&cache_key, &responses, 1).await;

    Ok(Json(ApiResponse::success(responses)))
}

#[derive(serde::Deserialize)]
pub struct ListParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

// ─── Health & Metrics Handlers ─────────────────────────────────

/// GET /health
/// Health check across all shards.
pub async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    let (db_write_healthy, db_read_healthy) = state.shard_router.health_check().await;
    let redis_healthy = state.cache.health_check().await.unwrap_or(false);
    let rabbitmq_healthy = state.queue_producer.health_check();

    let all_healthy = db_write_healthy && db_read_healthy && redis_healthy && rabbitmq_healthy;

    Json(HealthResponse {
        status: if all_healthy {
            "healthy".to_string()
        } else {
            "degraded".to_string()
        },
        services: HealthServices {
            database_write: db_write_healthy,
            database_read: db_read_healthy,
            redis: redis_healthy,
            rabbitmq: rabbitmq_healthy,
        },
    })
}

/// GET /metrics
pub async fn prometheus_metrics(State(state): State<AppState>) -> String {
    metrics::gauge!("backpressure_in_flight").set(state.backpressure.current_in_flight() as f64);

    let cb_state = state.circuit_breaker.current_state().await;
    let cb_val = match cb_state {
        crate::middleware::circuit_breaker::CircuitState::Closed => 0.0,
        crate::middleware::circuit_breaker::CircuitState::Open => 1.0,
        crate::middleware::circuit_breaker::CircuitState::HalfOpen => 2.0,
    };
    metrics::gauge!("circuit_breaker_state").set(cb_val);

    state.metrics_handle.render()
}
