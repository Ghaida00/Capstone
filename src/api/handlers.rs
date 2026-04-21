use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use chrono::Utc;
use serde_json::json;

use crate::api::responses::{ApiResponse, HealthResponse, HealthServices, ShardReplicaHealth};
use crate::db::models::{
    CreateTransactionRequest, IdempotencyKeyRow, TransactionResponse, TransactionRow,
    TransactionStatusRow, UserRow,
};
use crate::db::failover::retry_transient;
use crate::db::shard::ShardRouter;
use crate::error::{AppError, AppResult};
use crate::middleware::request_id::RequestId;
use crate::AppState;

// ─── Input Validation ──────────────────────────────────────────

/// Maximum length for account numbers (matches DB VARCHAR(50)).
const MAX_ACCOUNT_LEN: usize = 50;
/// Maximum length for reference IDs (matches DB VARCHAR(100)).
const MAX_REFERENCE_ID_LEN: usize = 100;

/// Validate an account number: non-empty, within length, safe characters.
fn validate_account(account: &str, field_name: &str) -> AppResult<()> {
    if account.is_empty() {
        return Err(AppError::BadRequest(format!(
            "{} must not be empty",
            field_name
        )));
    }
    if account.len() > MAX_ACCOUNT_LEN {
        return Err(AppError::BadRequest(format!(
            "{} must be at most {} characters",
            field_name, MAX_ACCOUNT_LEN
        )));
    }
    // Allow alphanumeric, hyphens, underscores, and dots
    if !account
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(AppError::BadRequest(format!(
            "{} contains invalid characters (alphanumeric, hyphens, underscores, dots only)",
            field_name
        )));
    }
    Ok(())
}

/// Validate a reference ID: within length, safe characters.
fn validate_reference_id(reference_id: &str) -> AppResult<()> {
    if reference_id.len() > MAX_REFERENCE_ID_LEN {
        return Err(AppError::BadRequest(format!(
            "reference_id must be at most {} characters",
            MAX_REFERENCE_ID_LEN
        )));
    }
    if !reference_id
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(AppError::BadRequest(
            "reference_id contains invalid characters".into(),
        ));
    }
    Ok(())
}

// ─── Transaction Handlers ──────────────────────────────────────

/// POST /api/v1/transactions
/// Shard-aware: routes to correct shard based on from_account.
pub async fn create_transaction(
    State(state): State<AppState>,
    request_id: Option<Extension<RequestId>>,
    Json(req): Json<CreateTransactionRequest>,
) -> AppResult<(StatusCode, Json<ApiResponse<TransactionCreatedResponse>>)> {
    // --- Input validation (#26) ---
    if req.amount <= Decimal::ZERO {
        return Err(AppError::BadRequest("Amount must be positive".into()));
    }
    validate_account(&req.from_account, "from_account")?;
    validate_account(&req.to_account, "to_account")?;
    if req.from_account == req.to_account {
        return Err(AppError::BadRequest(
            "Source and destination accounts must be different".into(),
        ));
    }

    let reference_id = req
        .reference_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    if let Some(ref rid) = req.reference_id {
        validate_reference_id(rid)?;
    }

    // Route transaction to shard first; idempotency key is shard-scoped too.
    let shard = ShardRouter::shard_for(&req.from_account);
    let writer = state.shard_router.writer(shard);

    // Keep Redis as fast-path, but align key with DB-backed idempotency.
    let idempotency_key = format!("txn:{}:{}", shard, reference_id);

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

    // Stable request hash for duplicate detection
    let request_hash = json!({
        "from_account": req.from_account,
        "to_account": req.to_account,
        "amount": req.amount.to_string(),
        "currency": req.currency,
        "reference_id": reference_id,
        "description": req.description,
    })
    .to_string();

    let response = TransactionCreatedResponse {
        reference_id: reference_id.clone(),
        status: "accepted".to_string(),
        message: format!("Transaction queued for processing (shard {})", shard),
    };

    let response_payload = serde_json::to_value(&response)
        .map_err(|e| AppError::Internal(format!("Failed to serialize idempotency payload: {e}")))?;

    // Reserve idempotency key in DB first.
    let insert_res = sqlx::query(
        r#"
        INSERT INTO idempotency_keys (
            idempotency_key,
            request_hash,
            status,
            response_payload,
            expires_at
        )
        VALUES ($1, $2, 'processing', $3, NOW() + INTERVAL '24 hours')
        ON CONFLICT (idempotency_key) DO NOTHING
        "#,
    )
    .bind(&idempotency_key)
    .bind(&request_hash)
    .bind(&response_payload)
    .execute(writer)
    .await?;

    // If the row already exists, validate and return the stored response
    // unless it is failed/expired, in which case we can try to revive it.
    if insert_res.rows_affected() == 0 {
        return handle_existing_idempotency_key(
            &state, writer, &idempotency_key, &request_hash, &response, &response_payload,
        )
        .await;
    }

    // Build queue message
    let queue_message = serde_json::json!({
        "from_account": req.from_account,
        "to_account": req.to_account,
        "amount": req.amount.to_string(),
        "currency": req.currency,
        "reference_id": reference_id,
        "description": req.description,
        "request_id": req_id,
        "shard": shard,
        "idempotency_key": idempotency_key,
        "request_hash": request_hash,
    });

    // Publish to RabbitMQ
    if let Err(e) = state.queue_producer.publish(&queue_message).await {
        let _ = sqlx::query(
            r#"
            UPDATE idempotency_keys
            SET status = 'failed',
                updated_at = NOW()
            WHERE idempotency_key = $1
            "#,
        )
        .bind(&idempotency_key)
        .execute(writer)
        .await;

        return Err(e);
    }

    // Keep Redis as a fast-path cache for accepted responses.
    let _ = state.cache.set(&idempotency_key, &response, 86400).await;

    metrics::counter!("transactions_created_total").increment(1);

    Ok((StatusCode::ACCEPTED, Json(ApiResponse::success(response))))
}

/// Handle the case where an idempotency key already exists in the database.
/// Extracted from create_transaction for clarity (#31: service extraction).
async fn handle_existing_idempotency_key(
    state: &AppState,
    writer: &sqlx::PgPool,
    idempotency_key: &str,
    request_hash: &str,
    response: &TransactionCreatedResponse,
    response_payload: &serde_json::Value,
) -> AppResult<(StatusCode, Json<ApiResponse<TransactionCreatedResponse>>)> {
    let existing: Option<IdempotencyKeyRow> = sqlx::query_as(
        r#"
        SELECT
            id,
            idempotency_key,
            request_hash,
            status,
            response_payload,
            expires_at,
            created_at,
            updated_at
        FROM idempotency_keys
        WHERE idempotency_key = $1
        "#,
    )
    .bind(idempotency_key)
    .fetch_optional(writer)
    .await?;

    let existing = match existing {
        Some(row) => row,
        None => {
            return Err(AppError::Internal(
                "Idempotency row disappeared after conflict".into(),
            ))
        }
    };

    if existing.request_hash != request_hash {
        return Err(AppError::BadRequest(
            "Idempotency key already used with a different request payload".into(),
        ));
    }

    // Normal duplicate replay: return the cached accepted response.
    if existing.status == "processing"
        || existing.status == "completed"
        || existing.status == "pending"
    {
        metrics::counter!("idempotency_hits_total").increment(1);

        if let Some(payload) = existing.response_payload {
            if let Ok(cached_response) =
                serde_json::from_value::<TransactionCreatedResponse>(payload)
            {
                let _ = state
                    .cache
                    .set(idempotency_key, &cached_response, 86400)
                    .await;

                return Ok((
                    StatusCode::ACCEPTED,
                    Json(ApiResponse::success(cached_response)),
                ));
            }
        }

        let _ = state.cache.set(idempotency_key, response, 86400).await;
        return Ok((
            StatusCode::ACCEPTED,
            Json(ApiResponse::success(response.clone())),
        ));
    }

    // Revive failed/expired reservation so the same key can be retried safely.
    if existing.status == "failed" || existing.expires_at <= Utc::now() {
        let revive_res = sqlx::query(
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
        .bind(response_payload)
        .execute(writer)
        .await?;

        if revive_res.rows_affected() == 0 {
            // Another request may have revived it first; return the latest stored response.
            let latest: Option<IdempotencyKeyRow> = sqlx::query_as(
                r#"
                SELECT
                    id, idempotency_key, request_hash, status,
                    response_payload, expires_at, created_at, updated_at
                FROM idempotency_keys
                WHERE idempotency_key = $1
                "#,
            )
            .bind(idempotency_key)
            .fetch_optional(writer)
            .await?;

            if let Some(latest) = latest {
                if let Some(payload) = latest.response_payload {
                    if let Ok(cached_response) =
                        serde_json::from_value::<TransactionCreatedResponse>(payload)
                    {
                        let _ = state
                            .cache
                            .set(idempotency_key, &cached_response, 86400)
                            .await;

                        return Ok((
                            StatusCode::ACCEPTED,
                            Json(ApiResponse::success(cached_response)),
                        ));
                    }
                }
            }

            return Ok((
                StatusCode::ACCEPTED,
                Json(ApiResponse::success(response.clone())),
            ));
        }
    } else {
        // Unexpected state: safest fallback is to return the current accepted response.
        let _ = state.cache.set(idempotency_key, response, 86400).await;
        return Ok((
            StatusCode::ACCEPTED,
            Json(ApiResponse::success(response.clone())),
        ));
    }

    let _ = state.cache.set(idempotency_key, response, 86400).await;
    Ok((
        StatusCode::ACCEPTED,
        Json(ApiResponse::success(response.clone())),
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
/// Fix #14: Uses cursor-based (keyset) pagination instead of broken LIMIT/OFFSET.
pub async fn list_transactions(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<ListParams>,
) -> AppResult<Json<ApiResponse<Vec<TransactionResponse>>>> {
    let limit = params.limit.unwrap_or(20).min(100) as i64;

    // Fix #14: cursor-based pagination. `before` is an ISO8601 timestamp that
    // acts as the keyset cursor — we only return rows created before this time.
    let cursor = params.before.as_deref().and_then(|s| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    // Short-lived cache keyed by cursor + limit
    let cache_key = format!(
        "txn_list:{}:{}",
        limit,
        cursor.map_or("latest".to_string(), |c| c.timestamp_millis().to_string())
    );
    if let Ok(Some(cached)) = state
        .cache
        .get::<Vec<TransactionResponse>>(&cache_key)
        .await
    {
        metrics::counter!("cache_hits_total").increment(1);
        return Ok(Json(ApiResponse::success(cached)));
    }

    // Query all shards in parallel with keyset filter
    let mut handles = Vec::new();
    for shard_idx in 0..state.shard_router.num_shards() {
        let pool = state.shard_router.reader(shard_idx).clone();

        handles.push(tokio::spawn(async move {
            match cursor {
                Some(before) => {
                    sqlx::query_as::<_, TransactionRow>(
                        "SELECT * FROM transactions WHERE created_at < $1 ORDER BY created_at DESC LIMIT $2",
                    )
                    .bind(before)
                    .bind(limit)
                    .fetch_all(&pool)
                    .await
                }
                None => {
                    sqlx::query_as::<_, TransactionRow>(
                        "SELECT * FROM transactions ORDER BY created_at DESC LIMIT $1",
                    )
                    .bind(limit)
                    .fetch_all(&pool)
                    .await
                }
            }
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
    /// ISO8601 timestamp cursor for keyset pagination. Returns results
    /// created strictly before this timestamp. Omit for the latest page.
    pub before: Option<String>,
    // Kept for backward compat but unused with keyset pagination
    #[allow(dead_code)]
    pub offset: Option<u32>,
}

/// GET /api/v1/transactions/status/{reference_id}
/// Fix #15: Searches all shards in parallel (was sequential).
pub async fn get_transaction_status(
    State(state): State<AppState>,
    Path(reference_id): Path<String>,
) -> AppResult<Json<ApiResponse<TransactionStatusResponse>>> {
    if reference_id.is_empty() {
        return Err(AppError::BadRequest(
            "reference_id must not be empty".into(),
        ));
    }
    // #26: Validate reference_id input
    validate_reference_id(&reference_id)?;

    let cache_key = format!("tx_status:{}", reference_id);

    // Try Redis first
    if let Some(cached) = state
        .cache
        .get::<TransactionStatusResponse>(&cache_key)
        .await?
    {
        metrics::counter!("cache_hits_total").increment(1);

        return Ok(Json(ApiResponse::success(cached)));
    }

    metrics::counter!("cache_misses_total").increment(1);

    // Fix #15: Search across shards in PARALLEL (was sequential for-loop)
    let mut handles = Vec::new();
    for shard_idx in 0..state.shard_router.num_shards() {
        let pool = state.shard_router.reader(shard_idx).clone();
        let ref_id = reference_id.clone();
        handles.push(tokio::spawn(async move {
            sqlx::query_as::<_, TransactionStatusRow>(
                r#"
                SELECT reference_id, status, processed_at
                FROM transactions
                WHERE reference_id = $1
                "#,
            )
            .bind(&ref_id)
            .fetch_optional(&pool)
            .await
        }));
    }

    for handle in handles {
        if let Ok(Ok(Some(row))) = handle.await {
            let response = TransactionStatusResponse {
                reference_id: row.reference_id.unwrap_or_default(),
                status: row.status,
                processed_at: row.processed_at,
            };

            let _ = state.cache.set(&cache_key, &response, 60).await;

            return Ok(Json(ApiResponse::success(response)));
        }
    }

    Err(AppError::NotFound(format!(
        "Transaction {} not found",
        reference_id
    )))
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TransactionStatusResponse {
    pub reference_id: String,
    pub status: String,
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// GET /api/v1/users/:account_number/balance
/// Read-heavy endpoint (uses read replica + Redis cache)
pub async fn get_balance(
    State(state): State<AppState>,
    Path(account_number): Path<String>,
) -> AppResult<Json<ApiResponse<BalanceResponse>>> {
    // #26: Validate account_number input
    validate_account(&account_number, "account_number")?;

    let cache_key = format!("balance:{}", account_number);

    // Try Redis cache first
    if let Some(cached) = state.cache.get::<BalanceResponse>(&cache_key).await? {
        metrics::counter!("cache_hits_total").increment(1);

        return Ok(Json(ApiResponse::success(cached)));
    }

    metrics::counter!("cache_misses_total").increment(1);

    // Route to correct shard. `reader()` is called inside the retry
    // closure so a transient replica failure on attempt 1 may pick a
    // different healthy replica on attempt 2.
    let shard = ShardRouter::shard_for(&account_number);
    let router = &state.shard_router;

    // Reads are idempotent — retry transient connection errors.
    let row: Option<UserRow> = retry_transient(
        || async {
            sqlx::query_as(
                r#"
                SELECT *
                FROM users
                WHERE account_number = $1
                AND status = 'active'
                "#,
            )
            .bind(&account_number)
            .fetch_optional(router.reader(shard))
            .await
        },
        2,
        20,
        "get_balance",
    )
    .await?;

    let row = match row {
        Some(r) => r,
        None => {
            return Err(AppError::NotFound(format!(
                "Account {} not found",
                account_number
            )))
        }
    };

    let response = BalanceResponse {
        account_number: row.account_number,
        balance: row.balance.to_string(),
        currency: "IDR".to_string(),
        status: row.status,
    };

    // Cache response (TTL 30 seconds)
    let _ = state.cache.set(&cache_key, &response, 30).await;

    Ok(Json(ApiResponse::success(response)))
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct BalanceResponse {
    pub account_number: String,
    pub balance: String,
    pub currency: String,
    pub status: String,
}

// ─── Health & Metrics Handlers ─────────────────────────────────

/// GET /health
/// Health check across all shards.
pub async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    let (db_write_healthy, db_read_healthy) = state.shard_router.health_check().await;
    let redis_healthy = state.cache.health_check().await.unwrap_or(false);
    let rabbitmq_healthy = state.queue_producer.health_check();

    let replicas: Vec<ShardReplicaHealth> = state
        .shard_router
        .replica_health()
        .into_iter()
        .enumerate()
        .map(|(shard, (total, healthy))| ShardReplicaHealth {
            shard,
            total,
            healthy,
        })
        .collect();

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
            replicas,
        },
    })
}

/// GET /metrics
///
/// Fix #30: Gauges for backpressure and the circuit breaker are now
/// published eagerly by the middleware layers themselves, so this
/// handler only has to render the Prometheus registry.
pub async fn prometheus_metrics(State(state): State<AppState>) -> String {
    state.metrics_handle.render()
}
