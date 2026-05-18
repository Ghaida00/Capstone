//! `/api/v2/admin/*` operator surface — exposes the rows that
//! the R-2 alerts (cross-shard outbox terminal failures and
//! refund-stuck rows) page on, plus the audit rows still at
//! `'processing'` past a configurable age threshold.
//!
//! Hard-gated by [`crate::middleware::auth::require_admin_middleware`]
//! (`ENABLE_AUTH=true` + JWT `role = "admin"`). The endpoints are
//! read-only — they do not mutate outbox or audit state. The
//! reconciliation procedure (clearing `lease_until`, resetting
//! `attempts`, manual credits) lives in
//! `docs/runbooks/cross-shard-outbox-reconciliation.md` so the
//! audit trail of operator actions runs through `psql` rather
//! than a thin HTTP wrapper.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::AppState;

/// Hard cap on `limit` per shard. Operator queries should never
/// need more rows than this in a single response — paginate by
/// raising `age_gt_secs` instead.
const LIMIT_CAP: i64 = 1000;

/// Default rows returned per shard when `limit` is omitted.
const DEFAULT_LIMIT: i64 = 100;

/// Build the admin sub-router. Mounted under `/api/v2/admin` by
/// [`crate::bootstrap::build_router`], with the admin gate layer
/// applied at mount time (not here) so this builder stays
/// state-free.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/outbox", get(list_outbox))
        .route("/stuck-transactions", get(list_stuck_transactions))
        .route(
            "/degradation",
            get(get_degradation).put(put_degradation),
        )
}

#[derive(Debug, Deserialize)]
pub struct SetDegradationBody {
    /// `normal` | `read_only` | `essential_only`.
    pub mode: String,
}

/// R-9: read the current degradation posture. Gauge-equivalent in
/// JSON form for an operator who is on the box, not in Grafana.
async fn get_degradation(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "mode": state.degradation.mode().as_str() }))
}

/// R-9: flip the degradation posture at runtime (no restart).
/// Admin-gated by `require_admin_middleware` at mount time.
async fn put_degradation(
    State(state): State<AppState>,
    Json(body): Json<SetDegradationBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let mode = crate::degradation::DegradationMode::parse(&body.mode).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_mode",
                "message": "mode must be one of: normal | read_only | essential_only",
            })),
        )
    })?;
    let set = state.degradation.set(mode);
    Ok(Json(serde_json::json!({ "mode": set.as_str() })))
}

#[derive(Debug, Deserialize)]
pub struct AdminOutboxQuery {
    /// `failed` returns outbox rows that hit the credit-terminal
    /// branch (R-2 page-worthy). `refund-stuck` returns rows past
    /// `MAX_ATTEMPTS` on the refund path (still `status='pending'`
    /// by design — sender is debited, refund has not landed).
    pub status: String,
    /// Restrict to a single shard. Omit to query every shard via
    /// `ShardRouter::all_shards()`.
    pub shard: Option<usize>,
    /// Only rows whose `updated_at` is older than this many seconds.
    /// Required — without a threshold the response would include
    /// rows that just landed in the failure path, swamping signal.
    pub age_gt_secs: i64,
    /// Per-shard row cap. Defaults to 100; hard-capped at 1000.
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AdminStuckTxQuery {
    /// Only rows in `'processing'` older than this many seconds.
    pub age_gt_secs: i64,
    pub shard: Option<usize>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct OutboxAdminRow {
    pub id: Uuid,
    pub from_account: String,
    pub to_account: String,
    pub to_shard: i32,
    pub amount: Decimal,
    pub currency: String,
    pub reference_id: String,
    pub status: String,
    pub attempts: i32,
    pub refund_required: bool,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct StuckTransactionRow {
    pub id: Uuid,
    pub from_account: String,
    pub to_account: String,
    pub amount: Decimal,
    pub currency: String,
    pub status: String,
    pub reference_id: Option<String>,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ShardRows<T> {
    pub shard: usize,
    pub rows: Vec<T>,
}

async fn list_outbox(
    State(state): State<AppState>,
    Query(params): Query<AdminOutboxQuery>,
) -> Result<Json<Vec<ShardRows<OutboxAdminRow>>>, (StatusCode, Json<serde_json::Value>)> {
    let limit = clamp_limit(params.limit);
    let num_shards = state.shard_router.num_shards();
    let shards_to_query: Vec<usize> = match params.shard {
        Some(s) if s < num_shards => vec![s],
        Some(s) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "shard_out_of_range",
                    "message": format!("shard {} >= num_shards {}", s, num_shards),
                })),
            ));
        }
        None => (0..num_shards).collect(),
    };

    // Two SQL shapes — keep both inline rather than format!-build
    // a query so the strings stay grep-able and don't risk
    // injecting a misformed status filter.
    let mut out = Vec::with_capacity(shards_to_query.len());
    for shard_idx in shards_to_query {
        let pool = state.shard_router.writer(shard_idx);
        let rows: Vec<OutboxAdminRow> = match params.status.as_str() {
            "failed" => sqlx::query_as(
                "SELECT id, from_account, to_account, to_shard, amount, currency, \
                        reference_id, status, attempts, refund_required, last_error, \
                        created_at, updated_at \
                 FROM cross_shard_outbox \
                 WHERE status = 'failed' \
                   AND updated_at < NOW() - (INTERVAL '1 second' * $1) \
                 ORDER BY updated_at DESC \
                 LIMIT $2",
            )
            .bind(params.age_gt_secs)
            .bind(limit)
            .fetch_all(pool)
            .await
            .map_err(internal)?,
            "refund-stuck" => sqlx::query_as(
                "SELECT id, from_account, to_account, to_shard, amount, currency, \
                        reference_id, status, attempts, refund_required, last_error, \
                        created_at, updated_at \
                 FROM cross_shard_outbox \
                 WHERE status = 'pending' \
                   AND refund_required = TRUE \
                   AND attempts >= 10 \
                   AND updated_at < NOW() - (INTERVAL '1 second' * $1) \
                 ORDER BY updated_at DESC \
                 LIMIT $2",
            )
            .bind(params.age_gt_secs)
            .bind(limit)
            .fetch_all(pool)
            .await
            .map_err(internal)?,
            other => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "invalid_status",
                        "message": format!("status must be 'failed' or 'refund-stuck', got '{}'", other),
                    })),
                ));
            }
        };
        out.push(ShardRows {
            shard: shard_idx,
            rows,
        });
    }
    Ok(Json(out))
}

async fn list_stuck_transactions(
    State(state): State<AppState>,
    Query(params): Query<AdminStuckTxQuery>,
) -> Result<Json<Vec<ShardRows<StuckTransactionRow>>>, (StatusCode, Json<serde_json::Value>)> {
    let limit = clamp_limit(params.limit);
    let num_shards = state.shard_router.num_shards();
    let shards_to_query: Vec<usize> = match params.shard {
        Some(s) if s < num_shards => vec![s],
        Some(s) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "shard_out_of_range",
                    "message": format!("shard {} >= num_shards {}", s, num_shards),
                })),
            ));
        }
        None => (0..num_shards).collect(),
    };

    let mut out = Vec::with_capacity(shards_to_query.len());
    for shard_idx in shards_to_query {
        let pool = state.shard_router.writer(shard_idx);
        let rows: Vec<StuckTransactionRow> = sqlx::query_as(
            "SELECT id, from_account, to_account, amount, currency, status, \
                    reference_id, failure_reason, created_at, updated_at \
             FROM transactions \
             WHERE status = 'processing' \
               AND created_at < NOW() - (INTERVAL '1 second' * $1) \
             ORDER BY created_at DESC \
             LIMIT $2",
        )
        .bind(params.age_gt_secs)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(internal)?;
        out.push(ShardRows {
            shard: shard_idx,
            rows,
        });
    }
    Ok(Json(out))
}

fn clamp_limit(requested: Option<i64>) -> i64 {
    let l = requested.unwrap_or(DEFAULT_LIMIT);
    l.clamp(1, LIMIT_CAP)
}

fn internal(e: sqlx::Error) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "error": "db_error",
            "message": e.to_string(),
        })),
    )
}
