//! HTTP handlers for `/api/v2/notifications/*`.
//!
//! Trivially short by design: extract → port call → map result.
//! No business logic.

use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;

use shared_kernel::error::{AppError, AppResult};
use shared_kernel::responses::ApiResponse;

use super::super::infrastructure::NotificationsDeps;
use super::super::ports::{NotificationEntry, NotificationError};

const DEFAULT_LIMIT: usize = 50;

#[derive(Debug, Deserialize)]
pub(crate) struct RecentQuery {
    /// Caller-provided page size; the application layer clamps
    /// to the module-private `MAX_RECENT`.
    pub limit: Option<usize>,
}

// ─── Error mapping ─────────────────────────────────────────

impl From<NotificationError> for AppError {
    fn from(err: NotificationError) -> AppError {
        match err {
            NotificationError::Validation(m) => AppError::BadRequest(m),
            NotificationError::Infra(m) => AppError::Internal(m),
        }
    }
}

// ─── Handler ───────────────────────────────────────────────

/// GET /api/v2/notifications/recent?limit=N
///
/// Returns the most recent in-memory notification entries,
/// newest-first. Page size is bounded by the application layer.
pub(crate) async fn recent(
    State(deps): State<NotificationsDeps>,
    Query(q): Query<RecentQuery>,
) -> AppResult<Json<ApiResponse<Vec<NotificationEntry>>>> {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT);
    let entries = deps.log.recent(limit).await?;
    Ok(Json(ApiResponse::success(entries)))
}
