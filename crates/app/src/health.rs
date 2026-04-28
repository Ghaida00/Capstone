//! `/health` and `/metrics` handlers — the two non-versioned
//! endpoints that survive the v1 cull.
//!
//! Rescued from the legacy `crates/app/src/api/handlers.rs` so
//! Step B (the v1 cull) can delete that file without taking
//! these handlers with it. Mounted directly by
//! [`crate::bootstrap::build_router`].

use axum::extract::State;
use axum::Json;

use shared_kernel::responses::{HealthResponse, HealthServices, ShardReplicaHealth};

use crate::AppState;

/// `GET /health` — health check across every shard, Redis, and
/// RabbitMQ. Returns `degraded` when any subsystem reports
/// unhealthy; otherwise `healthy`.
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

/// `GET /metrics` — render the Prometheus registry. Backpressure
/// and circuit-breaker gauges are published eagerly by the
/// middleware layers themselves, so this handler only has to
/// render the registry.
pub async fn prometheus_metrics(State(state): State<AppState>) -> String {
    state.metrics_handle.render()
}
