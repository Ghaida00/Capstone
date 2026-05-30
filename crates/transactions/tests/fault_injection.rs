//! T-6 fault-injection / contract-violation integration tests for
//! the transactions write path's idempotency surface.
//!
//! Two tests exercise the two sides of the idempotency contract:
//!
//!   1. `idempotency_replay_returns_same_response_for_identical_payload`
//!      — a well-behaved client retry under the same `reference_id`
//!      with the same body MUST return the SAME `TransactionAccepted`
//!      payload (not a fresh one, not a duplicate insert). This is
//!      the audit's R-11 replay-detection invariant — k6's 5%
//!      replay buffer relies on it.
//!
//!   2. `idempotency_conflict_rejects_same_key_with_different_payload`
//!      — a misbehaving client (or a payload-tamper attack) that
//!      reuses the same `reference_id` with a DIFFERENT amount MUST
//!      be rejected at the HTTP boundary, not silently committed.
//!      Maps `TransactionError::IdempotencyConflict` →
//!      `AppError::BadRequest` → HTTP 400. The audit's named
//!      fault-injection target for the idempotency layer.
//!
//! The audit's T-6 calls these out alongside RabbitMQ-down /
//! Sentinel-failover scenarios. Those two are operator-stack-level
//! and are exercised by T-9's deterministic failure-injection
//! pattern (DROP TABLE / DECIMAL overflow) for the cross-shard
//! processor's terminal paths. The two tests here close T-6 for
//! the synchronous request path through the transactions module.
//!
//! Wiring: Postgres + Redis via `testcontainers` (no RabbitMQ —
//! the `reserve` path is database-only under
//! `IdempotencyBackend::Pg`; the outbox row durably lands and we
//! never need to drain it). Synchronous PG backend so the test
//! drives the idempotency surface end-to-end without an intake
//! worker hop.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum_test::{TestResponse, TestServer};
use serde_json::json;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use tokio_util::sync::CancellationToken;

use accounts::ports::{
    AccountCreated, AccountError, AccountId, AccountService, Balance, CreateAccountInput,
    DynAccountService,
};
use shared_kernel::cache::redis::{RedisCache, RedisCacheConfig};
use shared_kernel::db::shard::{ShardRouter, ShardRouterConfig, ShardUrls};

const SCHEMA_SQL: &str = include_str!("../../../db/init.sql");

// ─── Fakes ─────────────────────────────────────────────────

/// Stand-in `AccountService` for tests that bypass the
/// cross-module verify. `verify_from_account` is set to `false`
/// when wiring the deps, so this fake's `get_balance` is never
/// invoked — its existence just satisfies the trait-object
/// requirement in `transactions::init`.
struct UnreachableAccountService;

#[async_trait]
impl AccountService for UnreachableAccountService {
    async fn get_balance(&self, _id: &AccountId) -> Result<Balance, AccountError> {
        panic!("verify_from_account was disabled — this fake must not be called");
    }

    async fn create_account(
        &self,
        _input: CreateAccountInput,
    ) -> Result<AccountCreated, AccountError> {
        panic!("verify_from_account was disabled — this fake must not be called");
    }
}

// ─── Fixture ──────────────────────────────────────────────

struct Fixture {
    server: TestServer,
    _pool: PgPool,
    _cancel: CancellationToken,
}

async fn setup() -> Fixture {
    // Postgres
    let pg = Postgres::default().start().await.unwrap();
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let pg_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");
    let pool = PgPool::connect(&pg_url).await.unwrap();
    sqlx::raw_sql(SCHEMA_SQL).execute(&pool).await.unwrap();

    // The senders need to exist as `active` users for the consumer
    // to ultimately drain them, but the synchronous reserve path
    // does NOT join against `users` — the row only needs to be
    // present at debit time. Seed anyway for realism.
    sqlx::query(
        r#"INSERT INTO users (account_number, full_name, balance, status)
           VALUES ('ACC-FI-001', 'sender', 1000000.00, 'active'),
                  ('ACC-FI-002', 'receiver', 0.00, 'active')"#,
    )
    .execute(&pool)
    .await
    .expect("seed users");

    // Redis
    let redis = Redis::default().start().await.unwrap();
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    // Container guards — leak so the OS keeps them alive for the
    // lifetime of the test process. Drop semantics on
    // `ContainerAsync` would otherwise race the deferred sqlx pool
    // teardown.
    Box::leak(Box::new(pg));
    Box::leak(Box::new(redis));

    let cancel = CancellationToken::new();

    // ShardRouter requires exactly NUM_SHARDS — degenerate two-shard
    // config (both slots point at the same PG) matching event_flow.rs.
    let single = ShardUrls {
        write_url: pg_url.clone(),
        read_urls: vec![pg_url.clone()],
    };
    let shard_config = ShardRouterConfig {
        shards: vec![single.clone(), single],
        write_pool_size: 4,
        read_pool_size: 4,
        health_check_interval_secs: 60,
    };
    let shards = ShardRouter::new(&shard_config, cancel.child_token())
        .await
        .unwrap();

    let cache_config = RedisCacheConfig {
        master_url: redis_url,
        read_url: None,
        pool_size: 4,
        sentinel_nodes: vec![],
        sentinel_master_name: "test-master".to_string(),
        sentinel_monitor_interval_secs: 60,
    };
    let cache = RedisCache::new(&cache_config, cancel.child_token())
        .await
        .unwrap();

    let accounts: DynAccountService = Arc::new(UnreachableAccountService);
    let tx_deps = transactions::init(
        shards,
        cache,
        accounts,
        /* verify_from_account */ false,
        // Pg backend exercises SqlxIdempotencyWriter — the production
        // path under PG-mode and the one the audit cites in R-11.
        transactions::IdempotencyBackend::Pg,
    );

    // axum 0.8 forbids `nest_service("/", _)`; mount the module
    // router directly as the app instead — the test posts to "/"
    // which is the router's own `POST /` route.
    let server = TestServer::new(transactions::router(tx_deps));

    Fixture {
        server,
        _pool: pool,
        _cancel: cancel,
    }
}

fn body_for(reference_id: &str, amount: &str) -> serde_json::Value {
    json!({
        "from_account": "ACC-FI-001",
        "to_account":   "ACC-FI-002",
        "amount":       amount,
        "currency":     "IDR",
        "reference_id": reference_id,
        "description":  "T-6 fault-injection",
    })
}

// ─── Tests ────────────────────────────────────────────────

/// Same key + same payload → replay; second response is the
/// SAME `TransactionAccepted` payload as the first. Audit's R-11
/// invariant; k6's 5% replay buffer relies on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_replay_returns_same_response_for_identical_payload() {
    let fx = setup().await;
    let reference_id = "ref-replay-001";

    let first: TestResponse = fx
        .server
        .post("/")
        .json(&body_for(reference_id, "12.34"))
        .await;
    assert_eq!(first.status_code(), StatusCode::ACCEPTED);
    let first_body: serde_json::Value = first.json();
    let first_message = first_body["data"]["message"].as_str().unwrap().to_owned();

    // Second request — identical payload — must return the same
    // accepted envelope, not a fresh 202 with a different message
    // or any 5xx from a constraint-violation INSERT.
    let second: TestResponse = fx
        .server
        .post("/")
        .json(&body_for(reference_id, "12.34"))
        .await;
    assert_eq!(second.status_code(), StatusCode::ACCEPTED);
    let second_body: serde_json::Value = second.json();
    assert_eq!(
        second_body["data"]["reference_id"], reference_id,
        "replay must echo the same reference_id"
    );
    assert_eq!(
        second_body["data"]["message"].as_str().unwrap(),
        first_message,
        "replay must return the originally-accepted message byte-identical"
    );
}

/// Same key + DIFFERENT payload → hash conflict; 400.
/// Audit's named fault-injection target — a payload-tamper attack
/// or a buggy client must not silently double-process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_conflict_rejects_same_key_with_different_payload() {
    let fx = setup().await;
    let reference_id = "ref-conflict-001";

    let first: TestResponse = fx
        .server
        .post("/")
        .json(&body_for(reference_id, "12.34"))
        .await;
    assert_eq!(first.status_code(), StatusCode::ACCEPTED);

    // Same reference_id, DIFFERENT amount → the SHA-256
    // request_hash on the second request differs from the
    // stored one; the reservation surfaces `HashConflict` → 400.
    let second: TestResponse = fx
        .server
        .post("/")
        .json(&body_for(reference_id, "99.99"))
        .expect_failure()
        .await;
    assert_eq!(second.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = second.json();
    // AppError serialises as `{"error": "...", "message": "..."}`
    // — no `success` field on error responses (that envelope is
    // success-side only via ApiResponse).
    assert_eq!(body["error"], "bad_request");
    // The message must mention the contract violation so an
    // operator reading a 400-rate alert can distinguish this
    // class from "client sent bad JSON".
    let message = body["message"].as_str().unwrap();
    assert!(
        message.to_lowercase().contains("idempotency")
            || message.to_lowercase().contains("payload"),
        "conflict body must signal the idempotency contract violation; got {message:?}"
    );

    // Sanity: the same reference_id with the ORIGINAL payload
    // still replays correctly — the reservation slot was not
    // poisoned by the rejected conflict attempt.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let third: TestResponse = fx
        .server
        .post("/")
        .json(&body_for(reference_id, "12.34"))
        .await;
    assert_eq!(third.status_code(), StatusCode::ACCEPTED);
}
