//! Event-flow integration test for the rewired consumer.
//!
//! Spins up Postgres + Redis + RabbitMQ via `testcontainers`, wires
//! the full transactions write path (HTTP handler → service →
//! producer → consumer → DB) plus the notifications subscriber,
//! then asserts that `POST /api/v2/transactions` yields a row in
//! `transactions` AND a `notifications/recent` entry referencing
//! the same `reference_id`.
//!
//! Regression detector for the Phase-2 follow-up consumer rewire —
//! see `docs/architecture/cutover-readiness.md` §2.3.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum_test::{TestResponse, TestServer};
use serde_json::{json, Value};
use sqlx::PgPool;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::cache::redis::{RedisCache, RedisCacheConfig};
use shared_kernel::db::shard::{ShardRouter, ShardRouterConfig, ShardUrls};
use shared_kernel::events::{EventPublisher, EventSubscriber, InProcessEventBus};
use shared_kernel::queue::producer::QueueProducer;

const SCHEMA_SQL: &str = include_str!("../../../db/init.sql");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_v2_transaction_lands_in_db_and_notifications() {
    // ── 1. Postgres ────────────────────────────────────────
    let pg = Postgres::default().start().await.unwrap();
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let pg_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");

    let pool = PgPool::connect(&pg_url).await.unwrap();
    sqlx::raw_sql(SCHEMA_SQL).execute(&pool).await.unwrap();

    let from_account = "ACC-EVT-001";
    let to_account = "ACC-EVT-002";
    seed_user(&pool, from_account, "1000000.00").await;
    seed_user(&pool, to_account, "0.00").await;

    // ── 2. Redis ───────────────────────────────────────────
    let redis = Redis::default().start().await.unwrap();
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    // ── 3. RabbitMQ ────────────────────────────────────────
    let rabbit = GenericImage::new("rabbitmq", "3-alpine")
        .with_exposed_port(ContainerPort::Tcp(5672))
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
        .start()
        .await
        .unwrap();
    let rabbit_port = rabbit.get_host_port_ipv4(5672).await.unwrap();
    let amqp_url = format!("amqp://guest:guest@127.0.0.1:{rabbit_port}");

    // ── 4. Infra ───────────────────────────────────────────
    let cancel = CancellationToken::new();

    // ShardRouter requires exactly NUM_SHARDS (currently 2 —
    // shard 2 is disabled for resource conservation); point both
    // at the same Postgres so routing is degenerate but valid.
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
        master_url: redis_url.clone(),
        read_url: None,
        pool_size: 4,
        sentinel_nodes: vec![],
        sentinel_master_name: "test-master".to_string(),
        sentinel_monitor_interval_secs: 60,
    };
    let cache = RedisCache::new(&cache_config, cancel.child_token())
        .await
        .unwrap();

    // Producer declares the exchange + queue topology; must run
    // before the consumer attempts to subscribe.
    let producer = QueueProducer::new(&amqp_url).await.unwrap();

    // ── 5. Bus + module wiring ─────────────────────────────
    let bus = InProcessEventBus::new();
    let publisher: Arc<dyn EventPublisher> = Arc::new(bus.clone());
    let subscriber: Arc<dyn EventSubscriber> = Arc::new(bus);

    let accounts_deps = accounts::init(shards.clone(), cache.clone());
    let tx_deps = transactions::init(
        shards.clone(),
        cache.clone(),
        accounts_deps.service.clone(),
        // Keep the cross-module verify ON for the integration
        // test — this is the only place the wiring is exercised
        // end-to-end, and we want the modular-monolith DI seam
        // covered. Production / load-test paths still bypass it
        // by leaving `TX_VERIFY_FROM_ACCOUNT` at its default.
        true,
    );

    // Drives the outbox row from `idempotency_keys` to RabbitMQ so
    // the consumer downstream sees the message — without this the
    // create returns 202 but nothing ever lands in the queue.
    let _publish_outbox_handles =
        transactions::spawn_publish_outbox(shards.clone(), producer, cancel.child_token());
    let (notif_deps, _notif_handle) = notifications::init(subscriber, cancel.child_token());

    // ── 6. Start consumer ──────────────────────────────────
    let _consumer_handle = transactions::start_consumer(
        &amqp_url,
        shards.clone(),
        publisher.clone(),
        cancel.child_token(),
    )
    .await
    .expect("start_consumer");

    // Give the consumer a moment to subscribe before publishing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── 7. Build router and POST ───────────────────────────
    let app = axum::Router::new()
        .nest_service("/api/v2/transactions", transactions::router(tx_deps))
        .nest_service("/api/v2/notifications", notifications::router(notif_deps));
    let server = TestServer::new(app);

    let reference_id = format!("evt-{}", Uuid::new_v4());
    let body = json!({
        "from_account": from_account,
        "to_account": to_account,
        "amount": "12345.00",
        "currency": "IDR",
        "reference_id": reference_id,
        "description": "event-flow test",
    });
    let resp: TestResponse = server.post("/api/v2/transactions").json(&body).await;
    assert_eq!(
        resp.status_code(),
        StatusCode::ACCEPTED,
        "POST /api/v2/transactions should return 202 (body: {})",
        resp.text()
    );

    // ── 8. Wait for consumer flush, assert DB row ──────────
    let mut found_db = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT from_account, to_account FROM transactions WHERE reference_id = $1",
        )
        .bind(&reference_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        if let Some((f, t)) = row {
            assert_eq!(f, from_account);
            assert_eq!(t, to_account);
            found_db = true;
            break;
        }
    }
    assert!(
        found_db,
        "expected a `transactions` row for reference_id={reference_id}"
    );

    // ── 9. Assert notifications/recent entry exists ────────
    let mut found_notif = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let resp: TestResponse = server.get("/api/v2/notifications/recent").await;
        if resp.status_code() != StatusCode::OK {
            continue;
        }
        let body: Value = resp.json();
        let entries = body["data"].as_array().cloned().unwrap_or_default();
        if entries
            .iter()
            .any(|e| e["payload"]["reference_id"].as_str() == Some(reference_id.as_str()))
        {
            found_notif = true;
            break;
        }
    }
    assert!(
        found_notif,
        "expected a notifications/recent entry for reference_id={reference_id}"
    );

    cancel.cancel();
}

async fn seed_user(pool: &PgPool, account: &str, balance: &str) {
    sqlx::query(
        r#"INSERT INTO users (account_number, full_name, balance, status)
           VALUES ($1, $2, $3::numeric, 'active')"#,
    )
    .bind(account)
    .bind(format!("Test User {account}"))
    .bind(balance)
    .execute(pool)
    .await
    .expect("seed user");
}
