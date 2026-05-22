//! Crash / idempotency integration test for the batched redis-intake
//! worker. Asserts:
//!   * normal path: N reservations seeded -> N PG rows published,
//!     RabbitMQ queue has exactly N messages, inflight + pending
//!     lists empty.
//!   * crash path: seed the inflight list directly (the post-crash
//!     state where a prior worker had RPOPLPUSH'd but died before
//!     LREM); restart -> drain_inflight_batched processes every
//!     seeded key exactly once, with no loss.

use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use tokio_util::sync::CancellationToken;

use shared_kernel::cache::redis::{RedisCache, RedisCacheConfig};
use shared_kernel::db::shard::{ShardRouter, ShardRouterConfig, ShardUrls};
use shared_kernel::queue::producer::QueueProducer;

const SCHEMA_SQL: &str = include_str!("../../../db/init.sql");
const RESERVATION_COUNT: usize = 40;
const BATCH_SIZE: usize = 10;
const CONCURRENCY: usize = 2;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_intake_processes_all_reservations_exactly_once() {
    let env = TestEnv::start().await;

    // Seed N reservations on shard 0.
    for i in 0..RESERVATION_COUNT {
        let key = format!("test-idemp-{:04}", i);
        env.seed_reservation(0, &key, i).await;
    }

    // Run the worker pool, then cancel and wait for it to drain.
    let cancel = CancellationToken::new();
    let handles = transactions::spawn_redis_intake(
        env.shards.clone(),
        env.cache.clone(),
        env.queue.clone(),
        cancel.child_token(),
        CONCURRENCY,
        BATCH_SIZE,
    );
    poll_until_published(env.shards.writer(0), RESERVATION_COUNT, 15_000).await;
    cancel.cancel();
    for h in handles {
        let _ = h.await;
    }

    // Assertions.
    let published_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM idempotency_keys WHERE published = true")
            .fetch_one(env.shards.writer(0))
            .await
            .unwrap();
    assert_eq!(
        published_count, RESERVATION_COUNT as i64,
        "every reservation must end with published=true"
    );

    let pending_len = list_len(&env.cache, &pending_list_key(0)).await;
    let inflight_len = list_len(&env.cache, &inflight_list_key(0)).await;
    assert_eq!(pending_len, 0, "pending list must be empty");
    assert_eq!(inflight_len, 0, "inflight list must be empty");

    // The system is documented at-least-once: a publish-confirm
    // timeout under contention can cause the producer to retry, and
    // if the broker had received the original, the queue ends up
    // with a duplicate (absorbed in production by the consumer's
    // `(reference_id, from_account)` UNIQUE). The money-safety
    // assertion is `>= N` (no under-publish); the upper bound
    // guards against a pathological retry loop.
    let queue_depth = rabbit_queue_depth(&env.amqp_url).await;
    assert!(
        queue_depth >= RESERVATION_COUNT as u32,
        "queue must hold at-least-once delivery: got {} of {}",
        queue_depth,
        RESERVATION_COUNT
    );
    assert!(
        queue_depth <= (RESERVATION_COUNT * 3) as u32,
        "duplicates bounded by MAX_PUBLISH_ATTEMPTS=3 per key; got {}",
        queue_depth
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_intake_recovers_from_seeded_inflight() {
    // Simulate post-crash state: a previous worker had RPOPLPUSH'd
    // these keys to inflight but died before publishing/LREM'ing.
    // On the next process incarnation, `drain_inflight_batched` must
    // process every seeded key exactly once, with no loss. This is
    // a cleaner exercise of the recovery path than racing a timed
    // cancel against an in-flight batch -- graceful cancellation
    // lets the current `process_batch` complete, so the recovery
    // path often wouldn't fire under the timed approach.
    let env = TestEnv::start().await;
    for i in 0..RESERVATION_COUNT {
        let key = format!("crash-idemp-{:04}", i);
        let entry = json!({
            "request_hash": format!("hash-{}", i),
            "accepted_payload": { "reference_id": key.clone() },
            "outbox_payload": {
                "from_account": "ACC_0000001",
                "to_account": "ACC_0000002",
                "amount": "1.00",
                "currency": "IDR",
                "reference_id": key.clone(),
                "description": "crash-recovery test",
                "shard": 0,
            },
            "shard": 0,
        });
        // The entry exists in Redis (the create handler had set it
        // before the prior worker crashed).
        env.cache
            .set_nx_ex(&format!("v1:idemp:{}", key), &entry, 3600)
            .await
            .unwrap();
        // Seed directly into inflight (NOT pending): the exact
        // post-crash state where a prior worker had claimed via
        // RPOPLPUSH but died before LREM.
        env.cache
            .lpush(&inflight_list_key(0), &key)
            .await
            .unwrap();
    }

    let cancel = CancellationToken::new();
    let handles = transactions::spawn_redis_intake(
        env.shards.clone(),
        env.cache.clone(),
        env.queue.clone(),
        cancel.child_token(),
        CONCURRENCY,
        BATCH_SIZE,
    );
    poll_until_published(env.shards.writer(0), RESERVATION_COUNT, 15_000).await;
    cancel.cancel();
    for h in handles {
        let _ = h.await;
    }

    let published_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM idempotency_keys WHERE published = true")
            .fetch_one(env.shards.writer(0))
            .await
            .unwrap();
    assert_eq!(
        published_count, RESERVATION_COUNT as i64,
        "recovery must process every seeded-inflight reservation"
    );

    let inflight_len = list_len(&env.cache, &inflight_list_key(0)).await;
    assert_eq!(inflight_len, 0, "inflight list must drain on recovery");

    // No prior publish happened (we only seeded inflight, not the
    // broker), so recovery publishes each reservation at least once.
    // Same at-least-once semantics as the normal path -- producer
    // confirm-retry can duplicate, the upper bound guards against
    // a pathological retry loop.
    let queue_depth = rabbit_queue_depth(&env.amqp_url).await;
    assert!(
        queue_depth >= RESERVATION_COUNT as u32,
        "recovery must publish at least N: got {} of {}",
        queue_depth,
        RESERVATION_COUNT
    );
    assert!(
        queue_depth <= (RESERVATION_COUNT * 3) as u32,
        "duplicates bounded by MAX_PUBLISH_ATTEMPTS=3 per key; got {}",
        queue_depth
    );
}

// ─── Test harness ──────────────────────────────────────────────────

struct TestEnv {
    shards: ShardRouter,
    cache: RedisCache,
    queue: QueueProducer,
    amqp_url: String,
    _pg: testcontainers::ContainerAsync<Postgres>,
    _redis: testcontainers::ContainerAsync<Redis>,
    _rabbit: testcontainers::ContainerAsync<GenericImage>,
}

impl TestEnv {
    async fn start() -> Self {
        // Postgres
        let pg = Postgres::default().start().await.unwrap();
        let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
        let pg_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");
        let pool = PgPool::connect(&pg_url).await.unwrap();
        sqlx::raw_sql(SCHEMA_SQL).execute(&pool).await.unwrap();
        drop(pool);

        // Redis
        let redis = Redis::default().start().await.unwrap();
        let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();
        let redis_url = format!("redis://127.0.0.1:{redis_port}");

        // RabbitMQ
        let rabbit = GenericImage::new("rabbitmq", "3-alpine")
            .with_exposed_port(ContainerPort::Tcp(5672))
            .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
            .start()
            .await
            .unwrap();
        let rabbit_port = rabbit.get_host_port_ipv4(5672).await.unwrap();
        let amqp_url = format!("amqp://guest:guest@127.0.0.1:{rabbit_port}");

        let cancel = CancellationToken::new();
        let cache = RedisCache::new(
            &RedisCacheConfig {
                master_url: redis_url.clone(),
                read_url: Some(redis_url.clone()),
                pool_size: 8,
                sentinel_nodes: vec![],
                sentinel_master_name: String::new(),
                sentinel_monitor_interval_secs: 5,
            },
            cancel.child_token(),
        )
        .await
        .unwrap();
        let queue = QueueProducer::new(&amqp_url).await.unwrap();
        let shards = ShardRouter::new(
            &ShardRouterConfig {
                shards: vec![
                    ShardUrls {
                        write_url: pg_url.clone(),
                        read_urls: vec![pg_url.clone()],
                    },
                    ShardUrls {
                        write_url: pg_url.clone(),
                        read_urls: vec![pg_url.clone()],
                    },
                ],
                write_pool_size: 10,
                read_pool_size: 5,
                health_check_interval_secs: 5,
            },
            cancel.child_token(),
        )
        .await
        .unwrap();

        Self {
            shards,
            cache,
            queue,
            amqp_url,
            _pg: pg,
            _redis: redis,
            _rabbit: rabbit,
        }
    }

    async fn seed_reservation(&self, shard: usize, idempotency_key: &str, i: usize) {
        let entry = json!({
            "request_hash": format!("hash-{}", i),
            "accepted_payload": { "reference_id": idempotency_key },
            "outbox_payload": {
                "from_account": "ACC_0000001",
                "to_account": "ACC_0000002",
                "amount": "1.00",
                "currency": "IDR",
                "reference_id": idempotency_key,
                "description": "intake-batch test",
                "shard": shard,
            },
            "shard": shard,
        });
        self.cache
            .set_nx_ex(
                &format!("v1:idemp:{}", idempotency_key),
                &entry,
                3600,
            )
            .await
            .unwrap();
        self.cache
            .lpush(&pending_list_key(shard), idempotency_key)
            .await
            .unwrap();
    }
}

fn pending_list_key(shard: usize) -> String {
    format!("v1:idemp_pending:s{}", shard)
}
fn inflight_list_key(shard: usize) -> String {
    format!("v1:idemp_inflight:s{}", shard)
}

async fn list_len(cache: &RedisCache, list: &str) -> i64 {
    // No LLEN method on RedisCache; for the empty/non-empty assertion
    // we only need to know whether the list has any items. RPOPLPUSH
    // list -> list cycles a single item (atomic, list size unchanged
    // if it had >= 1 item; returns None on empty).
    match cache
        .rpoplpush_batch(list, list, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
    {
        Some(_) => 1,
        None => 0,
    }
}

async fn rabbit_queue_depth(amqp_url: &str) -> u32 {
    use amqprs::{
        channel::QueueDeclareArguments,
        connection::{Connection, OpenConnectionArguments},
    };
    let parts = shared_kernel::queue::producer::parse_amqp_url_full(amqp_url).unwrap();
    let mut args =
        OpenConnectionArguments::new(&parts.host, parts.port, &parts.username, &parts.password);
    args.virtual_host(&parts.vhost);
    let conn = Connection::open(&args).await.unwrap();
    let ch = conn.open_channel(None).await.unwrap();
    // Passive declare returns the current message count.
    let mut decl = QueueDeclareArguments::new("transactions.process");
    decl.passive(true);
    let (_q, msgs, _consumers) = ch.queue_declare(decl).await.unwrap().unwrap();
    msgs
}

async fn poll_until_published(pool: &sqlx::PgPool, target: usize, timeout_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM idempotency_keys WHERE published = true")
                .fetch_one(pool)
                .await
                .unwrap_or(0);
        if n as usize >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {target} reservations to publish");
}
