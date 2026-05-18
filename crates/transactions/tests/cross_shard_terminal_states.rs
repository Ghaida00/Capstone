//! T-9: cross-shard outbox terminal-state integration tests.
//!
//! The cross-shard processor has a deliberately asymmetric
//! terminal-state design (R-2 deep-dive): the credit path
//! terminal-fails after `MAX_ATTEMPTS`, the refund path never
//! does. That design is load-bearing for money safety and was
//! previously exercised by zero tests. These three cover the
//! highest-financial-risk uncovered paths:
//!
//!   1. `credit_path_terminal_fails_after_max_attempts` — outbox
//!      row reaches `status='failed'` with the max-attempts error.
//!   2. `refund_path_holds_at_max_attempts_with_extended_lease` —
//!      refund row stays `status='pending'`, attempts held just
//!      below the cap, lease deferred far into the future, and
//!      the sender audit row stays `'processing'` (refund-stuck
//!      is still in-flight from the customer's view).
//!   3. `sender_audit_row_transitions_to_failed_after_credit_terminal_fail`
//!      — the R-8 contract: a credit terminal-fail flips the
//!      sender's `transactions` audit row to `'failed'` with
//!      `failure_reason` populated (was: deliberately left at
//!      `'processing'`; R-8 inverted that).
//!
//! Deterministic failure injection without flaky container-stop
//! timing:
//!   * credit failure — `DROP TABLE cross_shard_outbox_applied`
//!     so the very first statement in `apply_on_receiver`
//!     (`INSERT INTO cross_shard_outbox_applied`) raises SQLSTATE
//!     42P01 on every attempt.
//!   * refund failure — seed the sender balance near the
//!     `DECIMAL(18,2)` ceiling so the refund's `balance + amount`
//!     overflows with SQLSTATE 22003 on every attempt.
//!
//! Metric note: the `cross_shard_outbox_terminal_failures_total`
//! / `cross_shard_refund_stuck_total` increments live on the same
//! code lines as the durable DB writes asserted here. Asserting
//! the counter value would need a process-global Prometheus
//! recorder, which cannot be installed once-per-test across three
//! tests in one binary; the durable DB state is the authoritative
//! contract and proves the same branch executed.

use std::time::Duration;

use sqlx::PgPool;
use sqlx::Row;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::db::shard::{ShardRouter, ShardRouterConfig, ShardUrls};

const SCHEMA_SQL: &str = include_str!("../../../db/init.sql");

/// Max poll budget for a terminal state. 10 attempts × 250 ms poll
/// (×2 because the degenerate 2-shard config drains the row from
/// both shard slots per tick) is well under 10 s; 30 s is generous
/// slack for slow CI Docker.
const TERMINAL_DEADLINE: Duration = Duration::from_secs(30);
const POLL_STEP: Duration = Duration::from_millis(250);

struct Fixture {
    pool: PgPool,
    shards: ShardRouter,
    cancel: CancellationToken,
    // Container guard — dropped at end of test to tear down.
    _pg: ContainerAsync<Postgres>,
}

async fn setup() -> Fixture {
    let pg = Postgres::default().start().await.unwrap();
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let pg_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");

    let pool = PgPool::connect(&pg_url).await.unwrap();
    sqlx::raw_sql(SCHEMA_SQL).execute(&pool).await.unwrap();

    let cancel = CancellationToken::new();
    // Degenerate 2-shard config (mirrors event_flow.rs): both
    // slots point at the same PG so routing is valid but local.
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

    Fixture {
        pool,
        shards,
        cancel,
        _pg: pg,
    }
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

/// Insert a sender-side `transactions` audit row in `'processing'`
/// — the state the consumer leaves it in while the cross-shard
/// outbox row is still being drained.
async fn seed_processing_audit_row(pool: &PgPool, reference_id: &str, from_account: &str) {
    sqlx::query(
        r#"INSERT INTO transactions
           (id, from_account, to_account, amount, currency, status, reference_id)
           VALUES ($1, $2, 'RECIPIENT', 100.00, 'IDR', 'processing', $3)"#,
    )
    .bind(Uuid::new_v4())
    .bind(from_account)
    .bind(reference_id)
    .execute(pool)
    .await
    .expect("seed processing audit row");
}

#[allow(clippy::too_many_arguments)]
async fn insert_outbox_row(
    pool: &PgPool,
    from_account: &str,
    to_account: &str,
    to_shard: i32,
    amount: &str,
    reference_id: &str,
    refund_required: bool,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO cross_shard_outbox
           (id, from_account, to_account, to_shard, amount, currency,
            reference_id, status, refund_required, attempts)
           VALUES ($1, $2, $3, $4, $5::numeric, 'IDR', $6, 'pending', $7, 0)"#,
    )
    .bind(id)
    .bind(from_account)
    .bind(to_account)
    .bind(to_shard)
    .bind(amount)
    .bind(reference_id)
    .bind(refund_required)
    .execute(pool)
    .await
    .expect("insert outbox row");
    id
}

/// Poll `cross_shard_outbox` for `id` until `predicate(status,
/// attempts, lease_secs_from_now)` is true or the deadline passes.
/// Returns the final observed `(status, attempts, lease_secs)`.
async fn poll_outbox_until<F>(pool: &PgPool, id: Uuid, predicate: F) -> (String, i32, f64)
where
    F: Fn(&str, i32, f64) -> bool,
{
    let start = std::time::Instant::now();
    let mut last = (String::new(), -1, f64::MIN);
    while start.elapsed() < TERMINAL_DEADLINE {
        let row = sqlx::query(
            "SELECT status, attempts, \
             COALESCE(EXTRACT(EPOCH FROM (lease_until - NOW())), -1e9) AS lease_secs \
             FROM cross_shard_outbox WHERE id = $1",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read outbox row");
        let status: String = row.get("status");
        let attempts: i32 = row.get("attempts");
        let lease_secs: f64 = row.get("lease_secs");
        last = (status.clone(), attempts, lease_secs);
        if predicate(&status, attempts, lease_secs) {
            return last;
        }
        tokio::time::sleep(POLL_STEP).await;
    }
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credit_path_terminal_fails_after_max_attempts() {
    let fx = setup().await;

    seed_user(&fx.pool, "SENDER-CR", "1000000.00").await;
    // Force every credit attempt to error: the first statement in
    // apply_on_receiver inserts into this table.
    sqlx::query("DROP TABLE cross_shard_outbox_applied")
        .execute(&fx.pool)
        .await
        .expect("drop dedupe table");

    let reference_id = format!("t9-credit-{}", Uuid::new_v4());
    let id = insert_outbox_row(
        &fx.pool,
        "SENDER-CR",
        "RECIPIENT-CR",
        1,
        "500.00",
        &reference_id,
        false,
    )
    .await;

    let _h = transactions::spawn_cross_shard_processor(fx.shards.clone(), fx.cancel.child_token());

    let (status, attempts, _) = poll_outbox_until(&fx.pool, id, |s, _, _| s == "failed").await;

    assert_eq!(
        status, "failed",
        "credit path must terminal-fail the outbox row after MAX_ATTEMPTS (attempts={attempts})"
    );
    assert!(
        attempts >= 9,
        "attempts should have climbed to the cap, got {attempts}"
    );
    let last_error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM cross_shard_outbox WHERE id = $1")
            .bind(id)
            .fetch_one(&fx.pool)
            .await
            .unwrap();
    let last_error = last_error.unwrap_or_default();
    assert!(
        last_error.contains("max attempts at credit"),
        "last_error should record the credit terminal cause, got: {last_error}"
    );

    fx.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refund_path_holds_at_max_attempts_with_extended_lease() {
    let fx = setup().await;

    // Balance near the DECIMAL(18,2) ceiling (max 9999999999999999.99)
    // so the refund's `balance + amount` overflows (SQLSTATE 22003)
    // on every attempt.
    seed_user(&fx.pool, "SENDER-RF", "9999999999999990.00").await;
    let reference_id = format!("t9-refund-{}", Uuid::new_v4());
    // The audit row the refund CTE updates; must exist and be
    // non-'reversed' so the CTE reaches the overflowing balance UPDATE.
    seed_processing_audit_row(&fx.pool, &reference_id, "SENDER-RF").await;

    let id = insert_outbox_row(
        &fx.pool,
        "SENDER-RF",
        "RECIPIENT-RF",
        1,
        "1000000.00",
        &reference_id,
        true, // refund path
    )
    .await;

    let _h = transactions::spawn_cross_shard_processor(fx.shards.clone(), fx.cancel.child_token());

    // Stuck state: attempts held at MAX-1 (so the `attempts <
    // MAX_ATTEMPTS` claim filter keeps selecting it), status still
    // 'pending', lease pushed far out by REFUND_STUCK_BACKOFF_SECS
    // (300s). Wait for the deferred lease to confirm the stuck arm.
    let (status, attempts, lease_secs) = poll_outbox_until(&fx.pool, id, |s, a, l| {
        s == "pending" && a >= 9 && l > 250.0
    })
    .await;

    assert_eq!(
        status, "pending",
        "refund path must NEVER terminal-fail (sender is debited, only a refund makes them whole)"
    );
    assert!(
        attempts >= 9,
        "attempts should be held at the cap (MAX-1), got {attempts}"
    );
    assert!(
        lease_secs > 250.0,
        "refund-stuck must defer the lease by ~REFUND_STUCK_BACKOFF_SECS (300s), got {lease_secs:.0}s"
    );

    // The sender audit row must STAY 'processing' — refund-stuck is
    // still in-flight from the customer's view (contrast R-8 credit).
    let audit_status: String = sqlx::query_scalar(
        "SELECT status FROM transactions WHERE reference_id = $1 AND from_account = $2",
    )
    .bind(&reference_id)
    .bind("SENDER-RF")
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    assert_eq!(
        audit_status, "processing",
        "refund-stuck must leave the sender audit row at 'processing' (it is not terminal)"
    );

    fx.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sender_audit_row_transitions_to_failed_after_credit_terminal_fail() {
    // R-8 contract test. Pre-R-8 this asserted the row was LEFT at
    // 'processing'; R-8 inverted the contract — it must now reach a
    // terminal 'failed' state with a machine-readable reason.
    let fx = setup().await;

    seed_user(&fx.pool, "SENDER-R8", "1000000.00").await;
    let reference_id = format!("t9-r8-{}", Uuid::new_v4());
    seed_processing_audit_row(&fx.pool, &reference_id, "SENDER-R8").await;

    sqlx::query("DROP TABLE cross_shard_outbox_applied")
        .execute(&fx.pool)
        .await
        .expect("drop dedupe table");

    let id = insert_outbox_row(
        &fx.pool,
        "SENDER-R8",
        "RECIPIENT-R8",
        1,
        "500.00",
        &reference_id,
        false,
    )
    .await;

    let _h = transactions::spawn_cross_shard_processor(fx.shards.clone(), fx.cancel.child_token());

    // Precondition: the outbox row terminal-fails.
    let (status, _, _) = poll_outbox_until(&fx.pool, id, |s, _, _| s == "failed").await;
    assert_eq!(status, "failed", "precondition: outbox must terminal-fail");

    // R-8 contract: the sender audit row is *eventually* 'failed'.
    // mark_failed (outbox) and mark_sender_failed (audit row) are
    // two sequential awaits in handle_attempt_error — the outbox
    // flip is observable strictly before the audit flip completes,
    // so we must poll the audit row for the condition rather than
    // snapshot it the instant the outbox flips (under concurrent
    // load the processor task can be descheduled between the two
    // awaits long enough to lose that race). Condition-based wait,
    // not a fixed sleep.
    let start = std::time::Instant::now();
    let mut audit_status = String::new();
    let mut failure_reason: Option<String> = None;
    let mut processed_at: Option<chrono::DateTime<chrono::Utc>> = None;
    while start.elapsed() < TERMINAL_DEADLINE {
        let row = sqlx::query(
            "SELECT status, failure_reason, processed_at \
             FROM transactions WHERE reference_id = $1 AND from_account = $2",
        )
        .bind(&reference_id)
        .bind("SENDER-R8")
        .fetch_one(&fx.pool)
        .await
        .unwrap();
        audit_status = row.get("status");
        failure_reason = row.get("failure_reason");
        processed_at = row.get("processed_at");
        if audit_status == "failed" {
            break;
        }
        tokio::time::sleep(POLL_STEP).await;
    }

    assert_eq!(
        audit_status, "failed",
        "R-8: credit terminal-fail must flip the sender audit row 'processing' -> 'failed'"
    );
    let failure_reason = failure_reason.unwrap_or_default();
    assert!(
        failure_reason.contains("max attempts at credit"),
        "R-8: failure_reason must carry the terminal cause, got: {failure_reason}"
    );
    assert!(
        processed_at.is_some(),
        "R-8: processed_at must be stamped so 'time in processing' stops climbing"
    );

    fx.cancel.cancel();
}
