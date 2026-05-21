//! Integration tests for the `apply_transactions_batch` PL/pgSQL function.
//!
//! Spins up a Postgres testcontainer, applies the consolidated
//! schema (`db/init.sql`), seeds a small `users` fixture, then calls
//! the function with hand-crafted batches covering each outcome:
//!   - `completed`  — same-shard happy path
//!   - `failed`     — insufficient balance / sender inactive
//!   - `failed`     — same-shard recipient missing → atomic refund
//!   - `processing` — cross-shard (receiver_shard != sender_shard)
//!   - `skipped`    — intra-batch (reference_id, from_account) duplicate
//!
//! Money-safety assertions accompany each: balances are checked
//! after the call to verify the function preserved the per-row
//! debit / credit / refund semantics the Rust consumer used to
//! implement statement-by-statement.

use rust_decimal::Decimal;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

const SCHEMA_SQL: &str = include_str!("../../../db/init.sql");

#[derive(sqlx::FromRow, Debug)]
struct OutcomeRow {
    idx: i32,
    outcome: String,
    assigned_id: Option<Uuid>,
}

/// Bring up a fresh Postgres testcontainer, apply the consolidated
/// schema, and seed the `users` fixture used across the tests.
/// Returns the container guard (drop = teardown) and the pool.
async fn setup_db() -> (ContainerAsync<Postgres>, PgPool) {
    let pg = Postgres::default().start().await.unwrap();
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let pg_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");

    let pool = PgPool::connect(&pg_url).await.unwrap();
    sqlx::raw_sql(SCHEMA_SQL).execute(&pool).await.unwrap();

    sqlx::query(
        r#"INSERT INTO users (account_number, full_name, balance, status) VALUES
             ('ACC_TEST_SENDER1',    'Test Sender 1', 100.00, 'active'),
             ('ACC_TEST_SENDER2',    'Test Sender 2',  50.00, 'active'),
             ('ACC_TEST_RECVR_SAME', 'Test Recvr',      0.00, 'active')"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    (pg, pool)
}

/// Call `apply_transactions_batch` with one slice per array argument.
#[allow(clippy::too_many_arguments)]
async fn call_apply(
    pool: &PgPool,
    ids: &[Uuid],
    outbox_ids: &[Uuid],
    from_accounts: &[String],
    to_accounts: &[String],
    amounts: &[Decimal],
    currencies: &[String],
    reference_ids: &[String],
    descriptions: &[Option<String>],
    receiver_shards: &[i32],
    sender_shard: i32,
) -> Vec<OutcomeRow> {
    sqlx::query_as(
        r#"SELECT idx, outcome, assigned_id
           FROM apply_transactions_batch(
               $1::uuid[], $2::uuid[], $3::text[], $4::text[],
               $5::numeric[], $6::text[], $7::text[], $8::text[],
               $9::int[], $10::int
           )"#,
    )
    .bind(ids)
    .bind(outbox_ids)
    .bind(from_accounts)
    .bind(to_accounts)
    .bind(amounts)
    .bind(currencies)
    .bind(reference_ids)
    .bind(descriptions)
    .bind(receiver_shards)
    .bind(sender_shard)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn apply_batch_returns_all_four_outcome_classes() {
    let (_pg, pool) = setup_db().await;

    // 4-message batch:
    //  idx 1: same-shard happy path → 'completed'
    //  idx 2: insufficient balance  → 'failed'
    //  idx 3: cross-shard (recv_shard=1, sender_shard=0) → 'processing'
    //  idx 4: duplicate (ref,from) of idx 1 → 'skipped'
    let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    let outbox_ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    let from_accounts = vec![
        "ACC_TEST_SENDER1".to_string(),
        "ACC_TEST_SENDER2".to_string(),
        "ACC_TEST_SENDER1".to_string(),
        "ACC_TEST_SENDER1".to_string(),
    ];
    let to_accounts = vec!["ACC_TEST_RECVR_SAME".to_string(); 4];
    let amounts: Vec<Decimal> = vec![
        Decimal::new(1000, 2),  // 10.00
        Decimal::new(10000, 2), // 100.00 — exceeds SENDER2's 50.00
        Decimal::new(500, 2),   // 5.00 cross-shard
        Decimal::new(2500, 2),  // 25.00 — collides with idx 1 (ref, from)
    ];
    let currencies = vec!["IDR".to_string(); 4];
    let reference_ids = vec![
        "ref-1".to_string(),
        "ref-2".to_string(),
        "ref-3".to_string(),
        "ref-1".to_string(), // duplicate of idx 1
    ];
    let descriptions: Vec<Option<String>> = vec![None; 4];
    let receiver_shards = vec![0_i32, 0, 1, 0];

    let rows = call_apply(
        &pool,
        &ids,
        &outbox_ids,
        &from_accounts,
        &to_accounts,
        &amounts,
        &currencies,
        &reference_ids,
        &descriptions,
        &receiver_shards,
        0,
    )
    .await;

    assert_eq!(rows.len(), 4, "one row per input message");
    let by_idx: std::collections::HashMap<i32, &OutcomeRow> =
        rows.iter().map(|r| (r.idx, r)).collect();
    assert_eq!(by_idx[&1].outcome, "completed");
    assert_eq!(by_idx[&2].outcome, "failed");
    assert_eq!(by_idx[&3].outcome, "processing");
    assert_eq!(by_idx[&4].outcome, "skipped");

    // SENDER1: 100 - 10 (idx 1) - 5 (idx 3 cross-shard debit) = 85.00
    let s1: Decimal =
        sqlx::query_scalar("SELECT balance FROM users WHERE account_number = 'ACC_TEST_SENDER1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(s1, Decimal::new(8500, 2));

    // SENDER2 unchanged (idx 2 debit failed insufficient).
    let s2: Decimal =
        sqlx::query_scalar("SELECT balance FROM users WHERE account_number = 'ACC_TEST_SENDER2'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(s2, Decimal::new(5000, 2));

    // RECVR credited only by idx 1's same-shard credit (idx 3 is queued).
    let r: Decimal = sqlx::query_scalar(
        "SELECT balance FROM users WHERE account_number = 'ACC_TEST_RECVR_SAME'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(r, Decimal::new(1000, 2));

    // idx 3 queued a pending cross_shard_outbox row.
    let outbox: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cross_shard_outbox WHERE reference_id = 'ref-3' AND status = 'pending'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outbox, 1);
}

#[tokio::test]
async fn apply_batch_refunds_sender_on_same_shard_credit_miss() {
    let (_pg, pool) = setup_db().await;

    let id = Uuid::new_v4();
    let outbox_id = Uuid::new_v4();
    let rows = call_apply(
        &pool,
        &[id],
        &[outbox_id],
        &["ACC_TEST_SENDER1".to_string()],
        &["ACC_DOES_NOT_EXIST".to_string()],
        &[Decimal::new(2000, 2)], // 20.00
        &["IDR".to_string()],
        &["refund-test-1".to_string()],
        &[None],
        &[0_i32],
        0,
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "failed");
    assert_eq!(rows[0].assigned_id, Some(id));

    // Sender debited then refunded → net unchanged.
    let s1: Decimal =
        sqlx::query_scalar("SELECT balance FROM users WHERE account_number = 'ACC_TEST_SENDER1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(s1, Decimal::new(10000, 2));

    let status: String = sqlx::query_scalar("SELECT status FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "failed");
}

#[tokio::test]
async fn apply_batch_intra_batch_same_sender_sees_running_balance() {
    let (_pg, pool) = setup_db().await;

    // SENDER1 (balance 100) debits 60 then 50 in one batch:
    //  idx 1: 60.00 → 'completed', balance now 40
    //  idx 2: 50.00 → 'failed' (insufficient against running 40)
    let ids: Vec<Uuid> = (0..2).map(|_| Uuid::new_v4()).collect();
    let outbox_ids: Vec<Uuid> = (0..2).map(|_| Uuid::new_v4()).collect();
    let rows = call_apply(
        &pool,
        &ids,
        &outbox_ids,
        &["ACC_TEST_SENDER1".to_string(), "ACC_TEST_SENDER1".to_string()],
        &vec!["ACC_TEST_RECVR_SAME".to_string(); 2],
        &[Decimal::new(6000, 2), Decimal::new(5000, 2)],
        &vec!["IDR".to_string(); 2],
        &["partial-1".to_string(), "partial-2".to_string()],
        &[None, None],
        &[0_i32, 0],
        0,
    )
    .await;

    assert_eq!(rows.len(), 2);
    let by_idx: std::collections::HashMap<i32, &OutcomeRow> =
        rows.iter().map(|r| (r.idx, r)).collect();
    assert_eq!(by_idx[&1].outcome, "completed");
    assert_eq!(by_idx[&2].outcome, "failed");

    let s1: Decimal =
        sqlx::query_scalar("SELECT balance FROM users WHERE account_number = 'ACC_TEST_SENDER1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(s1, Decimal::new(4000, 2), "only the first debit landed");
}
