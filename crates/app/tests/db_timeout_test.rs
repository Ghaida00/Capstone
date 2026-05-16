//! WP1 / findings D-1 + R-3: every pooled connection must enforce
//! statement_timeout, lock_timeout, idle_in_transaction_session_timeout.

mod common;

use shared_kernel::db::pool::{DatabasePool, PoolTimeouts};

/// statement_timeout: a query longer than the configured budget is
/// cancelled by Postgres (SQLSTATE 57014 query_canceled), not run to
/// completion.
#[tokio::test]
async fn statement_timeout_cancels_overlong_query() {
    let (_seed_pool, url, _container) = common::spawn_postgres_with_url().await;

    let timeouts = PoolTimeouts {
        statement_ms: 500,
        lock_ms: 500,
        idle_in_tx_ms: 5_000,
    };

    let pool = DatabasePool::new_shard(&url, &[], 5, 5, timeouts)
        .await
        .expect("pool builds");

    let err = sqlx::query("SELECT pg_sleep(2)")
        .execute(pool.writer())
        .await
        .expect_err("query should be cancelled by statement_timeout");

    let db_err = err
        .as_database_error()
        .expect("a database error, not a pool/io error");
    assert_eq!(
        db_err.code().as_deref(),
        Some("57014"),
        "expected SQLSTATE 57014 query_canceled, got: {db_err:?}"
    );
}

/// The GUC is actually set on the session (defensive: proves it's the
/// connection option doing the work, not a fluke).
#[tokio::test]
async fn statement_timeout_guc_is_set_on_session() {
    let (_seed_pool, url, _container) = common::spawn_postgres_with_url().await;

    let pool = DatabasePool::new_shard(
        &url,
        &[],
        5,
        5,
        PoolTimeouts { statement_ms: 1_234, lock_ms: 500, idle_in_tx_ms: 5_000 },
    )
    .await
    .expect("pool builds");

    let shown: (String,) = sqlx::query_as("SHOW statement_timeout")
        .fetch_one(pool.writer())
        .await
        .expect("SHOW works");

    assert_ne!(shown.0, "0", "statement_timeout must not be disabled");
}
