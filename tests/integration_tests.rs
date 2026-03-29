//! Integration tests using `testcontainers` for ephemeral infrastructure.
//!
//! These tests spin up real PostgreSQL and Redis instances in Docker,
//! run the schema migration, then exercise the full data path.
//!
//! Requires Docker to be running. Skip with `--skip integration`.

mod common;

use sqlx::Row;
use uuid::Uuid;

/// Insert a transaction directly via SQL, then read it back and verify fields.
#[tokio::test]
async fn insert_and_read_transaction() {
    let (pool, _pg_container) = common::spawn_postgres().await;

    let id = Uuid::new_v4();
    let from = "ACC-INT-001";
    let to = "ACC-INT-002";

    // Insert
    sqlx::query(
        r#"INSERT INTO transactions (id, from_account, to_account, amount, currency, status, reference_id)
           VALUES ($1, $2, $3, 100000.00, 'IDR', 'pending', $4)"#,
    )
    .bind(id)
    .bind(from)
    .bind(to)
    .bind(format!("ref-{}", id))
    .execute(&pool)
    .await
    .expect("Insert should succeed");

    // Read back
    let row = sqlx::query("SELECT * FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("Should find the inserted transaction");

    assert_eq!(row.get::<Uuid, _>("id"), id);
    assert_eq!(row.get::<String, _>("from_account"), from);
    assert_eq!(row.get::<String, _>("to_account"), to);
    assert_eq!(row.get::<String, _>("status"), "pending");
}

/// Verify that a SQL transaction rollback leaves no trace in the database.
#[tokio::test]
async fn rollback_leaves_no_data() {
    let (pool, _pg_container) = common::spawn_postgres().await;

    let id = Uuid::new_v4();

    // Start a transaction, insert, then rollback
    let mut txn = pool.begin().await.expect("Begin transaction");

    sqlx::query(
        r#"INSERT INTO transactions (id, from_account, to_account, amount, currency, status)
           VALUES ($1, 'rollback-from', 'rollback-to', 50000.00, 'IDR', 'pending')"#,
    )
    .bind(id)
    .execute(&mut *txn)
    .await
    .expect("Insert in txn should succeed");

    // Explicit rollback
    txn.rollback().await.expect("Rollback should succeed");

    // Verify the row does NOT exist
    let row = sqlx::query("SELECT id FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .expect("Query should succeed");

    assert!(
        row.is_none(),
        "Rolled-back transaction should not be visible"
    );
}

/// Verify that the schema constraints work correctly:
/// - amount must be > 0
/// - status must be one of the allowed values
#[tokio::test]
async fn schema_constraints_enforced() {
    let (pool, _pg_container) = common::spawn_postgres().await;

    // Negative amount should fail
    let result = sqlx::query(
        r#"INSERT INTO transactions (from_account, to_account, amount, currency, status)
           VALUES ('a', 'b', -100.00, 'IDR', 'pending')"#,
    )
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "Negative amount should be rejected by CHECK constraint"
    );

    // Invalid status should fail
    let result = sqlx::query(
        r#"INSERT INTO transactions (from_account, to_account, amount, currency, status)
           VALUES ('a', 'b', 100.00, 'IDR', 'invalid_status')"#,
    )
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "Invalid status should be rejected by CHECK constraint"
    );
}
