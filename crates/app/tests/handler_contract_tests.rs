//! HTTP-level contract tests for module sub-routers — the
//! `axum-test` surface the audit's T-5 named.
//!
//! Strategy: build a router via the public `router(deps)` entry
//! point with a fake port impl (`FakeNotificationLog` here) so
//! the test exercises the real Axum extractor stack, the real
//! JSON shaping in `ApiResponse`, and the real error-mapping
//! (`NotificationError → AppError → HTTP`) without spinning up
//! Redis or Postgres.
//!
//! The transactions and accounts handlers also belong in this
//! file in principle. They are not here because their `Deps`
//! structs hold a concrete `shared_kernel::cache::redis::RedisCache`
//! (and, for accounts, a `moka::future::Cache`) that cannot be
//! constructed against an unreachable Redis quickly enough for a
//! unit-test budget — the `health_check()` inside
//! `RedisCache::new` blocks on the OS TCP-connect timeout. Adding
//! a `trait Cache` seam to the Deps structs would close that gap;
//! it is a deliberate follow-on (see remediation-impact-log.md
//! T-5 row). The transactions create-handler is meanwhile covered
//! end-to-end by `crates/transactions/tests/event_flow.rs`, and
//! the cross-shard terminal-state branch by
//! `crates/transactions/tests/cross_shard_terminal_states.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum_test::TestServer;
use chrono::Utc;
use uuid::Uuid;

use notifications::ports::{
    NotificationEntry, NotificationError, NotificationKind, NotificationLog,
};

/// Drop-in `NotificationLog` for tests — returns the entries the
/// test seeded, or a controlled error.
#[derive(Clone)]
struct FakeNotificationLog {
    behavior: Behavior,
}

#[derive(Clone)]
enum Behavior {
    Returns(Vec<NotificationEntry>),
    Infra(&'static str),
}

#[async_trait]
impl NotificationLog for FakeNotificationLog {
    async fn recent(
        &self,
        limit: usize,
    ) -> Result<Vec<NotificationEntry>, NotificationError> {
        match &self.behavior {
            Behavior::Returns(v) => Ok(v.iter().take(limit).cloned().collect()),
            Behavior::Infra(msg) => Err(NotificationError::Infra((*msg).to_owned())),
        }
    }
}

fn make_entry(recipient: &str) -> NotificationEntry {
    NotificationEntry {
        id: Uuid::new_v4(),
        kind: NotificationKind::TransactionCommitted,
        recipient: recipient.to_owned(),
        summary: format!("Received 1.00 IDR for {recipient}"),
        payload: serde_json::json!({}),
        created_at: Utc::now(),
    }
}

fn router_with_log(log: FakeNotificationLog) -> axum::Router {
    notifications::router(notifications::NotificationsDeps {
        log: Arc::new(log) as Arc<dyn NotificationLog>,
    })
}

// ─── GET /recent — happy path ───────────────────────────────

#[tokio::test]
async fn recent_returns_200_with_seeded_entries() {
    let log = FakeNotificationLog {
        behavior: Behavior::Returns(vec![make_entry("ACC_001"), make_entry("ACC_002")]),
    };
    let server = TestServer::new(router_with_log(log));
    let res = server.get("/recent").await;
    assert_eq!(res.status_code(), StatusCode::OK);

    let body: serde_json::Value = res.json();
    assert_eq!(body["success"], true);
    let data = body["data"].as_array().expect("data is array");
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["recipient"], "ACC_001");
    assert_eq!(data[1]["recipient"], "ACC_002");
    // Wire-shape pin: `kind` must serialise as the snake_case
    // string the OpenAPI contract documents — a Rust-side enum
    // rename would silently break clients without this assertion.
    assert_eq!(data[0]["kind"], "transaction_committed");
}

#[tokio::test]
async fn recent_returns_empty_array_when_no_entries() {
    let log = FakeNotificationLog {
        behavior: Behavior::Returns(vec![]),
    };
    let server = TestServer::new(router_with_log(log));
    let res = server.get("/recent").await;
    assert_eq!(res.status_code(), StatusCode::OK);

    let body: serde_json::Value = res.json();
    assert_eq!(body["success"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

// ─── GET /recent?limit=N — page-size handling ─────────────

#[tokio::test]
async fn recent_honours_caller_supplied_limit() {
    // The application layer clamps to MAX_RECENT=200; here we
    // only test that the caller's `limit` reaches the port (the
    // fake's `take(limit)` proves it propagated end-to-end
    // through `Query<RecentQuery>` extraction).
    let log = FakeNotificationLog {
        behavior: Behavior::Returns(vec![
            make_entry("a"),
            make_entry("b"),
            make_entry("c"),
            make_entry("d"),
            make_entry("e"),
        ]),
    };
    let server = TestServer::new(router_with_log(log));
    let res = server.get("/recent?limit=2").await;
    assert_eq!(res.status_code(), StatusCode::OK);
    let body: serde_json::Value = res.json();
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn recent_rejects_non_integer_limit_with_400() {
    // Axum's `Query<RecentQuery>` extractor fails serde with a
    // 400 before the handler runs — the contract says the
    // endpoint must not return 500 for a malformed query string.
    let log = FakeNotificationLog {
        behavior: Behavior::Returns(vec![]),
    };
    let server = TestServer::new(router_with_log(log));
    let res = server
        .get("/recent?limit=not-a-number")
        // axum-test asserts 2xx by default; opt out so the test
        // can see the deserialisation failure.
        .expect_failure()
        .await;
    assert_eq!(res.status_code(), StatusCode::BAD_REQUEST);
}

// ─── GET /recent — infra error mapping ─────────────────────

#[tokio::test]
async fn recent_returns_500_when_store_reports_infra_error() {
    // NotificationError::Infra → AppError::Internal → 500.
    // The handler must not leak the internal message verbatim;
    // it goes through the AppResponse error shape.
    let log = FakeNotificationLog {
        behavior: Behavior::Infra("simulated store outage"),
    };
    let server = TestServer::new(router_with_log(log));
    let res = server.get("/recent").expect_failure().await;
    assert_eq!(res.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
}
