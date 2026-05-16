# Remediation Impact Log

Tracks changes that affect SLOs or other cross-cutting properties
(resilience, money-safety, security surface, data consistency).

| Date | Finding | Change | SLO / property impact | Direction | Verified by |
|------|---------|--------|-----------------------|-----------|-------------|
| 2026-05-16 | D-1 / R-3 | All pooled PG connections now set `statement_timeout` (= `db_query_timeout_secs`×1000ms, default 2000), `lock_timeout`=500ms, `idle_in_transaction_session_timeout`=5000ms via `PgConnectOptions::options()` | Availability + P95 latency: caps pool-slot hold time, removes the abandoned-query → PoolTimedOut → retry-amplification cascade (D-1). Regression risk: a legitimately long query (e.g. the idempotency cleanup sweep) now dies at 2 s — must use `SET LOCAL statement_timeout` at any such site. | Improves availability/P95; bounded regression risk on long scans | `crates/app/tests/db_timeout_test.rs` (SQLSTATE 57014) + full `-p app` / `-p shared_kernel` suites green |
