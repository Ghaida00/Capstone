# Remediation Impact Log

Tracks changes that affect SLOs or other cross-cutting properties
(resilience, money-safety, security surface, data consistency).

| Date | Finding | Change | SLO / property impact | Direction | Verified by |
|------|---------|--------|-----------------------|-----------|-------------|
| 2026-05-16 | D-1 / R-3 | Server-side `ALTER DATABASE peakload_db SET statement_timeout=2000 / lock_timeout=500 / idle_in_transaction_session_timeout=5000`, applied per shard primary by `bootstrap-schema.sh` unconditionally (idempotent; survives pgBouncer txn pooling via RESET-ALL→default). Supersedes the reverted client-side WP1. | Availability + P95: caps pool-slot hold time, removes abandoned-query → PoolTimedOut → retry-amplification cascade. Regression: long idempotency cleanup sweep now dies at 2s — tracked as D-NEW-cleanup-timeout (WP9). | Improves availability/P95; bounded regression on long scans (owned by WP9) | `db/bootstrap/test-timeouts.sh` (drives real bootstrap-schema.sh + project's exact pgBouncer image; asserts cancel through pgBouncer) + live-stack `SHOW statement_timeout` via `peakload-pgbouncer-shard0`. Mechanism independently proven 2026-05-16 (isolated postgres+edoburu/pgbouncer experiment). |
