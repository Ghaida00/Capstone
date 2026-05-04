# 0005 — Patroni over pg_auto_failover

**Status:** Accepted
**Date:** 2026-01-20

## Context

We need automatic primary-replica failover for each of our three
Postgres shards. The application already retries transient errors
([`crates/shared_kernel/src/db/failover.rs`](../../crates/shared_kernel/src/db/failover.rs)),
HAProxy already routes writes to whichever node serves
`GET /primary` 200 ([ADR-0006](0006-haproxy-primary-routing.md));
what was missing is the orchestrator that decides who is primary.

We initially planned `pg_auto_failover` for its operational simplicity
(single binary, simple monitor model). On evaluation it forced a
**downgrade to PostgreSQL 15**: the only maintained Docker image
bundles PG 15, and the project does not support newer.

PG 18 mattered for our workload:
- **Async I/O (io_uring)** — meaningful win on sequential scans and
  vacuum under transaction-ingest.
- **Skip scan on btree** — `(status, created_at)` filters get the
  optimiser improvement.
- **Parallel vacuum improvements** — less bloat on the `transactions`
  table under sustained writes.

## Decision

Use **Patroni + etcd**, on `postgres:18.3-bookworm`, as the HA
orchestrator.

Patroni controls Postgres through normal `pg_ctl` / `initdb` paths,
so it is PG-version-agnostic. The image is the upstream Postgres
image plus `patroni[etcd3]` plus a small templating entrypoint
(`db/patroni/`).

## Consequences

- **PG 18 retained**, with the optimiser/vacuum benefits above.
- **No single-monitor SPOF.** etcd is a 3-node cluster; tolerates one
  loss. (The full quorum still becomes a SPOF — see "operational
  caveats" in [`db/patroni/README.md`](../../db/patroni/README.md).)
- **Built-in `GET /primary` REST API on :8008.** Matches the contract
  HAProxy already wanted (ADR-0006), so we deleted a custom
  socat + healthcheck.sh sidecar that the pg_auto_failover plan would
  have required.
- **More moving parts.** We carry Patroni + etcd vs. a single
  monitor process. Worth it given PG-version flexibility.
- **Watchdog disabled** in Docker (no `/dev/watchdog`). Re-enable on
  bare-metal production for STONITH-style self-fencing on DCS loss.
- **No future HA migration ahead.** Patroni is the industry default
  and is what we would migrate to anyway.

## Alternatives considered

- **pg_auto_failover** — rejected due to the PG 15 lock-in.
- **Stolon** — equivalent design space to Patroni; Patroni has
  larger community + clearer docs for the etcd-backed setup.
- **Managed cloud Postgres** (AWS RDS / GCP Cloud SQL) — out of
  scope for a self-hosted capstone; would be the pragmatic choice
  in a real production deployment.
