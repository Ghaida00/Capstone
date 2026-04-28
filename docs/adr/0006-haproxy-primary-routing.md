# 0006 — HAProxy `GET /primary` for write routing

**Status:** Accepted
**Date:** 2026-01-20

## Context

Each shard has two Postgres nodes: one primary, one replica. The
application (via pgBouncer) needs a stable address for writes that
*always* lands on the current primary, even seconds after a Patroni
promotion.

libpq supports `target_session_attrs=read-write` against a multi-host
URL, which would let pgBouncer fail over on its own — except
**pgBouncer does not propagate the multi-host URL to libpq for
backend connections**. Its `databases` section parses a single
host/port. We need an external router.

## Decision

Put one HAProxy in front of all three shards. For each shard, the
backend is the two Postgres nodes; `option httpchk` runs:

```
GET /primary HTTP/1.0
```

against each node's `:8008` (Patroni's REST API in our setup —
[ADR-0005](0005-patroni-over-pg-auto-failover.md)). A node returning
**200** is considered up; **503** (or anything non-2xx) is down.
Because exactly one node per scope holds the etcd leader lock,
exactly one is up at a time.

```
pgBouncer-shardN  →  pg-haproxy:500N  →  whichever node is GET /primary 200
```

`on-marked-down shutdown-sessions` forcibly closes connections to
a node that just lost primary status. pgBouncer rebuilds backend
connections on the next checkout; the app's
[`shared_kernel/db/failover.rs`](../../crates/shared_kernel/src/db/failover.rs)
classifies the resulting error as transient and retries.

## Consequences

- **Orchestrator-agnostic interface.** The HA layer beneath HAProxy
  can be Patroni today, Stolon tomorrow, a managed service after that
  — as long as it serves the `GET /primary` 200/503 contract on
  `:8008`. The HAProxy directory takes zero edits during such a swap.
- **Worst-case unavailability**: `inter × fall = 2s × 2 = 4s` for
  HAProxy to detect the flip, plus Patroni's promotion latency
  (~5–15s with default DCS ttl). The app's retry budget
  (`DB_WRITE_RETRY_MAX_ATTEMPTS` × backoff in `.env.example`) is sized
  to soak this window.
- **Single HAProxy is a SPOF for write routing.** Acceptable for the
  capstone (single-host Docker). For production, deploy two HAProxy
  instances behind a VIP (keepalived) or a cloud LB.
- **One process for all three shards.** ~20MB RSS vs. three separate
  HAProxies. They share fate (same compose host) but losing the host
  loses all shards anyway.

## Alternatives considered

- **libpq multi-host + `target_session_attrs`.** Doesn't compose
  with pgBouncer's `databases` parser. Dead end.
- **Three separate HAProxies (one per shard).** No HA benefit; wastes
  ~20MB and triples config diff surface.
- **Sidecar healthcheck + socat per node.** What pg_auto_failover
  would have required. Patroni's built-in REST API made this
  unnecessary.
