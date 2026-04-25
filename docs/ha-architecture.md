# HA Architecture — Postgres + Redis

This document explains **how failover works end-to-end** for the peakload
stack and **which pieces are orchestrator-agnostic**, so that swapping
the Postgres HA layer (Patroni → anything else) touches as few files as
possible.

Read this once before editing anything under `db/patroni/`, `haproxy/`,
or the PostgreSQL / pgBouncer services in `docker-compose.yml`.

---

## 1. Big picture

```
                   nginx (public :8080)
                        │
                 ┌──────┴──────┐
                 │   app × 2   │    Rust API, stateless
                 └──────┬──────┘
                        │
          ┌─────────────┴─────────────┐
          │                           │
     writes                        reads
          │                           │
   ┌──────┴───────────┐       ┌───────┴────────┐
   │ pgBouncer-shardN │       │ direct to      │
   │   (3 instances,  │       │ both nodes of  │
   │   one per shard) │       │ each shard     │
   └──────┬───────────┘       │ (round-robin,  │
          │                   │ health-aware)  │
   ┌──────┴────────────┐      └────────────────┘
   │    pg-haproxy     │   role-aware TCP proxy
   │ ports 5000/1/2    │   only forwards to the current primary
   └──────┬────────────┘
          │ TCP 5432
   ┌──────┴──────────────────────┐
   │ pg-shardN-node-a / -node-b  │   paired Postgres 18 nodes
   │   patroni + PG 18           │   per shard (primary + replica)
   │   REST API on :8008         │
   └──────┬──────────────────────┘
          │ leader election / heartbeats
          │
   ┌──────┴────────────────────┐
   │ etcd-1 / etcd-2 / etcd-3  │   3-node DCS, tolerates 1 loss
   └───────────────────────────┘
```

Redis side is unchanged: `app` resolves the current master via
`redis-sentinel-1/2/3`; Sentinel promotes `redis-replica` on master loss.
See [../redis/sentinel.conf](../redis/sentinel.conf).

---

## 2. Failover choreography (Postgres)

Timeline when `pg-shard0-node-a` (current primary) dies:

```
 t=0    node-a crashes
 t≈5s   node-a's etcd lease (leader key in
        /service/peakload-shard0/leader) starts ticking toward expiry
 t=ttl  leader key expires (default ttl: 30s — can be tightened)
 t+1s   node-b's patroni process acquires the leader lock first,
        runs pg_promote(), and publishes itself as primary in etcd
 t+2s   pg-haproxy's GET /primary probe on node-a → 503
        pg-haproxy's GET /primary probe on node-b → 200
 t+3s   pg-haproxy marks node-a DOWN, closes its backend sessions
        (on-marked-down shutdown-sessions), and forwards new conns
        to node-b
 t+4s   pgBouncer-shard0's in-flight transactions fail with a
        transient error. src/db/failover.rs:21 classifies it as
        transient and the Rust retry wrapper reconnects against
        the fresh pgBouncer → haproxy path, which now lands on
        node-b
 t+∞    node-a eventually returns. It sees the leader key held by
        node-b and rejoins as a streaming replica. If WAL has
        diverged, pg_rewind (or pg_basebackup) reconciles it.
```

**Two things the application already gets right:**
- `src/db/failover.rs` classifies `Io / PoolTimedOut / PoolClosed /
  WorkerCrashed` as transient and retries. These are exactly the
  errors raised during the promotion window.
- `src/db/pool.rs` keeps per-replica `AtomicBool` flags for reads, so
  a node that is briefly unreachable during its own promotion is
  simply skipped for reads until it comes back.

**Retry budget matters.** `DB_WRITE_RETRY_MAX_ATTEMPTS` × backoff needs
to cover the promotion window (typically 5–15s with default
`ttl: 30, retry_timeout: 10`). The defaults in `.env.example` soak
~4.2s; requests lasting longer surface as 5xx to the HTTP caller,
which is expected behaviour.

---

## 3. Layered responsibilities

| Layer                | What it does                                    | Orchestrator-specific? |
|----------------------|-------------------------------------------------|------------------------|
| app (Rust)           | Retries transient errors, health-aware reads   | **No**                 |
| pgBouncer            | Connection pooling; points at a stable proxy   | **No** — upstream is `pg-haproxy:500N` |
| pg-haproxy           | Routes writes to the **current** primary       | **No** — depends only on the `GET /primary` 200/503 contract on :8008 |
| patroni + PG         | Runs PG; promotes replica on leader loss       | **Yes** — this is the HA tool |
| etcd                 | Leader-election / config store for Patroni     | **Yes** — replaced or removed under a different HA tool |

The dashed line sits around `patroni + PG` and `etcd`. Everything above
is stable and survived the earlier migration away from pg_auto_failover.
Everything inside was rewritten during that migration.

---

## 4. Why Patroni and not pg_auto_failover

We originally planned pg_auto_failover as a simpler-to-deploy
alternative. We switched to Patroni before implementing it because the
only Docker image available for pg_auto_failover bundles PostgreSQL 15,
and keeping PG 18 mattered for the workload:

- **Async I/O (io_uring)** — meaningful win on sequential scans and
  vacuum under the transaction-ingest pattern.
- **Skip scan on btree** — optimiser win for `(status, created_at)`-
  style filters.
- **Parallel vacuum improvements** — less bloat on the `transactions`
  table under sustained writes.

Patroni is PG-version-agnostic because it controls Postgres through
normal `pg_ctl` / `initdb`. Our image is just
`postgres:18.3-bookworm` + `patroni[etcd3]` + a small entrypoint.

Secondary benefits we got "for free" by going straight to Patroni:

- **No single-monitor SPOF.** etcd is a 3-node cluster; tolerates one
  loss.
- **Built-in HTTP API** on :8008 with the exact `GET /primary` 200/503
  contract our HAProxy config already wanted. We deleted a custom
  socat + healthcheck.sh sidecar that pg_auto_failover would have
  required.
- **No future HA migration ahead.** Patroni is the industry default
  and what we'd have migrated to anyway.

---

## 5. Known operational caveats

- **etcd quorum SPOF cluster-wide.** If two etcd nodes die
  simultaneously, Patroni loses the DCS and **demotes all primaries
  to read-only** to prevent split-brain. Deliberate design: data
  safety wins over availability. Mitigation = deploy etcd on separate
  hosts in a real production deployment (out of scope for the
  capstone, which is single-host Docker).
- **Watchdog disabled.** Patroni supports STONITH-style watchdog
  integration. We disabled it because the capstone runs in Docker
  without `/dev/watchdog` access. In production on bare metal, enable
  it to guarantee a self-fencing demote on DCS loss.
- **Two-node shards.** Each shard has only primary + replica. If both
  die simultaneously, manual recovery is required. A sync replica or
  a third node could be added if the budget allows.
- **Promotion latency vs. retry budget.** Default DCS ttl is 30s, which
  is the upper bound of the "primary is definitely gone" detection
  window. Lower it (and `retry_timeout` proportionally) if you want
  faster failover at the cost of more false positives on network
  blips. `patronictl edit-config` is the right tool for this, not the
  bootstrap template.
- **pgBouncer stale pool.** HAProxy's `on-marked-down shutdown-sessions`
  forces backend closures on primary flip. pgBouncer then rebuilds
  server connections on the next client acquire — no manual reload
  required.

---

## 6. Swapping Patroni in the future

The contract between the app-facing layer and the HA layer is:

> **Each Postgres node serves HTTP `GET /primary` on port 8008,
> returning 200 iff it is currently the writable primary and any
> non-2xx otherwise.**

Any replacement that honours this contract fits into the current
topology with zero edits to `haproxy/`, `pgbouncer-shard*`, the app,
or `db/bootstrap/`. The edit scope is confined to `db/patroni/`
(rename and rewrite) plus the etcd services in `docker-compose.yml`
(keep or remove based on the replacement's DCS choice).

See [../db/patroni/README.md](../db/patroni/README.md) §7 for a
detailed file-by-file walkthrough of what would change under such a
swap.

---

## 7. Further reading

- Patroni docs: https://patroni.readthedocs.io
- etcd operations: https://etcd.io/docs/latest/op-guide/
- HAProxy `option httpchk` reference: https://docs.haproxy.org/3.2/configuration.html
- Redis Sentinel (our Redis HA layer): https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
