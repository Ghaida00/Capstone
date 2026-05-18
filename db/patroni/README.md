# `db/patroni/` — PostgreSQL HA layer

Patroni + etcd run each Postgres shard as a primary + replica pair,
promoting the replica when the primary dies. Everything above this
layer (app, pgBouncer, HAProxy, `db/bootstrap/`) is orchestrator-agnostic.

For *why* Patroni (vs. pg_auto_failover) and *why* PG 18 see
[ADR-0005](../../docs/adr/0005-patroni-over-pg-auto-failover.md).

## Files

| File                            | Purpose                                                  |
|---------------------------------|----------------------------------------------------------|
| `Dockerfile`                    | `postgres:18.3-bookworm` + `patroni[etcd3]` + `envsubst` |
| `patroni-entrypoint.sh`         | Renders the config from the template, then `exec patroni` |
| `templates/patroni.yml.tmpl`    | Shared config template; `envsubst` fills per-node values |
| `post-bootstrap.sh`             | One-shot `CREATE DATABASE ${POSTGRES_DB}` after init     |

Every Postgres node runs this exact image. Per-node variance (hostname,
shard scope, node name) comes from environment variables in
`docker-compose.yml`.

The etcd cluster (3 nodes) is the upstream `quay.io/coreos/etcd` image
declared inline in `docker-compose.yml` — no project-specific config.

## Operational cheatsheet

```bash
# Cluster state for a shard
docker compose exec pg-shard0-node-a patronictl list peakload-shard0

# Watch live
docker compose exec pg-shard0-node-a patronictl --watch 2 list peakload-shard0

# Planned failover
docker compose exec pg-shard0-node-a patronictl failover peakload-shard0 --candidate shard0-b

# Simulate primary crash
docker compose kill pg-shard0-node-a

# Reset a node after data-dir corruption (rejoins as replica via pg_basebackup)
docker compose stop pg-shard0-node-a
docker volume rm peakload_pg_shard0_node_a_data
docker compose start pg-shard0-node-a

# Inspect etcd state
docker compose exec etcd-1 etcdctl get --prefix /service/peakload-shard0/
```

## Live config changes

Anything in `patroni.yml.tmpl` is **bootstrap-only**. To change live
cluster settings (e.g. `max_connections`, `shared_buffers`):

```bash
docker compose exec pg-shard0-node-a patronictl edit-config
```

That writes to etcd and Patroni propagates it to the scope.

## Tunable knobs

| Setting                      | Default | Tune when…                                    |
|------------------------------|---------|-----------------------------------------------|
| `bootstrap.dcs.ttl`          | 30 s    | Want faster failover (lower) vs. fewer false positives on network blips (higher). Pair with `retry_timeout` ≤ ttl/2. |
| `maximum_lag_on_failover`    | 1 MiB   | Replicas lag more under load → raise; want stricter durability → lower. |
| `shared_buffers`             | 64 MB   | Per-node memory limit raised in compose. Rule of thumb: 25% of container RAM. |
| `max_connections`            | 120     | Sized as `pgBouncer (DEFAULT_POOL_SIZE + RESERVE_POOL_SIZE) + headroom = (80 + 20) + 20 = 120`. The +20 headroom is for autovacuum / replication / admin. Pair with `DB_WRITE_POOL_SIZE` so `app_replicas × DB_WRITE_POOL_SIZE ≤ DEFAULT_POOL_SIZE` (now 2 × 40 = 80) — keeps pgBouncer from silently queueing the app (D-2). |
| `basebackup.max-rate`        | 100 M   | Replica joins hurting primary traffic → lower; joins too slow → raise. |

## Pool sizing formula

The four-layer pool stack (app pool → pgBouncer → PG) must obey one
inequality so the bouncer is not silently queueing the app and so PG
has slots left for non-app work:

```
app_replicas × DB_WRITE_POOL_SIZE
        ≤ pgBouncer DEFAULT_POOL_SIZE
        ≤ pgBouncer DEFAULT + RESERVE_POOL_SIZE   (burst ceiling)
        ≤ PG max_connections − 20                  (headroom: autovacuum,
                                                    replication, admin)
```

To size from a target peak concurrent-writer count `W` per shard:

| Knob                                       | Formula                          | Current (W=80) |
|--------------------------------------------|----------------------------------|----------------|
| `.env DB_WRITE_POOL_SIZE`                  | `ceil(W / app_replicas)`         | 40             |
| compose `pgBouncer DEFAULT_POOL_SIZE`      | `W`                              | 80             |
| compose `pgBouncer RESERVE_POOL_SIZE`      | `~20%` of DEFAULT (round to ten) | 20             |
| Patroni `max_connections`                  | `(DEFAULT + RESERVE) + 20`       | 120            |
| `.env DB_READ_POOL_SIZE`                   | sized for read RPS, not writers  | 80             |

`W` is bounded above by PG `max_connections − 20`. Picking `W` too low
caps write throughput; too high pushes pgBouncer's burst ceiling into
PG's reserved-for-admin slots and risks `FATAL: too many connections`
for autovacuum / Patroni replication.

When changing any of these, change ALL four together (the inequality
is fragile). Verify with the k6 stress scenario and watch:
- `peakload:http_req_duration` p99 (queue waits show up here, not in
  `pg_stat_activity` since the queue is at the bouncer, not at PG).
- `pg_stat_activity` row count on each primary should track the
  bouncer's active backend connection count, not the app's
  client-side pool maximum.

## Caveats

- **etcd quorum loss demotes all primaries to read-only.** Deliberate;
  data safety wins over availability. Run etcd nodes on separate hosts
  in production.
- **Watchdog disabled** (no `/dev/watchdog` in Docker). Re-enable on
  bare-metal production for STONITH-style self-fencing.
- **Two-node shards.** Both die → manual recovery. Add a third node or
  a sync replica if budget allows.
