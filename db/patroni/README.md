# `db/patroni/` — PostgreSQL HA layer (Patroni + etcd)

Everything under this directory is specific to **Patroni** as the HA
orchestrator. The app, pgBouncer, HAProxy, and `db/bootstrap/` all sit
above this layer and know nothing about it; if we ever swap Patroni for
yet another tool, the edit scope is confined to this directory plus the
etcd services in `docker-compose.yml`.

For the full topology (including how HAProxy routes to the current
primary) read [../../docs/ha-architecture.md](../../docs/ha-architecture.md)
first — that document is the source of truth. This README only covers
the files in this directory.

---

## 1. What lives here

| File                                   | Purpose                                                |
|----------------------------------------|--------------------------------------------------------|
| `Dockerfile`                           | `postgres:18.3-bookworm` + `patroni[etcd3]` + `envsubst` |
| `patroni-entrypoint.sh`                | Renders the config from the template, then `exec patroni` |
| `templates/patroni.yml.tmpl`           | Shared config template — `envsubst` fills in the per-node values |
| `post-bootstrap.sh`                    | One-shot script that `CREATE DATABASE ${POSTGRES_DB}`  |
| `README.md`                            | This file                                              |

Every Postgres NODE in the cluster runs this exact image. Variance
between nodes (hostname, shard scope, node name) is injected through
environment variables in `docker-compose.yml`, not through separate
images.

The `etcd` cluster that Patroni talks to is a stock `bitnami/etcd`
image and is declared entirely inside `docker-compose.yml` — there is
no corresponding subdirectory here because etcd has no
project-specific configuration.

---

## 2. Why Postgres 18 (and not Postgres 15)

An earlier iteration of this stack used `pg_auto_failover`, whose
upstream image bundles PostgreSQL 15. That would have forced a
downgrade from the 18.3 we were previously running. We chose Patroni
instead precisely so we could stay on 18, where the peakload workload
benefits from:

- **Async I/O (io_uring backend)** — sequential scans and vacuum are
  meaningfully faster under the transaction-ingest pattern.
- **Skip scan on btree indexes** — many of our hot queries filter by
  `status` + `created_at` and will pick up the optimiser win for free.
- **Better parallel vacuum** — reduces bloat on the `transactions`
  table under sustained writes.

Patroni is PG-version-agnostic because it controls PG through normal
`pg_ctl` / `initdb` paths; swapping the base image to `19-beta` later
requires only a Dockerfile bump.

---

## 3. First-boot and failover behaviour

### First-boot

All six nodes come up in parallel. Each one:

1. Loads `patroni.yml`, which points at the etcd cluster.
2. Tries to acquire the `/service/${PATRONI_SCOPE}/leader` lock in etcd.
3. The first node in each scope to win the race:
   - Runs `initdb` into `$PGDATA`.
   - Creates the app user (`${POSTGRES_USER}`) from the
     `bootstrap.users` section.
   - Runs `post-bootstrap.sh`, which `CREATE DATABASE ${POSTGRES_DB}`.
   - Publishes itself in etcd as the primary and boots PG.
4. The loser in the same scope sees the primary in etcd and runs
   `pg_basebackup` from it, then boots as a streaming replica.

The three shards operate independently — their scopes
(`peakload-shard0`, `…shard1`, `…shard2`) live in separate key prefixes
in etcd and never interact.

### Promotion (primary loss)

When the primary of a shard dies:

1. Its leader key expires in etcd after `ttl` seconds (default: 30s).
2. All surviving replicas of that scope race to acquire the lock.
3. The winner runs `pg_promote()` and starts accepting writes.
4. HAProxy's `GET /primary` probe on :8008 — served by Patroni's REST
   API — flips from the old primary to the new one within one
   `inter × fall` window (4s by default).
5. `on-marked-down shutdown-sessions` in haproxy.cfg severs stale
   pgBouncer → old-primary connections. pgBouncer rebuilds its
   backend pool on the next client checkout. The app's
   `src/db/failover.rs` retry wrapper swallows the window for
   idempotent writes.

Total availability gap: typically **5–15 seconds** from primary crash
to first successful write against the new primary. Tune
`DB_WRITE_RETRY_*` in `.env` if you observe longer windows under
pathological network conditions.

---

## 4. Why a config template instead of one YAML per node

Three shards × two nodes = six near-identical Patroni configs. Keeping
them as one template + `envsubst` means:

- A typo fix lands in one place, not six.
- The compose file is the single source of truth for per-node
  variance (scope, name, hostname).
- Review diffs are obvious — you see the template once.

The rendered configs live under `/var/lib/patroni/patroni.yml` inside
each container and are regenerated on every container start. Patroni
reads it fresh each boot, so changes pushed to the template take
effect after a `docker compose restart pg-shard0-node-a`.

### What belongs in the template vs. in etcd

The template only defines **bootstrap-time** defaults. Anything you
change there after the cluster is alive is ignored. To change live
cluster-wide settings (e.g. `max_connections`, `shared_buffers`,
replication timeouts), use:

```
docker compose exec pg-shard0-node-a patronictl edit-config
```

That writes to etcd and Patroni propagates it to all members of the
scope with the appropriate restart / reload semantics.

---

## 5. Operational cheatsheet

**List cluster state for a shard:**
```
docker compose exec pg-shard0-node-a patronictl list peakload-shard0
```

**Watch cluster events live:**
```
docker compose exec pg-shard0-node-a patronictl --watch 2 list peakload-shard0
```

**Force a failover (planned maintenance):**
```
docker compose exec pg-shard0-node-a \
    patronictl failover peakload-shard0 --candidate shard0-b
```

**Simulate a primary crash (capstone demo):**
```
docker compose kill pg-shard0-node-a
```
Expect HAProxy to repoint within ~10s and the app's
`retry_transient` wrapper to soak the window.

**Reset a node after data-dir corruption:**
```
docker compose stop    pg-shard0-node-a
docker volume rm       peakload_pg_shard0_node_a_data
docker compose start   pg-shard0-node-a
```
The node rejoins as a replica and `pg_basebackup`s from the current
primary automatically.

**Inspect what's in etcd (debugging):**
```
docker compose exec etcd-1 etcdctl get --prefix /service/peakload-shard0/
```

---

## 6. Resource envelope

Per Patroni node, at idle:
- PostgreSQL 18:  ~50 MB RSS
- Patroni (Python): ~35 MB RSS
- Total:           ~90 MB; compose limit 192 MB leaves headroom for
                   WAL traffic during `pg_basebackup` on replica join.

Per etcd node, at idle: ~40 MB RSS. Three-node cluster ≈ 120 MB.
Patroni's etcd traffic is tiny (member heartbeats + leader watches);
CPU is negligible.

---

## 7. Safe-to-diverge knobs

Values in `patroni.yml.tmpl` you are likely to tune:

| Setting                         | Default | Tighten if…                         |
|---------------------------------|---------|-------------------------------------|
| `bootstrap.dcs.ttl`             | 30s     | You want faster failover detection (lower), at the cost of more false positives under network blips (higher). Pair with `retry_timeout` ≤ ttl/2. |
| `maximum_lag_on_failover`       | 1 MiB   | Your replicas lag more under load → raise. You want stricter durability → lower. |
| `shared_buffers`                | 64MB    | You raise the per-node memory limit in compose. Rule of thumb: 25% of container RAM. |
| `max_connections`               | 120     | pgBouncer's DEFAULT_POOL_SIZE × number of pgBouncer instances. Currently 40 × 3 = 120, which is the floor. |
| `basebackup.max-rate`           | 100M    | Replica joins are hurting primary traffic → lower. Joins are too slow → raise. |

After any tweak in the template, the change only applies to **new
cluster bootstraps**. For live clusters use `patronictl edit-config`.
