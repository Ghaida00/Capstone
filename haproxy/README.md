# `haproxy/` — role-aware Postgres write router

HAProxy sits between `pgbouncer-shardN` and the Patroni-managed node
pair that makes up each shard, forwarding every write to whichever
node is the **current primary**.

This layer is what decouples the app from the HA orchestrator.
pgBouncer points at a stable hostname/port (`pg-haproxy:500N`);
HAProxy figures out which node is writable right now. If we ever
replace Patroni, this directory stays untouched as long as the new
orchestrator serves the same `GET /primary` HTTP contract on :8008.

---

## 1. Port map

| Port     | Purpose                                             |
|----------|-----------------------------------------------------|
| `5000`   | shard 0 — writes to whichever node is primary       |
| `5001`   | shard 1 — same                                      |
| `5002`   | shard 2 — same                                      |
| `7000`   | HAProxy stats dashboard (HTTP, read-only)           |
| `8008`   | **NOT bound here** — that's the *target* port on    |
|          | each PG node where HAProxy sends health probes,     |
|          | served by Patroni's built-in REST API              |

Only `7000` is exposed to the host (via
`127.0.0.1:7000:7000` in docker-compose.yml) for local debugging.
The 5000-series ports are internal to `peakload-network` and
consumed by pgBouncer.

## 2. How the role decision works

Every 2 seconds HAProxy sends an HTTP request to each backend node:

```
GET /primary HTTP/1.0
```

Patroni's REST API on :8008 answers:

- **`200 OK`** if this node currently holds the etcd leader lock
  for its scope (i.e. is the primary).
- **`503 Service Unavailable`** if this node is a replica, unhealthy,
  or in the middle of a role change.

`option httpchk` + `http-check expect status 200` means only the node
returning 200 is considered UP. Because each shard's backend has only
two servers (primary + replica), exactly one is UP at any given time
and HAProxy routes traffic unambiguously.

### The flip

When Patroni promotes the replica:

1. **Old primary → replica** (or unreachable): next health probe
   returns non-200 → HAProxy marks it DOWN. The
   `on-marked-down shutdown-sessions` directive forcibly closes any
   existing client connections to this backend.
2. **New primary (formerly replica) → primary**: next probe returns
   200 → HAProxy marks it UP.
3. pgBouncer's in-flight transactions on the closed connections fail
   with a connection error. `src/db/failover.rs:21` classifies that
   as transient and the Rust retry wrapper retries against the fresh
   path, now landing on the promoted node.

The worst-case unavailability window is `inter × fall = 2s × 2 = 4s`
for HAProxy to mark the old primary DOWN, plus whatever Patroni took
to promote the replica (typically ~5–10s driven by the etcd leader
lease TTL in `db/patroni/templates/patroni.yml.tmpl`). Tune the app's
`DB_WRITE_RETRY_MAX_ATTEMPTS` / `_BACKOFF_MS` to cover this envelope.

## 3. Why one HAProxy, three frontends (not three HAProxies)?

- **Resource efficiency.** A single HAProxy process handles thousands
  of concurrent TCP sessions per MB of RAM. Three instances would
  waste ~20 MB for no HA benefit (the app already has pgBouncer in
  front, and losing one HAProxy would lose all three shards anyway
  because they share the compose host).
- **Operational simplicity.** One config file, one log stream, one
  stats dashboard.
- **No shared fate.** HAProxy is deliberately stateless — a crash is
  recovered by `restart: unless-stopped`. On restart, backends are
  re-probed and primaries re-detected within 4 seconds.

If you need HAProxy HA (not just PG HA), deploy two HAProxy instances
behind a VIP (keepalived) or a cloud load balancer. That is a
legitimate production hardening step. Deferred for the capstone.

## 4. Why not use libpq's multi-host + `target_session_attrs`?

libpq supports `postgresql://host1,host2/db?target_session_attrs=read-write`,
which looks like it would let pgBouncer fail over on its own. In practice
it doesn't work because **pgBouncer does not propagate the multi-host URL
to libpq for backend connections** — its `databases` section parses a
single host/port. HAProxy is the pragmatic workaround.

## 5. What survives a future HA swap

Everything in this directory. Patroni, the tool we swapped pg_auto_
failover for, happens to serve the exact same `/primary` 200/503
contract on :8008 that our sidecar-and-socat setup served before,
so this config took zero edits during that migration. Any future
replacement (Stolon, repmgr, a managed cloud Postgres service) that
offers the same contract requires zero edits here either.

See [../docs/ha-architecture.md](../docs/ha-architecture.md) for the
full topology and orchestrator-swap story.
