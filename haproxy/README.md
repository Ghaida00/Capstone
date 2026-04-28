# `haproxy/` — Postgres write router

HAProxy sits between `pgbouncer-shardN` and the Patroni-managed node
pair for each shard, forwarding every write to the **current primary**.

For *why* this layer exists and how the orchestrator-swap story works,
see [ADR-0006](../docs/adr/0006-haproxy-primary-routing.md).

## Files

| File             | Purpose                                       |
|------------------|-----------------------------------------------|
| `haproxy.cfg`    | Frontends + backends. Three shard frontends + a stats endpoint. |
| `Dockerfile`     | Stock `haproxy:lts-alpine`.                   |

## Port map

| Port   | Purpose                                                  |
|--------|----------------------------------------------------------|
| `5000` | shard 0 — writes to whichever node is primary            |
| `5001` | shard 1 — same                                           |
| `5002` | shard 2 — same                                           |
| `7000` | HAProxy stats dashboard (HTTP, read-only, host-bound to `127.0.0.1`) |

The `:8008` port referenced in `haproxy.cfg` is **the target** on each
PG node — Patroni's REST API. HAProxy does not bind it.

## Operational

**View live status**: open `http://127.0.0.1:7000` while the stack runs.

**Promotion timing** (worst case): `inter × fall = 2s × 2 = 4s` for
HAProxy to mark the old primary DOWN, plus Patroni's promotion latency
(~5–15 s with default DCS ttl). Tune `DB_WRITE_RETRY_*` in `.env` to
cover this.

**Failure mode**: HAProxy is a single process. A crash recovers via
`restart: unless-stopped` and re-detects primaries within ~4 s. For
production, deploy two HAProxy instances behind a VIP (keepalived) or
a cloud LB.
