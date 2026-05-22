# Runbook: Postgres primary down / Patroni failover

**Last reviewed:** 2026-05-18
**Owner:** infra on-call
**Related ADRs / code:**
[db/patroni/README.md](../../db/patroni/README.md),
[ADR-0005](../adr/0005-patroni-over-pg-auto-failover.md),
[failover.rs](../../crates/shared_kernel/src/db/failover.rs),
`.env` `DB_WRITE_RETRY_*`.

## Symptom

- Short burst of 5xx on the write path (`POST /api/v2/transactions`)
  and `peakload:http_availability:ratio_rate5m` dips for ~5–15 s.
- `db_replica_failover_total` / `db_retry_attempt_total` climb.
- One `pg-shardN-node-{a,b}` container unhealthy or killed.

A *planned* or *clean* failover should be nearly invisible — the
`.env` retry schedule (6 attempts ≈ 4.2 s, sized to cover etcd lease
expire + `pg_promote` + HAProxy flip ≈ 10 s typical) absorbs it. A
visible, sustained outage means the failover did not complete.

## Severity & blast radius

- Customer-visible? Partial — writes briefly 5xx, reads continue
  (replica round-robin + writer fallback). Auto-recovers if Patroni
  promotes successfully.
- Reversible? Yes (automatic). Manual only if Patroni cannot elect.
- Time-to-broken: if no replica is promotable, writes stay down until
  manual recovery.

## Detect / confirm

```bash
# Cluster state for the affected shard (scope = peakload-shardN):
docker compose exec pg-shard0-node-a patronictl list peakload-shard0
docker compose exec pg-shard1-node-a patronictl list peakload-shard1
```

Look for a `Leader` row. No leader + no `running` replica = stuck.

```bash
curl -sG http://<host>:9090/api/v1/query \
  --data-urlencode 'query=rate(db_retry_exhausted_total[1m])'
```

## Mitigate (stop the bleed)

1. If a leader exists and replica is `running` but stale, let the
   retry schedule ride — confirm recovery within ~15 s. If not:
2. Force a failover to a healthy candidate:
   ```bash
   docker compose exec pg-shard0-node-a \
     patronictl failover peakload-shard0 --candidate shard0-b
   ```
   Expected: `patronictl list` shows the candidate as `Leader`,
   writes recover within the retry window.
3. If both nodes of a shard are down (two-node shard caveat): this is
   manual recovery — restore the most-recent healthy node from its
   data volume, or reseed the replica (Recover step 2).

## Recover (return to normal)

1. Bring the crashed node back; it rejoins as replica via
   `pg_basebackup`:
   ```bash
   docker compose start pg-shard0-node-a
   docker compose exec pg-shard0-node-a patronictl list peakload-shard0
   ```
2. On data-dir corruption, reset the node so it re-clones:
   ```bash
   docker compose stop pg-shard0-node-a
   docker volume rm peakload_pg_shard0_node_a_data
   docker compose start pg-shard0-node-a
   ```
3. Confirm both nodes `running`, one `Leader`, replica lag low.

## Rollback

A forced `patronictl failover` is not reversible — you cannot
"un-promote". If you promoted the wrong node, failover again to the
intended one once it is healthy. Do not delete data volumes unless
the node is confirmed corrupt and the other node is the current,
healthy leader.

## Postmortem checklist

- Did the `.env` retry window fully mask the failover? If clients saw
  5xx, was the promotion slower than the ~10 s assumption — retune
  `DB_WRITE_RETRY_*` or Patroni `ttl`/`retry_timeout`.
- Two-node-shard exposure: was this a single-node blip or a
  both-nodes event? If the latter, escalate the "add a third node /
  sync replica" backlog item.
