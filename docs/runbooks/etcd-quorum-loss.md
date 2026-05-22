# Runbook: etcd quorum loss (all primaries read-only)

**Last reviewed:** 2026-05-18
**Owner:** infra on-call
**Related ADRs / code:**
[db/patroni/README.md](../../db/patroni/README.md) (Caveats),
[ADR-0005](../adr/0005-patroni-over-pg-auto-failover.md),
[docs/architecture.md](../architecture.md).

## Symptom

- **Every** shard's writes fail simultaneously (not one shard — all).
  `POST /api/v2/transactions` returns 5xx across the board;
  `peakload:http_availability:ratio_rate5m` drops hard.
- Reads may still succeed (replicas serve until they too are demoted).
- `patronictl list` errors or shows no leader on any scope.

This is the **deliberate** Patroni safety behaviour: when etcd loses
quorum (≥2 of 3 etcd nodes down), Patroni cannot hold the leader
lock, so it demotes every primary to read-only. Data safety wins over
availability — this is by design, not a bug.

## Severity & blast radius

- Customer-visible? **Yes, total write outage**, all shards.
- Reversible? Yes — fully, once etcd quorum returns. No data loss
  (that is the entire point of the demotion).
- Time-to-broken: immediate and global. This is a top-severity page.

## Detect / confirm

```bash
# etcd cluster health (3-node cluster):
docker compose exec etcd-1 etcdctl endpoint health --cluster
docker compose ps | grep etcd
# Patroni sees no DCS:
docker compose exec pg-shard0-node-a patronictl list peakload-shard0
```

`etcdctl endpoint health` failing on ≥2 endpoints = quorum lost.

## Mitigate (stop the bleed)

The only real mitigation is **restore etcd quorum** — there is no
app-side workaround, and you must not try to force a primary writable
(that defeats the safety guarantee and risks split-brain).

1. Identify which etcd nodes are down:
   ```bash
   docker compose ps etcd-1 etcd-2 etcd-3
   ```
2. Restart the failed etcd node(s):
   ```bash
   docker compose up -d etcd-2 etcd-3
   docker compose exec etcd-1 etcdctl endpoint health --cluster
   ```
3. The moment ≥2 etcd nodes are healthy, quorum returns and Patroni
   re-acquires the leader lock per shard automatically; primaries
   come back read-write within ~one `ttl` (30 s).

## Recover (return to normal)

1. Confirm all 3 etcd nodes healthy.
2. `patronictl list peakload-shard0` / `peakload-shard1` show a
   `Leader` per scope.
3. Write smoke test: `POST /api/v2/transactions` returns 202.
4. Watch `peakload:http_availability:ratio_rate5m` climb back.

## Rollback

N/A — there is nothing to roll back. Do **not** attempt manual
`pg_promote` or edit etcd keys by hand to "speed it up"; a forced
primary while etcd is partitioned is exactly the split-brain the
demotion exists to prevent.

## Postmortem checklist

- Why did ≥2 etcd nodes fail together? Co-located on one host/disk?
  The production guidance (Patroni README caveat) is etcd on separate
  hosts — was that violated?
- Was the global write outage paged within seconds? An all-shard
  availability drop should be the fastest alert in the system
  (`PeakloadAvailabilityFastBurn`).
