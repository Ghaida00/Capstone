# Runbook: Redis master failover (post-Sentinel-promotion)

**Last reviewed:** 2026-05-18
**Owner:** infra on-call
**Related ADRs / code:**
`.env` `REDIS_SENTINEL_*`, [cache/redis.rs](../../crates/shared_kernel/src/cache/redis.rs),
metric `redis_master_failover_total`.

## Symptom

- Brief spike in cache-write errors / cache-miss rate just after a
  Redis master change; idempotency-reservation path may log warnings.
- `redis_master_failover_total` increments.
- `redis-master` container restarted or Sentinel promoted a replica.

The app is Sentinel-aware: it resolves the master via
`REDIS_SENTINEL_NODES` on startup and re-polls every
`REDIS_SENTINEL_MONITOR_INTERVAL_SECS` (5 s); on promotion it rebuilds
its write pool automatically. So the expected blip is ≤ ~5 s.

## Severity & blast radius

- Customer-visible? Minimal/partial — the write path's durability is
  in Postgres + the outbox, not Redis; a Redis blip degrades cache
  hit-rate and the fast idempotency path, not money correctness.
- Reversible? Yes, automatic within one monitor interval.
- Time-to-broken: only "broken" if the app fails to re-resolve (e.g.
  `REDIS_SENTINEL_NODES` empty/misconfigured → no auto-failover).

## Detect / confirm

```bash
curl -sG http://<host>:9090/api/v1/query \
  --data-urlencode 'query=increase(redis_master_failover_total[10m])'

docker compose exec redis-sentinel-1 \
  redis-cli -p 26379 sentinel get-master-addr-by-name peakload-master

docker compose logs --since 5m peakload-app-1 | grep -i "redis master"
```

App log should show it picked up the new master address within ~5 s.

## Mitigate (stop the bleed)

1. If the app re-resolved (log shows new master, errors stopped):
   nothing to do — it self-healed. Confirm cache hit-rate recovers
   on the Grafana *Cache Hit Rate* panel.
2. If the app did **not** re-resolve (errors persist > ~30 s):
   - Confirm `REDIS_SENTINEL_NODES` is non-empty in the running
     container: `docker inspect peakload-app-1 | grep REDIS_SENTINEL`.
     If empty, the deployment disabled auto-failover — set it and
     `docker compose up -d app`.
   - If set but stale, bounce the app instances so they re-resolve
     on startup: `docker compose restart app`.
3. If Sentinel itself has no quorum (can't agree on a master):
   restart the failed sentinel(s) — `docker compose up -d
   redis-sentinel-2 redis-sentinel-3`.

## Recover (return to normal)

1. `sentinel get-master-addr-by-name peakload-master` returns the new
   master; replicas re-attached.
2. App cache-error log lines stop; hit-rate panel recovers.
3. `redis_master_failover_total` flat again.

## Rollback

Do not manually `SLAVEOF` / re-point Redis by hand while Sentinel is
managing the topology — let Sentinel own promotion. Restarting app
instances is the safe lever.

## Postmortem checklist

- Was the blip within the ~5 s monitor interval? If longer, was
  `REDIS_SENTINEL_MONITOR_INTERVAL_SECS` raised, or did pool rebuild
  stall?
- Did any idempotency reservations fall back to the Postgres path
  cleanly (no double-processing)? Spot-check
  `idempotency_redis_fallback_total`.
