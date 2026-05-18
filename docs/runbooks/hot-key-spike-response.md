# Runbook: Hot-key spike (single-account row-lock contention)

**Last reviewed:** 2026-05-18
**Owner:** payments on-call
**Related ADRs / code:**
[consumer.rs](../../crates/transactions/src/infrastructure/consumer.rs)
(per-shard batch, `users` row UPDATE), k6 `hotkey` scenario in
[k6/load-test.js](../../k6/load-test.js).

## Symptom

- p95/p99 latency spike concentrated on the write path while overall
  RPS is normal — `peakload:http_latency_under_slo:ratio_rate5m`
  dips, `PeakloadLatencySLOBreach` may fire.
- The spike correlates with a small set of (or one) `from_account`
  or `to_account` — a celebrity/payroll/exchange account taking
  disproportionate traffic.
- One shard's `transactions_batch_size{shard="N"}` and DB lock-wait
  rise while the other shard is calm (hot key hashes to one shard).

## Severity & blast radius

- Customer-visible? Partial — elevated latency for transactions
  touching the hot account (and queue-mates behind it in a batch);
  other accounts mostly unaffected if on the other shard.
- Reversible? Yes — transient; subsides when the burst ends or is
  shed.
- Time-to-broken: stays "degraded" under sustained hammering; becomes
  "broken" only if lock waits cascade into pool exhaustion on that
  shard.

## Detect / confirm

```bash
# Which account is hot (sender side):
PGPASSWORD=$POSTGRES_PASSWORD psql -h <host> -p 5000 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT from_account, count(*) FROM transactions
     WHERE created_at > NOW() - INTERVAL '5 min'
     GROUP BY from_account ORDER BY 2 DESC LIMIT 5;"

# Lock waits on the hot row:
PGPASSWORD=$POSTGRES_PASSWORD psql -h <host> -p 5000 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT wait_event_type, wait_event, count(*) FROM pg_stat_activity
     WHERE state='active' GROUP BY 1,2 ORDER BY 3 DESC;"
```

Grafana: *Latency by Endpoint* + per-shard *Batch Size Distribution*
(one shard hot, one calm = single hot key).

## Mitigate (stop the bleed)

1. **Confirm it is contention, not a dead shard.** `pg_stat_activity`
   should show `Lock` / `transactionid` waits on the hot account's
   row — not connection errors. If errors, this is a shard-health
   incident → Patroni runbook instead.
2. **Shed at the edge if abusive.** The in-app per-IP limiter and the
   nginx edge limiter exist for this. If the hot key is a single
   abusive client, tighten/confirm rate limiting on that source
   rather than letting it serialise a shard.
3. **Let backpressure do its job.** `MAX_CONCURRENT_REQUESTS` +
   backpressure shed protect the rest of the system; rising
   `backpressure_shed_total` here is the system correctly
   prioritising overall availability over the hot key's throughput —
   do not raise the limit to "help" the hot key (that spreads the
   contention).
4. **If a legitimate workload** (payroll run): communicate expected
   elevated latency for that account; do not mitigate technically —
   the row lock is correctness (no double-spend). It will drain.

## Recover (return to normal)

1. Hot-account write count returns to baseline; `pg_stat_activity`
   lock waits clear.
2. p95/p99 back under SLO; `PeakloadLatencySLOBreach` clears.
3. Both shards' batch-size distributions symmetric again.

## Rollback

If you tightened a rate limit as mitigation, restore the prior value
once the spike subsides (track it as a temporary override, not a
silent permanent change — see audit R-5 "safe defaults, dangerous
overrides").

## Postmortem checklist

- Legitimate (payroll/exchange) vs abusive? If recurring and
  legitimate, consider a product-level answer (account sharding /
  sub-accounts) — a single ledger row is an inherent serialisation
  point.
- Did per-shard observability (O-10 batch-size-by-shard) make the
  "one shard hot" diagnosis fast? If not, note the gap.
- Did backpressure/rate-limit protect the rest of the system, or did
  the contention leak into pool exhaustion? If the latter, link the
  pool-sizing formula (D-2/D-6, Patroni README).
