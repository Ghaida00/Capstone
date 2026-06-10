# Residual Risk Register — Known, Accepted, Watched

**Last reviewed:** 2026-06-10
**Status of the system at review:** all four profile cells (A/B/C/D)
pass T2 full certification at 260 tx/s sustained
(see [runbooks/exhibition-readiness.md](runbooks/exhibition-readiness.md),
matrix-20260610-101440).

This document lists every known risk that is **deliberately not fixed**
at the capstone bar, so an operator or evaluator reads the system's
true posture here instead of discovering it during an incident. Each
entry says what the exposure is, why it is accepted, what (if anything)
bounds the blast radius today, and what would force a fix. A claim of
"drop-in replacement with no regression" is only honest alongside this
list.

| # | Risk | Exposure | Bounded by | Fix trigger |
|---|------|----------|------------|-------------|
| 1 | No backup / PITR | Logical corruption or full-shard loss is unrecoverable (RPO ∞) | Patroni replica per shard (node loss only) | Any real money data — fix **first** |
| 2 | pg-haproxy is a SPOF | All DB connections cross one proxy container | Container restart policy; healthchecks | Multi-host deploy or HA requirement |
| 3 | pg-haproxy CPU-cap sensitivity | Under-provisioned `cpus:` limit re-creates ~100 ms CFS tails | Limit raised 0.15→0.5; ~6% periods still throttle on 3-shard D | Adding shards/read load without re-budgeting |
| 4 | RabbitMQ classic (non-quorum) queues | Broker node loss can drop queued (accepted-not-applied) transactions | Single-node broker anyway; outbox reconciliation runbook for cross-shard rows | Multi-node RabbitMQ or durability SLA |
| 5 | tx-status cache race (mitigated) | Status can read stale "pending" up to ~1 s after terminal | 1 s transient-cache TTL; terminal states cached only when terminal | Client SLA on status freshness < 1 s |
| 6 | Single-host compose topology | Host pause/loss freezes or kills everything at once | 1 s pool-acquire timeout sheds load cleanly (verified 2026-06-10) | Production deploy → multi-host orchestration |
| 7 | Auth disabled by default | API is unauthenticated unless `ENABLE_AUTH=true` | Intended deployment is behind a trusted edge | Any internet-reachable deploy — enable + set secrets |

---

## 1. No backup / PITR (the one true production blocker)

Full statement in [disaster-recovery.md](disaster-recovery.md) (audit
item OPS-4). Patroni protects against **node** failure, not **data**
failure: a runaway `DELETE` or bad migration replicates to the replica
within milliseconds. There is no WAL archive, so no point-in-time
recovery target exists.

**Accepted because:** the capstone holds synthetic data.
**Not acceptable for:** anything holding real balances. This is the
first remediation item for a production deploy, ahead of everything
else in this register.

## 2. pg-haproxy single point of failure

One HAProxy container routes every write (`:5000–5002` → Patroni
primary) and every Tier-1 read (`:5010–5012` → replicas). If the
container dies, the app loses all database connectivity until Docker
restarts it (seconds, not minutes — but a full brownout while it
lasts). There is no second proxy or client-side failover path.

**Accepted because:** single-host compose already has the host itself
as a bigger SPOF (#6); a second proxy on the same host buys little.
**Revisit when:** the stack spans hosts — then run ≥2 HAProxy
instances behind the app's multi-URL support, or move routing
client-side.

## 3. pg-haproxy CPU-cap sensitivity (CFS throttling)

Empirically established 2026-06-10: with `PG_HAPROXY_CPU_LIMIT=0.15`
and ~0.21 CPU demand, Docker CFS froze the proxy for the remainder of
67% of 100 ms scheduler periods, quantizing read tails to ~70–100 ms
and breaching balance p95 by ~10× on 2-shard profiles. Raising the
limit to 0.5 (and pgBouncer to 0.3) fixed certification, but **the
failure mode is structural**: every connection in the system crosses
this proxy, so its CPU budget must scale with shard count and load.
At 0.5 under the 3-shard D profile it still throttles ~6% of periods
— inside thresholds, but with little headroom.

**Diagnostic signature:** latency tails clustering just under ~100 ms
(or multiples) → check `nr_throttled` in the container's
`/sys/fs/cgroup/cpu.stat` **before** theorizing about disk or network
(`diag/read-tail-probe.sh` compares direct vs proxied paths).
**Revisit when:** adding shards, raising sustained load, or moving to
a host where 0.5 CPU is a larger relative slice — re-measure, don't
assume.

## 4. RabbitMQ classic queues (audit item OPS-3)

`transactions.process` is a classic queue on a single-node broker.
Loss of the broker node loses any transactions that were accepted
(202 returned, intake recorded) but not yet applied by the consumer.
Quorum queues would survive broker-node loss in a multi-node cluster;
on a single node they buy nothing, which is why this is deferred
together with the single-host topology (#6).

**Bounded by:** the cross-shard outbox and its
[reconciliation runbook](runbooks/cross-shard-outbox-reconciliation.md)
cover the money-safety half (no half-applied transfers); the loss here
is "accepted work disappears," visible to clients as a transaction
stuck in `pending`.
**Revisit when:** the broker goes multi-node — switch to quorum queues
in the same change.

## 5. Transaction-status cache race (mitigated, not eliminated)

History: terminal-only status caching (0d09c0f) left transient
`pending` polls uncached, which under load pressured the single-shard
reader pool into 5xx and an nginx 502 storm. The fix (576cc1c) added a
1 s transient cache. The residual race: a status cached as `pending`
can be served for up to ~1 s after the consumer actually completed the
transaction. No incorrect terminal state is ever served; the staleness
is one-directional and bounded by the TTL.

**Accepted because:** the e2e SLO is seconds-scale; a ≤1 s stale
`pending` is invisible inside it.
**Revisit when:** any client contract requires status freshness
tighter than the TTL.

## 6. Single-host topology — host pauses hit everything at once

The 2026-06-10 D-profile certification run recorded the failure mode
live: a ~1.5 s host-level stall (Docker Desktop memory pressure) froze
in-flight queries on **both** app replicas and reset Patroni REST
health connections on two shards in the same second. 41 read requests
(0.008%) returned 500 via the 1 s pool-acquire timeout, and the system
recovered in under 2 s with zero nginx upstream errors and no cascade.

That is the **designed** degradation path (shed, don't queue), and it
held. But the trigger is structural: on one host, every container
shares the same pause, so no amount of intra-stack redundancy converts
this into high availability.

**Accepted because:** capstone runs on one machine by definition;
[architecture.md](architecture.md) documents compose-as-prod honestly.
**Revisit when:** deploying for availability — the fix is more hosts,
not more containers.

## 7. Authentication off by default

`ENABLE_AUTH` defaults to false; without it every API endpoint is
unauthenticated. The `.env*.example` files ship `CHANGEME` credentials
for every backing service, which is correct for a private dev loop and
wrong for anything reachable from the internet.

**Operational rule:** an internet-reachable deploy (e.g. the cloud
soak box) must set real secrets **and** `ENABLE_AUTH=true` with its
key material, before the stack comes up — not after.

---

## What is deliberately *not* on this list

* **Read/balance latency tails** — closed 2026-06-10 (#3 was the
  cause); all profiles certify with balance p95 ≤ 5.4 ms.
* **Cross-shard drain throughput** — closed by the single-statement
  CTE apply (c173d2f); Profile C full cert shows 0 errors, e2e p99
  2.57 s.
* **Profile D viability on 16 GB** — certified 2026-06-10; the old
  "lab only if <12 GB RAM" caveat is obsolete.
