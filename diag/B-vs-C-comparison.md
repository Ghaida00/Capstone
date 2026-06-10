# Profile B vs C — load-test-1m-sample-e2e.js (15 min, 16-core/16GB laptop)

> **CORRECTION (2026-06-10, see diag/readtail-A/):** the shared-disk-fsync
> diagnosis below is partly wrong. Instrumented probes show the read tails
> (balance p95 ~68-121 ms) come from **Docker CFS throttling of pg-haproxy**
> (`PG_HAPROXY_CPU_LIMIT=0.15` < ~0.2 demand on 2-shard profiles; 67% of
> 100 ms periods throttled). Direct-to-node reads stay <1 ms under the same
> load; raising the limit to 1.0 turns Profile A fully green, balance p95
> 79.9 ms → 3.28 ms. The e2e-timeout half of C's story was the cross-shard
> drain bug, fixed separately by the CTE apply (c173d2f). Disk fsync remains
> plausible only as a minor contributor, not the mechanism.

Same workload (~260 tx/s created, ~278 iters/s target), same k6 script, 0 errors both.

| Metric                     | Profile C (2-shard, 4 PG nodes) | Profile B (1-shard, 2 PG nodes) |
|----------------------------|---------------------------------|---------------------------------|
| tx created / s             | 258                             | 260  (≈ identical)              |
| POST /transactions p95     | 7.49 ms                         | 3.46 ms                         |
| GET /balance p95           | **121 ms ✗**                    | 4.37 ms ✓                       |
| GET /transactions p95      | 48.6 ms                         | 3.19 ms                         |
| http_req_duration p99      | 236.7 ms                        | 10.5 ms                         |
| e2e median                 | 1.49 s                          | 1.06 s                          |
| e2e p99                    | 7.23 s                          | 2.65 s                          |
| **e2e timeouts**           | **5042 (~46% of sampled) ✗**    | **0 ✓**                         |

## B resource trace (the "why nothing was saturated" proof)
- Total CPU across all 21 containers: **avg ~2.2 cores, peak ~3.2 cores of 16.** Not CPU bound.
- RabbitMQ `transactions.process` queue: peaked **366** (~1.4 s of work), drained to **0**. Consumer kept up.
- Redis `idempotency:pending`: **never had keys** (always empty) → intake stage never backed up.
- Disk writes over 15 min: primary 3.6 GB, replica 3.2 GB (~7 MB/s for the one shard).

## Diagnosis
Throughput is identical and CPU/RAM are idle in both, so this is **not** a capacity ceiling.
The difference is latency + whether the async pipeline drains. The only structural
difference is **4 PG nodes (C) vs 2 (B)** sharing **one laptop disk**.

The e2e pipeline is **commit-bound**: the consumer debits/credits + INSERTs, then commits
with `synchronous_commit=remote_write` (waits for the replica's fsync ack) + group commit.
On Docker Desktop, concurrent fsync latency spikes badly (documented in this repo's env
notes). Four PG nodes fsyncing the same virtual disk = ~2× the concurrent fsync contention
vs two → higher commit latency → consumer falls behind → queue backs up → e2e timeouts.
Reads are slow in C for the same reason: the replicas share the contended disk.

**Counterintuitive key point:** splitting writes across 2 shards *lowers* per-primary load
(130/s vs 260/s) yet C is slower. That proves the bottleneck is the **shared disk**, not
per-node capacity — adding a shard adds a node that competes for the one disk instead of
relieving it.

## Takeaway
"High-capacity" Profile C only pays off when nodes have **independent I/O** (separate disks
/ a real multi-host cluster) or when a single shard is CPU/connection-bound. On a
single-disk laptop where the bottleneck is **fsync concurrency**, more shards = more
contention = worse. The laptop's limit was never CPU (16 idle cores) — it's disk fsync.

## To make the disk claim airtight (not yet measured directly)
We measured bytes, not fsync wait. Re-run C with `diag/sample-stats.sh profileC 960 5`
and expect: `transactions.process` queue grows large/unbounded (vs B's ≤366), CPU still
idle. That directly confirms "consumer can't keep up" — which, with idle CPU, means disk.
