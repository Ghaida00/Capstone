# Disaster Recovery — Honest Capstone Status

**Last reviewed:** 2026-05-21
**Status:** Deferred (capstone scope). This document exists so the
gap is **explicit**, not silently absent.

---

This document answers the four questions every disaster-recovery
document should answer (per Campbell & Majors, *Database Reliability
Engineering*, Ch. 10), at the capstone bar. The honest answers are
mostly "nothing" or "manual"; the value of writing them down is that
a future operator (or evaluator) does not have to guess what the
RPO / RTO is before they can act on an outage.

---

## 1. What is backed up, where, how often?

**Nothing automated.** No `pg_dump` cron, no WAL archive, no
`wal-g` / `pgBackRest` sidecar, no S3 target. Docker volumes are
the only persistence — they live on the dev host's disk until
`docker compose down -v` deletes them.

The full audit framing of this gap is [OPS-4](audit/2026-05-16-phase2-ops-infrastructure.md#ops-4---no-pitr--scheduled-backup-story-for-postgres):
the missing backup story is named there as "the single biggest gap
on the ops axis from a customer-trust standpoint." For the capstone
it is deliberately deferred (the system holds synthetic data, not
real money); for a production deploy this would be the **first**
remediation item.

## 2. RPO / RTO per failure class

| Failure class | RPO | RTO | Notes |
|---|---|---|---|
| Single shard node loss (primary or replica) | **0** | **5–15s** | Patroni intra-shard failover; the surviving node promotes. App's transient-error retry wrapper ([shared_kernel/db/failover.rs](../crates/shared_kernel/src/db/failover.rs)) soaks the window for idempotent writes. This is the one failure class the system actually handles. |
| Both nodes of a single shard lost | **∞** | **manual rebuild** | Re-run `init.sql`; data on that shard is gone. |
| Whole etcd cluster quorum loss | **0** (eventual; primaries demote to read-only) | **manual etcd recovery** | All Postgres primaries demote to read-only until etcd quorum is restored. R-9 graceful-degradation gate (WP7) flips automatically to `read_only` if wired via the env, so reads keep serving while writes shed with 503. |
| Host (laptop) loss | **∞** | **manual restore from `init.sql`** | Compose stack is local-only; data lives in named Docker volumes; loss of the host = loss of every shard. |
| Accidental `docker compose down -v` | **∞** | **manual restore from `init.sql`** | Same as host loss but self-inflicted. |
| Logical corruption (runaway DELETE / migration) | **∞** | **manual restore from `init.sql`** | Patroni replicates the corruption to both nodes; without WAL archive there is no point-in-time recovery target. |

## 3. How do I restore?

For the only restore-from-scratch path the capstone supports:

```bash
docker compose down -v       # destroys all data volumes
docker compose up -d --build # re-creates with init.sql applied via db-bootstrap-schema
```

There is no other procedure. `init.sql` is the only schema source;
no PITR target, no `pg_restore` from a backup file.

For cross-shard outbox stuck rows (R-2 / R-8 — separate failure
class from "shards lost"), the runbook is at
[docs/runbooks/cross-shard-outbox-reconciliation.md](runbooks/cross-shard-outbox-reconciliation.md).
That is a money-safety recovery procedure, not a DR procedure.

## 4. How do I verify restore works?

There is no automated DR drill. The capstone equivalent is the
testcontainers integration test suite ([crates/transactions/tests/event_flow.rs](../crates/transactions/tests/event_flow.rs))
which spins up the full data plane from scratch on every test run
and asserts the schema applies cleanly — that is the only ongoing
"can we rebuild from `init.sql`?" gate. WP4's CI workflow runs it
on every PR.

A real DR drill would be a `make dr-drill` target that:

1. Takes a known-state snapshot of `users` + `transactions`.
2. Wipes the volumes.
3. Restores from the (not-yet-existing) backup.
4. Verifies the snapshot matches what was restored.

That is the audit's prescribed pattern; it is the natural extension
once a backup story (OPS-4) lands.

---

## Why this stub exists

The audit's DOC-6 finding said: "the cheapest acceptable state is
'we don't have backups; here is what restore means in that case' —
*written down honestly*." This document is that. It is **not** a
production-grade DR procedure; it is the honest record of what
recovery means today so a future operator does not have to discover
it during an incident.

The path forward is named in the audit's
[OPS-4 prescribed pattern](audit/2026-05-16-phase2-ops-infrastructure.md#ops-4---no-pitr--scheduled-backup-story-for-postgres):
either a minimal `pg_dump` cron container (RPO ≈ 24h) or the full
`pgBackRest` sidecar with continuous WAL archive (RPO ≤ 60s).
Neither is committed; both are tractable from this baseline.

## References

- [docs/audit/2026-05-16-phase2-ops-infrastructure.md](audit/2026-05-16-phase2-ops-infrastructure.md) — OPS-4 backup gap, OPS-5 HAProxy SPOF, OPS-7 production-deployment gap
- [docs/audit/2026-05-16-phase2-documentation.md](audit/2026-05-16-phase2-documentation.md) — DOC-6 DR doc absence (this document closes that finding)
- [docs/runbooks/](runbooks/) — money-safety runbooks (cross-shard reconciliation, Patroni failover, etcd quorum loss, Redis Sentinel, RabbitMQ backlog, hot-key spike)
- *Database Reliability Engineering* (Campbell & Majors, O'Reilly 2018), Ch. 10 — the RPO / RTO framing this document uses
