# Disaster Recovery — Honest Capstone Status

**Last reviewed:** 2026-06-10
**Status:** Backup/PITR is implemented as an **env toggle**
(`BACKUP_ENABLED`, §3.6c in `.env.example`) — **off by default**,
intended on for cloud deploys with object storage. With the toggle
off (every laptop profile), everything below the toggle section
still describes reality. The restore procedure is documented but
**has not been rehearsed** — until one restore has actually been
performed on the cloud box, treat the backup as unproven.

---

This document answers the four questions every disaster-recovery
document should answer (per Campbell & Majors, *Database Reliability
Engineering*, Ch. 10), at the capstone bar. The honest answers are
mostly "nothing" or "manual"; the value of writing them down is that
a future operator (or evaluator) does not have to guess what the
RPO / RTO is before they can act on an outage.

---

## 1. What is backed up, where, how often?

**With `BACKUP_ENABLED=false` (default, all laptop profiles):
nothing automated.** Docker volumes are the only persistence —
they live on the dev host's disk until `docker compose down -v`
deletes them.

**With `BACKUP_ENABLED=true` (intended for cloud):** pgBackRest
(baked into the Patroni image, dormant when off) archives every
completed WAL segment from each shard primary to the repo named by
the `BACKUP_*` vars (S3-compatible object storage, or a posix path
for testing), lz4-compressed, optionally encrypted at rest.
`PG_ARCHIVE_TIMEOUT` (default 300 s) bounds idle RPO. Base backups
run from host cron via [scripts/backup-shard.sh](../scripts/backup-shard.sh)
(full Sunday, diff weekdays; stanza per shard = `peakload-shardN`,
shared by both nodes so a failover keeps archiving to the same
stanza — verified across a live timeline switch 2026-06-10).
**Enable-time rule:** run `backup-shard.sh` immediately after the
first boot with backups on — until `stanza-create` has run,
archive attempts fail and Postgres retains WAL; alert on
`pg_stat_archiver.failed_count`.

This closes the audit's [OPS-4](audit/2026-05-16-phase2-ops-infrastructure.md#ops-4---no-pitr--scheduled-backup-story-for-postgres)
("the single biggest gap on the ops axis") for toggle-on deploys.
Toggle-off deploys deliberately retain the old posture: the laptop
holds synthetic data and is `down -v`'d routinely.

## 2. RPO / RTO per failure class

| Failure class | RPO | RTO | Notes |
|---|---|---|---|
| Single shard node loss (primary or replica) | **0** | **5–15s** | Patroni intra-shard failover; the surviving node promotes. App's transient-error retry wrapper ([shared_kernel/db/failover.rs](../crates/shared_kernel/src/db/failover.rs)) soaks the window for idempotent writes. This is the one failure class the system actually handles. |
| Both nodes of a single shard lost | **∞** | **manual rebuild** | Re-run `init.sql`; data on that shard is gone. |
| Whole etcd cluster quorum loss | **0** (eventual; primaries demote to read-only) | **manual etcd recovery** | All Postgres primaries demote to read-only until etcd quorum is restored. R-9 graceful-degradation gate (WP7) flips automatically to `read_only` if wired via the env, so reads keep serving while writes shed with 503. |
| Host (laptop) loss | **∞** | **manual restore from `init.sql`** | Compose stack is local-only; data lives in named Docker volumes; loss of the host = loss of every shard. |
| Accidental `docker compose down -v` | **∞** | **manual restore from `init.sql`** | Same as host loss but self-inflicted. |
| Logical corruption (runaway DELETE / migration) | **∞** | **manual restore from `init.sql`** | Patroni replicates the corruption to both nodes; without WAL archive there is no point-in-time recovery target. |

The `∞` rows above describe the **toggle-off** default. With
`BACKUP_ENABLED=true` (§1), every class except "single node loss"
changes to RPO ≈ seconds under load / ≤ `PG_ARCHIVE_TIMEOUT` idle,
RTO = the §3 restore procedure (minutes-to-hours, unrehearsed) —
for host loss this additionally assumes the repo is object storage,
not a volume on the lost host.

## 3. How do I restore?

For the only restore-from-scratch path the capstone supports:

```bash
docker compose down -v       # destroys all data volumes
docker compose up -d --build # re-creates with init.sql applied via db-bootstrap-schema
```

There is no other procedure with the toggle off. `init.sql` is the
only schema source; no PITR target, no `pg_restore` from a backup
file.

**With `BACKUP_ENABLED=true`, per-shard restore (outline — must be
rehearsed before it can be trusted):**

```bash
# 1. Stop both nodes of the affected shard.
docker compose stop pg-shardN-node-a pg-shardN-node-b

# 2. Clear the shard's Patroni state from etcd so the restored
#    data dir becomes the new cluster truth (run from any other
#    patroni container; confirms interactively).
docker exec -it peakload-pg-shard<other>-node-a \
  patronictl -c /var/lib/patroni/patroni.yml remove peakload-shardN

# 3. On the target node: wipe PGDATA, restore (add
#    --type=time --target='...' for PITR), replay, promote.
docker compose run --rm --no-deps --entrypoint bash pg-shardN-node-a
  rm -rf "$PGDATA"/*
  gosu postgres pgbackrest --stanza=peakload-shardN restore [--type=time --target='2026-...']

# 4. Start node-a; with the DCS state cleared and a populated
#    PGDATA, Patroni adopts the restored dir and leads.
docker compose up -d pg-shardN-node-a

# 5. Re-seed the replica from the new leader (empty data dir →
#    Patroni runs pg_basebackup automatically).
docker exec peakload-pg-shardN-node-b sh -c 'rm -rf "$PGDATA"/*'
docker compose up -d pg-shardN-node-b
```

Cross-shard caveat: restoring ONE shard to an earlier point in
time desynchronizes it from the other shards' present (a
cross-shard transfer can end up half-rewound). After any PITR,
run the [cross-shard outbox reconciliation](runbooks/cross-shard-outbox-reconciliation.md)
sweep and treat its report as the source of truth for repair.

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

That is the audit's prescribed pattern; with the backup toggle now
in place, the drill becomes runnable on any toggle-on deploy: take
a backup, note a row count + checksum, wipe one shard, walk §3,
compare. **Do this once on the cloud box before calling the backup
real.** What WAS verified (2026-06-10, posix repo, local): toggle
renders config and turns archiving on; stanza survives a Patroni
failover/timeline switch; full backup of a live shard completes
(159 MB → 47 MB, ~11 s) and `pgbackrest info` reports `status: ok`;
toggle-off recreate returns the node to `archive_mode=off` with the
config removed. The restore path is the untested half.

---

## Why this stub exists

The audit's DOC-6 finding said: "the cheapest acceptable state is
'we don't have backups; here is what restore means in that case' —
*written down honestly*." This document is that. It is **not** a
production-grade DR procedure; it is the honest record of what
recovery means today so a future operator does not have to discover
it during an incident.

Of the audit's
[OPS-4 prescribed patterns](audit/2026-05-16-phase2-ops-infrastructure.md#ops-4---no-pitr--scheduled-backup-story-for-postgres)
(minimal `pg_dump` cron vs. full pgBackRest WAL archive), the
pgBackRest path is the one implemented — as the `BACKUP_ENABLED`
toggle described in §1, with RPO ≈ seconds under load and bounded
by `PG_ARCHIVE_TIMEOUT` when idle. What keeps this document honest
rather than done: the toggle defaults off, and no restore has been
rehearsed yet.

## References

- [docs/audit/2026-05-16-phase2-ops-infrastructure.md](audit/2026-05-16-phase2-ops-infrastructure.md) — OPS-4 backup gap, OPS-5 HAProxy SPOF, OPS-7 production-deployment gap
- [docs/audit/2026-05-16-phase2-documentation.md](audit/2026-05-16-phase2-documentation.md) — DOC-6 DR doc absence (this document closes that finding)
- [docs/runbooks/](runbooks/) — money-safety runbooks (cross-shard reconciliation, Patroni failover, etcd quorum loss, Redis Sentinel, RabbitMQ backlog, hot-key spike)
- *Database Reliability Engineering* (Campbell & Majors, O'Reilly 2018), Ch. 10 — the RPO / RTO framing this document uses
