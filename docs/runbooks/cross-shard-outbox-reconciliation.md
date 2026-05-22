# Runbook: Cross-shard outbox stuck (credit terminal-fail / refund-stuck)

**Last reviewed:** 2026-05-18
**Owner:** payments on-call
**Related ADRs / code:**
[cross_shard_processor.rs](../../crates/transactions/src/infrastructure/cross_shard_processor.rs),
[admin.rs](../../crates/app/src/admin.rs),
[slo.alerts.yml](../../prometheus/rules/slo.alerts.yml) (group `peakload-cross-shard-outbox`),
audit R-2 / R-8 ([phase2-resilience-slo.md](../audit/2026-05-16-phase2-resilience-slo.md)).

## Symptom

One of two alerts fired:

- **`PeakloadCrossShardCreditTerminalFailure`** (severity: page).
  A cross-shard credit exhausted `MAX_ATTEMPTS` (10). The sender was
  debited, the recipient was never credited, the outbox row is
  `status='failed'`, and (R-8) the sender's `transactions` audit row
  is `status='failed'` with `failure_reason` like
  `max attempts at credit: …`. **Money is stranded** — it left the
  sender and reached no one.
- **`PeakloadCrossShardRefundStuck`** (severity: ticket). A
  compensating refund has failed past `MAX_ATTEMPTS` and is in the
  300 s defer-lease loop. The sender was debited, the recipient was
  correctly *not* credited, but the sender has **not been refunded**.
  The outbox row stays `status='pending'`, `attempts` held at 9.

## Severity & blast radius

- Customer-visible? **Yes.** Credit-terminal: sender sees a `failed`
  transaction (R-8) — they can safely retry; the money is gone from
  their balance until reconciled. Refund-stuck: sender sees
  `processing` indefinitely and is down the money until the refund
  lands.
- Reversible? **Yes, with manual action.** No data is lost; the
  outbox + audit rows are the durable record of exactly what is owed.
- Time-to-broken: already broken at alert time — this is a
  money-correctness incident, not a degradation. Treat credit-terminal
  as page-now.

## Detect / confirm

Enumerate the affected rows via the admin surface (requires
`ENABLE_AUTH=true` + a JWT with `role:"admin"`):

```bash
# Credit terminal-fails, both shards, older than 5 min:
curl -s -H "Authorization: Bearer $ADMIN_JWT" \
  "http://<host>:8080/api/v2/admin/outbox?status=failed&age_gt_secs=300"

# Refund-stuck rows:
curl -s -H "Authorization: Bearer $ADMIN_JWT" \
  "http://<host>:8080/api/v2/admin/outbox?status=refund-stuck&age_gt_secs=300"

# Sender audit rows still 'processing' (the refund-stuck customers):
curl -s -H "Authorization: Bearer $ADMIN_JWT" \
  "http://<host>:8080/api/v2/admin/stuck-transactions?age_gt_secs=300"
```

If the admin surface is unavailable, query the shard primary directly
(HAProxy direct-to-primary ports: 5000 = shard0, 5001 = shard1):

```bash
docker compose exec pg-haproxy true   # confirm reachable
PGPASSWORD=$POSTGRES_PASSWORD psql -h <host> -p 5000 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT id, from_account, to_account, to_shard, amount, attempts,
          refund_required, status, last_error
     FROM cross_shard_outbox
    WHERE status='failed'
       OR (status='pending' AND attempts>=10 AND refund_required)
    ORDER BY updated_at;"
```

Grafana: dashboard **Peakload Capstone** → row **🚨 Write Path Under
Failure** → panel *Cross-Shard Terminal Failures & Refund-Stuck (R-2)*.

## Mitigate (stop the bleed)

Classify each row first — the fix differs by case.

### Case A — credit-terminal, recipient *does* exist (transient outage)

The receiver shard was down/unreachable during all 10 attempts but
the recipient account is real and active. **Re-drive** the row:

```sql
-- On the SENDER shard primary (the shard that owns the outbox row).
-- Re-arms exactly one more attempt: claim filter is `attempts < 10`.
BEGIN;
UPDATE cross_shard_outbox
   SET status='pending', attempts=9, lease_until=NULL, last_error=NULL
 WHERE id='<outbox_id>';
-- Re-open the sender audit row so the re-driven credit can complete it:
UPDATE transactions
   SET status='processing', failure_reason=NULL
 WHERE reference_id='<reference_id>' AND from_account='<from_account>';
COMMIT;
```

Expected result: within ~1 s the processor re-claims the row, the
credit lands on the now-healthy receiver shard, outbox → `completed`,
audit row → `completed`. Confirm with the admin `outbox` query
returning empty for that id. If it terminal-fails again → the
receiver problem is not transient; go to Case B.

### Case B — credit-terminal, recipient never existed / inactive

A real recipient was never going to receive this. Do **not** re-drive
(it will just terminal-fail again). The correct outcome is a
sender refund. Flip the row onto the refund path:

```sql
-- SENDER shard primary.
BEGIN;
UPDATE cross_shard_outbox
   SET status='pending', attempts=0, refund_required=TRUE,
       lease_until=NULL, last_error=NULL
 WHERE id='<outbox_id>';
COMMIT;
```

Expected result: the processor takes the refund path
(`refund_sender`), credits the sender back, flips the audit row to
`reversed`, marks the outbox `completed`. Confirm sender balance
restored and audit row `status='reversed'`.

### Case C — refund-stuck

The refund itself keeps failing (e.g. sender account constraint,
balance overflow, sender shard issue). Inspect `last_error` on the
row. Fix the underlying cause (most commonly: the sender account
was deactivated — reactivate it, or escalate to manual ledger
adjustment). Once the cause is fixed the 300 s defer-lease will
re-attempt automatically; to force an immediate retry:

```sql
UPDATE cross_shard_outbox SET lease_until=NULL WHERE id='<outbox_id>';
```

If the refund can never succeed automatically (account permanently
gone), perform the **manual ledger credit**:

```sql
-- SENDER shard primary. Single tx so balance + audit move together.
BEGIN;
UPDATE users
   SET balance = balance + <amount>
 WHERE account_number='<from_account>' AND status='active';
UPDATE transactions
   SET status='reversed', processed_at=NOW(), updated_at=NOW()
 WHERE reference_id='<reference_id>' AND from_account='<from_account>'
   AND status <> 'reversed';
UPDATE cross_shard_outbox
   SET status='completed', last_error='manual reconciliation <ticket>'
 WHERE id='<outbox_id>';
COMMIT;
```

Record the ticket id in `last_error` so the audit trail of operator
actions is in the DB, not just chat.

## Recover (return to normal)

1. Re-run the admin `outbox` and `stuck-transactions` queries — both
   should return empty for the affected window.
2. Confirm the R-2 alert has cleared in Prometheus
   (`/api/v1/alerts`).
3. Spot-check affected sender balances against expectation.

## Rollback

The SQL above is idempotent in spirit but not trans! If a re-drive
(Case A) races a manual credit (Case C) you can double-pay. **Never**
run Case A/B and Case C on the same row. If unsure, set
`status='failed', lease_until=NOW()+INTERVAL '1 hour'` to freeze the
row out of the working set while you investigate, then proceed.

## Postmortem checklist

- Was the receiver-shard outage (Case A) itself alerted? If a shard
  was down for 10 attempts (~minutes) without its own page, that is a
  separate gap.
- Did `PeakloadCrossShardCreditTerminalFailure` fire within 1 min as
  designed? If the lag was longer, check Prometheus scrape health.
- How many rows? A single row is reconciliation; a batch is a
  receiver-shard or routing incident — link the shard runbook.
