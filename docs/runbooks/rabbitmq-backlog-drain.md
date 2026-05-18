# Runbook: RabbitMQ backlog growing (consumer not draining)

**Last reviewed:** 2026-05-18
**Owner:** payments on-call
**Related ADRs / code:**
[consumer.rs](../../crates/transactions/src/infrastructure/consumer.rs)
(`QUEUE_NAME = transactions.process`, `BATCH_SIZE = 100`,
`BATCH_FLUSH_MS = 50`), [callback.rs](../../crates/shared_kernel/src/queue/callback.rs).

## Symptom

- `transactions.process` queue depth climbing in the RabbitMQ UI /
  `rabbitmqctl list_queues`.
- 202→`completed` latency rising: clients get 202 but the row stays
  `processing` far longer than the ~`BATCH_FLUSH_MS/2` design point.
- `transactions_processed_total` rate flat or zero while inbound POST
  rate is non-zero; possibly `transactions_requeued_total` or
  `amqp_channel_close_total` / `amqp_consumer_cancel_total` climbing
  (consumer went deaf — see O-14 callback handling).

## Severity & blast radius

- Customer-visible? Partial — writes are *accepted* (money is safe in
  the outbox/queue) but not *reflected*; balances/history lag.
- Reversible? Yes — messages are durable; draining catches up.
- Time-to-broken: degrades gradually; becomes broken if the broker
  hits a memory/disk alarm and blocks publishers (then POSTs 5xx).

## Detect / confirm

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
# Expect: transactions.process with consumers >= 1. consumers = 0 is
# the smoking gun (consumer crashed or was broker-cancelled).

curl -sG http://<host>:9090/api/v1/query \
  --data-urlencode 'query=rate(transactions_processed_total[1m])'
curl -sG http://<host>:9090/api/v1/query \
  --data-urlencode 'query=rate(amqp_consumer_cancel_total[5m])'
docker compose logs --since 10m peakload-app-1 | grep -iE "consumer|cancel|nack|requeue"
```

Grafana: row **🚨 Write Path Under Failure** → *Broker-Initiated
Cancel / Channel Close* and *Outbox Ship Rate (per shard)*.

## Mitigate (stop the bleed)

1. **consumers = 0** (consumer deaf — broker cancelled or channel
   closed): the consumer callback fires the cancellation token to
   trigger re-subscribe; if it has not recovered, bounce the app:
   ```bash
   docker compose restart app
   docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
   ```
   Expected: `consumers >= 1`, `messages` starts falling.
2. **consumers ≥ 1 but draining slower than inbound**: the consumer
   is rate-bounded by `BATCH_SIZE` / DB write throughput. Check the
   DB write path is healthy (a slow/locked shard throttles the
   consumer — see the Patroni and hot-key runbooks). Resolve the DB
   bottleneck first; the consumer then catches up on its own.
3. **Broker memory/disk alarm** (publishers blocked, POST 5xx):
   ```bash
   docker compose exec rabbitmq rabbitmqctl status | grep -A3 alarm
   ```
   Free the resource (disk) or raise the watermark only as a
   stopgap; the real fix is draining the backlog (steps 1–2).
4. **Poison loop** (`dlq_messages_total` / `transactions_requeued_total`
   climbing fast): a message batch keeps failing non-fatally and
   requeuing. Inspect logs for the SQLSTATE; poison messages
   (CHECK/overflow) should route to the DLX automatically — if they
   are requeuing instead, that is a classifier gap to escalate.

## Recover (return to normal)

1. `list_queues` shows `messages` trending to ~0 and `consumers ≥ 1`.
2. 202→`completed` latency back to sub-second.
3. `rate(transactions_processed_total[1m])` ≥ inbound POST rate until
   the backlog clears.

## Rollback

Restarting the app to restore the consumer is safe (messages are
durable, idempotency-keyed). Do **not** purge the queue to "fix" the
backlog — that is silent money loss; every message is an accepted
transaction.

## Postmortem checklist

- Root cause: consumer-deaf (broker cancel/close) vs DB-throttled vs
  broker-resource? Each links a different upstream runbook.
- If consumer-deaf: did the O-14 callback fire the cancellation
  token and trigger re-subscribe, or did it need a manual bounce?
- Was the backlog paged before 202→completed SLO breach, or did a
  customer notice first?
