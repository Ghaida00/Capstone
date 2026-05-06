# 0004 — In-process event bus, AMQP swap deferred

**Status:** Accepted
**Date:** 2026-03-22

## Context

`notifications` is event-driven: it reacts to `transactions.committed`,
`accounts.balance_changed`, etc. without inbound module dependencies
([ADR-0003](0003-port-adapter-shape.md)). The publishing modules
(`transactions`, `accounts`) must not know who subscribes.

The natural production transport is AMQP — RabbitMQ is already in
the stack for the transactions write path. But standing up an
AMQP-backed pub/sub for cross-module events would have meant exchange
topology, durable queues, consumer ack protocols, dead-letter routing,
and integration tests against a live broker — substantial work for a
fan-out that, today, runs entirely inside one process.

## Decision

Define two traits in `shared_kernel::events` — `EventPublisher` and
`EventSubscriber` — plus a neutral `Event` envelope. Implement them
with `tokio::sync::broadcast` for now (the `InProcessEventBus`).

Publishers and subscribers depend only on the traits. The transport is
swappable.

## Consequences

- **Zero broker complexity for fan-out events today.** No exchange
  declarations, no consumer ack handling, no DLX wiring for the event
  path. (The transactions *write* path still uses AMQP — see
  [`crates/transactions/src/infrastructure/consumer.rs`](../../crates/transactions/src/infrastructure/consumer.rs).
  This decision is about cross-module *events*, not transport.)
- **Subscribers must tolerate lag.** `tokio::broadcast` drops on
  laggy receivers. Subscribers count drops via metrics; the
  notification-log approach absorbs gaps.
- **Restart loses in-flight events.** Acceptable for notifications
  (best-effort, eventually-consistent log); unacceptable if a future
  consumer needs durable delivery — at which point we swap to AMQP.
- **Swap surface is exactly two trait impls.** When durability or
  cross-process delivery becomes load-bearing, replace
  `InProcessEventBus` with an AMQP-backed impl. Publishing/subscribing
  code does not change.
- **Notifications log is in-memory.** A `VecDeque<NotificationEntry>`
  capped at 512. Restarts lose history. The `notification_log` table
  is intentionally deferred until persistence becomes load-bearing.

## Alternatives considered

- **AMQP from day one.** ~1 week of broker plumbing for delivery
  guarantees we do not need today. Defer.
- **Outbox pattern (DB-backed event log + relay).** Right answer for
  durable cross-service events; overkill for the in-process fan-out
  we have now. Revisit when the swap above happens.
