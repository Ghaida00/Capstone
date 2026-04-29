# `shared_kernel` — cross-cutting infrastructure

**Not a domain module.** This crate is the horizontal layer every
business module depends on. It owns *no* business rules and *no*
tables. If logic here started to know about accounts, transactions,
or notifications, it would belong in that domain crate instead.

For why this crate exists at all see
[ADR-0002](../../docs/adr/0002-cargo-workspace-split.md). For why
the event bus inside it stays in-process see
[ADR-0004](../../docs/adr/0004-in-process-event-bus.md).

## What lives here

| Module                      | Purpose                                                                 |
|-----------------------------|-------------------------------------------------------------------------|
| [`db`](./src/db/)           | sqlx pools, sharded router, primary/replica routing, transient-error retry |
| [`cache`](./src/cache/)     | Redis facade with Sentinel-aware master discovery                       |
| [`queue`](./src/queue/)     | RabbitMQ producer (consumer lives in `transactions`)                    |
| [`events`](./src/events.rs) | Type-neutral event bus — `Event` envelope + `EventPublisher` / `EventSubscriber` traits + `InProcessEventBus` |
| [`error`](./src/error.rs)   | Application-wide `AppError` + `IntoResponse` so every module returns identical error JSON |
| [`responses`](./src/responses.rs) | Standard JSON response wrapper used by every module's HTTP layer  |

## Tables owned

None. This crate never speaks SQL on its own behalf — it only hands
out typed pool handles to the modules that do.

## Ports exposed

None. There is no `ports.rs` because no business module asks
`shared_kernel` to "do" anything domain-shaped. Modules import
concrete types (`ShardRouter`, `RedisCache`, `QueueProducer`, the
event traits) directly.

## Ports consumed

None. This crate sits at the bottom of the dependency graph.

## Dependency rule

```
shared_kernel  ←  accounts
              ←  transactions
              ←  notifications
              ←  app
```

`shared_kernel` may import from the standard library and external
crates only. It MUST NOT `use` anything from `accounts`,
`transactions`, `notifications`, or `app`. Phase 4 made this a
compile error: those crates simply do not appear in this crate's
`Cargo.toml`, so the import would not resolve.

## Operational notes

- **DB failover window** is 5–15 s.
  [`db/failover.rs`](./src/db/failover.rs) wraps idempotent writes
  in a retry budget sized to soak that window. Tune via the
  `DB_RETRY_*` env vars.
- **Redis Sentinel monitor** runs as a background task started by
  `RedisCache::new`. It watches for master address changes and
  bumps `redis_master_failover_total` on every promotion observed.
- **Event bus uses `tokio::broadcast`** which drops messages on
  laggy receivers. Subscribers are expected to count drops via
  metrics rather than rely on lossless delivery — see ADR-0004.
- **Queue consumer is NOT here.** It used to be, before Phase 2.
  It now lives in [`transactions/src/infrastructure/consumer.rs`](../transactions/src/infrastructure/consumer.rs)
  because consuming a `transactions` exchange is a transactions
  concern. Only the producer half stayed in shared_kernel because
  every module may need to publish.
