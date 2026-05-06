# `app` — composition root and binary

**Not a domain module.** This is the only `[[bin]]` in the workspace
and its single job is to wire the other crates together. If you find
yourself adding business logic here, it belongs in `accounts`,
`transactions`, or `notifications` instead.

For why the workspace has a dedicated composition-root crate see
[ADR-0002](../../docs/adr/0002-cargo-workspace-split.md).

## What lives here

| File / module                                       | Purpose                                                            |
|-----------------------------------------------------|--------------------------------------------------------------------|
| [`main.rs`](./src/main.rs)                          | Process entry point, mimalloc allocator, distroless `--health-check` probe |
| [`app.rs`](./src/app.rs)                            | `App` struct: holds the wired router + cancel token, `.run()` until shutdown |
| [`bootstrap.rs`](./src/bootstrap.rs)                | `init_tracing` / `init_metrics` / `init_infrastructure` / `build_router` — the actual wiring |
| [`config.rs`](./src/config.rs)                      | Env-var → typed `Config`. Single place env-var keys are spelled out |
| [`health.rs`](./src/health.rs)                      | `/health` and `/metrics` handlers                                  |
| [`middleware/`](./src/middleware/)                  | Cross-cutting request-protection stack — see below                 |

## Middleware stack

Applied uniformly to every `/api/v2/*` sub-router via
`apply_protection_stack` in [`bootstrap.rs`](./src/bootstrap.rs):

```
request → backpressure → circuit_breaker → rate_limit → auth → handler
```

| File                                                              | Concern                                                |
|-------------------------------------------------------------------|--------------------------------------------------------|
| [`middleware/auth.rs`](./src/middleware/auth.rs)                  | Optional JWT (pass-through when `ENABLE_AUTH=false`)   |
| [`middleware/rate_limit.rs`](./src/middleware/rate_limit.rs)      | Redis-backed per-key token bucket                      |
| [`middleware/circuit_breaker.rs`](./src/middleware/circuit_breaker.rs) | Sliding-window error trip                          |
| [`middleware/backpressure.rs`](./src/middleware/backpressure.rs)  | Concurrency cap with shed metric                       |
| [`middleware/request_id.rs`](./src/middleware/request_id.rs)      | Propagates `X-Request-Id` for tracing                  |
| [`middleware/metrics.rs`](./src/middleware/metrics.rs)            | Per-request Prometheus counters / histograms           |

## What it wires (in `build_router`)

```text
build_router
├── accounts::init       → accounts::router        → /api/v2/accounts
├── transactions::init   → transactions::router    → /api/v2/transactions
│       (cross-module dep: accounts::ports::DynAccountService)
├── notifications::init  → notifications::router   → /api/v2/notifications
│       (subscribes to shared_kernel::events::EventSubscriber)
├── /health   → health::health_check
└── /metrics  → health::prometheus_metrics
```

Each module crate's `init()` returns its own state bundle and
`router()` consumes that bundle, so by the time `nest_service` mounts
them the parent router does not need a `FromRef<AppState>` impl. The
parent's `AppState` only carries shared infra handles (shard router,
cache, queue producer, metrics handle) — it does not carry per-module
deps.

## Tables owned

None.

## Ports exposed

None. `app` is the consumer side of every other crate's ports; it
never publishes any of its own.

## Ports consumed

- `accounts::ports::DynAccountService` — passed into `transactions::init`
  so the transactions write path can call `get_balance` before
  reserving an idempotency row.
- `shared_kernel::events::EventSubscriber` — the in-process bus
  handle is built in `bootstrap.rs` and handed to
  `notifications::init`.

## Operational notes

- **`AppState` is intentionally minimal.** Fix #30 removed
  `circuit_breaker` and `backpressure` from it: their metrics are
  published eagerly from the middleware layer, so `/metrics` does
  not need a reference. Resist re-adding things "just in case" —
  every field here is a global handle and they are hard to remove
  once a module starts depending on the type's shape.
- **Distroless health probe.** `peakload-capstone --health-check`
  short-circuits `main` and opens a TCP connection to `localhost:3000`
  expecting `200 OK` from `/health`. Used by the Docker `HEALTHCHECK`
  directive in distroless images that do not ship `curl`.
- **Graceful shutdown.** `app.rs` owns the `CancellationToken`. Every
  long-running subsystem (replica health checks in `shared_kernel::db`,
  Sentinel monitor in `shared_kernel::cache`, rate-limiter sweeper,
  notifications dispatcher) receives a child token and is expected
  to exit promptly when it fires. If you add a new background task,
  thread the token through `init_infrastructure` rather than spawning
  with `tokio::spawn` directly.
- **Tracing exporter is `stdout` by default.** To send traces to a
  collector, swap `opentelemetry_stdout` for `opentelemetry-otlp` in
  [`bootstrap.rs::init_tracing`](./src/bootstrap.rs).
