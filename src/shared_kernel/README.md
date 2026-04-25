# `shared_kernel` — cross-cutting infrastructure

> Skeleton only. The real code still lives under `src/db/`,
> `src/cache/`, `src/middleware/`, `src/queue/` etc. until migration
> Phase 1 starts (see `../../docs/architecture/migration-plan.md`).

This directory holds **infrastructure, not business logic**. Every
module may import from here; nothing here may import from any
module. It is the leaf of the dependency graph — the one place in
the tree where `shared_kernel::*` can appear on the right side of
a `use` statement.

---

## 1. What belongs here

Planned contents, once migrated:

- `db/` — the shard router, read/write pool types, retry wrappers.
  (Current home: `src/db/`.)
- `cache/` — the Redis facade + Sentinel-aware client.
  (Current home: `src/cache/`.)
- `events/` — the cross-module event bus (RabbitMQ-backed today,
  abstraction hides the transport so a future in-process or
  pub-sub backend swap is a one-file change).
  (Current home: split across `src/queue/`.)
- `errors/` — shared `InfraError`, `ApiError`, and mapping helpers
  used by every module's error enums and by the api layer.
- `idempotency/` — the idempotency-key middleware + storage
  abstraction, currently in `src/middleware/`. Lives here rather
  than in a business module because EVERY module's api benefits
  from the same behaviour.
- `money/` — canonical `Money`, `Currency`, amount arithmetic.
  Used by both `accounts` and `transactions` without either
  owning the type.
- `ids/` — strongly-typed id newtypes (`AccountId`, `TransactionId`)
  if we find the modules want to share identifier validation.
- `observability/` — metrics labels, tracing helpers.
- `rate_limit/` — the token-bucket primitive. (Current home:
  `src/middleware/rate_limit.rs`; the middleware wrapper stays in
  `src/middleware/`, the primitive moves here so it is reusable
  inside modules for per-key throttling like notifications'
  per-recipient cap.)

## 2. What does NOT belong here

- **Any mention of a specific business concept** — no `Account`,
  no `Transaction`, no `Notification`. Those live inside their
  modules.
- **Any trait whose methods take module-specific DTOs.**
  `shared_kernel::events::Event` is a neutral envelope; the
  payloads it carries are owned by whoever publishes them.
- **HTTP handlers.** `src/middleware/` stays where it is — the
  handlers are axum-shaped and don't share code with modules.

## 3. Dependency rules recap

- `shared_kernel` imports from: standard library, external crates
  (sqlx, redis, lapin, axum, metrics, tracing, …).
- `shared_kernel` imports from: **nothing else in this repo**.
- Every module may import from `shared_kernel`. The api layer may.
  The bootstrap may.

See [../../docs/architecture/dependency-rules.md §2](../../docs/architecture/dependency-rules.md) for the full matrix.

## 4. Why "kernel" rather than "common"

"Common" is a bucket that fills with everything nobody knows where
to put — the fate of every `util.rs` in history. "Kernel" is load-
bearing infrastructure that the modules orbit. A piece of code
belongs here only if it is genuinely required by more than one
module; if you find yourself adding something here because it's
convenient rather than necessary, it probably belongs in a module
instead.
