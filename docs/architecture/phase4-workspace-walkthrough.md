# Phase 4 Walkthrough — Workspace Crate Split

> **Audience:** anyone who already read
> [phase1-accounts-walkthrough.md](./phase1-accounts-walkthrough.md),
> [phase2-transactions-walkthrough.md](./phase2-transactions-walkthrough.md),
> and [phase3-notifications-walkthrough.md](./phase3-notifications-walkthrough.md).
> Phase 4 is mostly Cargo plumbing — but the consequences are
> structural. After Phase 4, the **compiler** enforces the
> module boundaries; the previous phases relied on convention +
> `pub(crate)` + grep.

---

## 1. What changed

Before Phase 4: one Cargo crate (`peakload-capstone`) with the
entire codebase under `src/`. Module boundaries enforced by
convention.

After Phase 4: a Cargo workspace with 5 crates. Module
boundaries enforced by `Cargo.toml` dependency lists.

```
Capstone/
├── Cargo.toml                 ← workspace root: members + workspace.dependencies
├── crates/
│   ├── app/                   ← binary (peakload-capstone)
│   ├── shared_kernel/         ← cross-cutting infra
│   ├── accounts/              ← leaf module
│   ├── transactions/          ← depends on accounts via ports
│   └── notifications/         ← depends only on shared_kernel
├── docs/
│   └── architecture/
│       └── module-template/   ← formerly src/modules/_template/
└── tests/                     ← gone; integration tests live in crates/app/tests/
```

## 2. Dependency graph (now compiler-enforced)

```
                   ┌───────────────────────────────┐
                   │            app                │   binary
                   │  (main, bootstrap, config,    │   only crate that
                   │   middleware, legacy v1)      │   knows everything
                   │                               │
                   └─┬───────┬───────┬───────┬─────┘
                     │       │       │       │
                     ▼       ▼       ▼       ▼
        ┌──────────┐ ┌────────────┐ ┌────────────────┐ ┌──────────────┐
        │ accounts │ │transactions│ │ notifications  │ │ shared_kernel│
        └────┬─────┘ └──────┬─────┘ └────────┬───────┘ └──────┬───────┘
             │              │                │                │
             └──────────────┼────────────────┼────────────────┘
                            │                │
                            ▼                ▼
                     ┌────────────────────────────┐
                     │ shared_kernel              │ infra:
                     │   events / db / cache /    │ events bus
                     │   queue::producer / error /│ DB + cache + queue
                     │   responses                │ error + ApiResponse
                     └────────────────────────────┘
```

Read the arrows as Cargo dependencies. Note:

- **No arrow from `accounts` to `transactions`** — the leaf
  cannot reach into the consumer of its ports.
- **No arrow from `notifications` to `accounts` or
  `transactions`** — notifications is purely event-driven.
- **No arrow from `shared_kernel` to anything** — kernel
  neutrality is a `Cargo.toml` fact now, not a grep.

Try it: add `use transactions::ports::*;` to
`crates/accounts/src/api/handlers.rs` and run
`cargo check -p accounts`. You get
`error[E0432]: unresolved import 'transactions'` — exactly
what we want. Phase 4 is "the compiler caught up to the
design".

## 3. The kernel-local config slices

Phases 1–3 had a hidden coupling: `ShardRouter::new`,
`RedisCache::new`, and `QueueProducer::new` all took a
`&Config` reference, and `Config` was the binary's env-var
loader. Moving the kernel into its own crate without breaking
that coupling would mean dragging `Config` into the kernel —
which would mean dragging `dotenvy`, `env::var`, and the rest
of the binary's plumbing.

Phase 4 cut the knot by introducing kernel-local config
slices:

- [`shared_kernel::db::shard::ShardRouterConfig`](../../crates/shared_kernel/src/db/shard.rs) +
  `ShardUrls`
- [`shared_kernel::cache::redis::RedisCacheConfig`](../../crates/shared_kernel/src/cache/redis.rs)
- `QueueProducer::new(amqp_url: &str)` — single param, no
  struct needed.

The `app` crate's bootstrap still owns `Config`; it just
constructs each kernel slice inline. See
[`crates/app/src/bootstrap.rs::init_infrastructure`](../../crates/app/src/bootstrap.rs)
for the conversion.

## 4. Per-crate Cargo.toml shape

Workspace root `Cargo.toml` does **two** things:

1. Lists the members.
2. Pins every shared dependency version in
   `[workspace.dependencies]`.

Per-crate `Cargo.toml` files reference those by
`<dep>.workspace = true`. Bumping `tokio` is a one-line
change at the root; all five crates pick it up.

The dependency lists below are the truth — review them when
you suspect a forbidden cross-crate use:

| crate           | depends on                                              |
|-----------------|---------------------------------------------------------|
| `shared_kernel` | (no internal deps)                                      |
| `accounts`      | `shared_kernel`                                         |
| `transactions`  | `shared_kernel`, `accounts`                             |
| `notifications` | `shared_kernel`                                         |
| `app`           | `shared_kernel`, `accounts`, `transactions`, `notifications` |

Anything not in that table is a forbidden import and the
compiler will say so.

## 5. What lives where now (the file-by-file map)

### `crates/shared_kernel/src/`

| Path                 | What it is                                              |
|----------------------|---------------------------------------------------------|
| `lib.rs`             | Module declarations + dependency-rule docstring         |
| `events.rs`          | `Event`, `EventPublisher`, `EventSubscriber`, `InProcessEventBus` |
| `db/mod.rs`          | `failover` + `pool` + `shard` declarations              |
| `db/shard.rs`        | `ShardRouter` + new `ShardRouterConfig` / `ShardUrls`   |
| `db/failover.rs`     | `retry_transient` retry helper                          |
| `db/pool.rs`         | `DatabasePool` (per-shard read/write split)             |
| `db/shard_tests.rs`  | Unit + property tests for the hash routing              |
| `cache/redis.rs`     | `RedisCache` + `MasterPoolHandle` + `RedisCacheConfig`  |
| `queue/producer.rs`  | `QueueProducer` + `parse_amqp_url` (now `pub`)          |
| `error.rs`           | `AppError` + `IntoResponse` impl + `AppResult`          |
| `responses.rs`       | `ApiResponse<T>` + `HealthResponse` + shard health DTOs |

### `crates/accounts/src/`

The Phase 1 layout, untouched. Imports updated:
`crate::db::shard::ShardRouter` →
`shared_kernel::db::shard::ShardRouter`. Same for cache,
error, responses.

### `crates/transactions/src/`

The Phase 2 layout, untouched. The cross-module dep is now a
genuine Cargo dep:

```toml
# crates/transactions/Cargo.toml
[dependencies]
accounts = { path = "../accounts" }
```

Imports use the crate name directly:
`use accounts::ports::{AccountError, AccountId, DynAccountService};`.

### `crates/notifications/src/`

The Phase 3 layout, untouched. No `accounts` or `transactions`
in `Cargo.toml` — the event bus is the only path in.

### `crates/app/src/`

Everything that did not belong in a module crate:

| Path                   | What stays + why                                         |
|------------------------|----------------------------------------------------------|
| `main.rs`              | Entry point + global allocator + `AppState`              |
| `app.rs`               | `App::new` / `App::run` (lifecycle); calls `transactions::start_consumer` |
| `bootstrap.rs`         | Tracing / metrics / infra / router build, the wiring     |
| `config.rs`            | Env-var loader; binary-only                              |
| `middleware/*`         | Auth, rate-limit, circuit-breaker, backpressure, request-id, metrics — all consumed only by `bootstrap.rs` |
| `api/handlers.rs`      | **Legacy v1 handlers** — slated for deletion at v1 cull  |
| `api/mod.rs`           | Declares `handlers` only; `responses` moved to kernel     |
| `db/models.rs`         | **Legacy DTOs** — only the v1 handlers still consume these; the queue consumer no longer does. |
| `db/mod.rs`            | Declares `models` only; everything else moved to kernel   |

Two of those entries are flagged "legacy". Those are the items
Step B clears in the v1 cull. The Step-A consumer rewire (Phase
2 follow-up) is **done on this branch** — the `queue/` directory
is gone and the consumer lives in
[`crates/transactions/src/infrastructure/consumer.rs`](../../crates/transactions/src/infrastructure/consumer.rs).
See [`cutover-readiness.md`](./cutover-readiness.md) for the
remaining cull gates and
[`v1-caller-inventory.md`](./v1-caller-inventory.md) for the
caller catalogue Step B operates on.

## 6. Compile-time invariants you can prove

Run these greps at any time. They should each return zero
matches; if one starts returning matches, a PR has broken the
seal:

```bash
# 1. shared_kernel imports nothing from any module:
rg 'use accounts|use transactions|use notifications' crates/shared_kernel
#   → no matches.

# 2. accounts imports nothing from sibling modules:
rg 'use transactions|use notifications' crates/accounts
#   → no matches.

# 3. transactions sees accounts ONLY through ports:
rg 'use accounts::' crates/transactions
#   → only `accounts::ports::*` lines.

# 4. notifications has zero module-level deps:
rg 'use accounts|use transactions' crates/notifications
#   → no matches.
```

Even better: those greps are now belt-and-suspenders. Without
adding the dependency to `Cargo.toml`, the compiler refuses
the `use` statement before grep ever runs. The greps are for
catching `Cargo.toml` rot (someone adding a dep "just to make
it compile").

## 7. CI / build performance

`cargo build -p <crate>` only rebuilds that crate's tree. So:

- A change in `accounts::infrastructure::repository` rebuilds
  `accounts`, `transactions` (downstream), and `app`. Not
  `shared_kernel`, not `notifications`.
- A change in `notifications::application` rebuilds
  `notifications` + `app`. Nothing else.
- A change in `shared_kernel::db::shard` rebuilds everything.

CI should run per-crate test jobs in parallel:

```yaml
- cargo test -p shared_kernel
- cargo test -p accounts        ┐
- cargo test -p transactions    ├ all parallel
- cargo test -p notifications   ┘
- cargo test -p app             ← integration tests + bin
```

The local cold build is `~44s` end-to-end; per-crate cold
builds for the leaf modules are sub-15s.

## 8. What was NOT done in Phase 4

Each of these is a separate, smaller PR in the
[cutover-readiness checklist](./cutover-readiness.md):

1. **Delete legacy v1 handlers** + the `/api/v1/*` route. See the
   pre-cull caller catalogue in
   [`v1-caller-inventory.md`](./v1-caller-inventory.md).
2. ~~**Rewire the queue consumer** into~~
   ~~`transactions::infrastructure::consumer`.~~
   ✅ **Done** (Step A on this branch). The consumer now lives at
   [`crates/transactions/src/infrastructure/consumer.rs`](../../crates/transactions/src/infrastructure/consumer.rs)
   and the bootstrap calls `transactions::start_consumer(...)`.
3. **Drop `db/models.rs`** — the legacy DTOs lost their consumer
   half in Step A, so Step B's only remaining caller is the v1
   HTTP handlers; the file goes away when those go away.
4. **Wire `db_write_retry_backoff_ms`** into the
   `retry_transient` call sites — currently parsed from env
   but unused. Pre-existing latent gap, surfaced as a
   `dead_code` warning during the workspace split.

After those four, the binary contains only:

- composition root (`main.rs`, `app.rs`, `bootstrap.rs`)
- env config (`config.rs`)
- HTTP middleware
- the `/health` + `/metrics` standalone routes

Everything business-domain lives in a module crate. Phase 5
(microservice extraction) becomes mechanical at that point.

## 9. Phase 5 setup — what Phase 4 gives you for free

To pull `notifications` out into its own service tomorrow:

1. New binary crate `crates/notifications-service/` whose
   `main.rs` is the bootstrap for that service.
2. Its `Cargo.toml` depends on `notifications` + a small
   `app-style` infrastructure layer (or its own).
3. The existing in-process `EventBus` is replaced with the
   AMQP-backed impl that satisfies the same `EventPublisher`
   / `EventSubscriber` traits.
4. The remaining monolith stops including
   `notifications` in its `Cargo.toml`. Done.

Because the dependency seal is real today, none of those
steps requires touching the module's Rust code.
