# Phase 1 Walkthrough — The `accounts` Module

> **Audience:** anyone opening this codebase for the first time — an
> intern, a new hire, a reviewer who last saw the monolith layout.
> This document is the **one you should read first** if you want to
> understand what the `src/modules/accounts/` directory is doing,
> why each file exists, and how the request flows through it.
>
> **Prerequisite reading** (optional, but referenced here):
> - [modular-monolith.md](./modular-monolith.md) — why modules at all.
> - [module-anatomy.md](./module-anatomy.md) — file-by-file rules.
> - [dependency-rules.md](./dependency-rules.md) — what imports what.
> - [migration-plan.md](./migration-plan.md) — where this phase fits.

---

## 1. What you are looking at

Peakload's original codebase was a **flat monolith**: one big
[src/api/handlers.rs](../../src/api/handlers.rs) file talked
directly to shared DB and cache handles, for every business concern
mixed together — transactions, accounts, idempotency, everything.

The `accounts` module in `src/modules/accounts/` is the **first
bounded-context extraction**. It carves the account-balance code
out of the monolith and wraps it in the standard module shape
(domain / application / infrastructure / api) so that:

1. Account-related changes touch only this folder.
2. Other modules (e.g. `transactions`, once extracted) will
   interact with accounts through one published trait, never
   by reading the `users` table directly.
3. When it's time to lift accounts into its own service, we
   move one directory rather than surgically disentangling code.

**Important:** this is "Phase 1 **partial**" (see
[migration-plan.md §Phase 1](./migration-plan.md)). That means:

- The **new** `/api/v2/accounts/{account_number}/balance`
  endpoint lives alongside the original
  `/api/v1/users/{account_number}/balance`.
- Both endpoints hit the same `users` table on the same shards
  and return byte-identical JSON.
- Nothing in the old flat code has been deleted.

When a future session is confident the new path is good, the
"cutover" task is a one-line router change (drop the v1 route,
point `/api/v1` at the module if needed for URL stability) plus
deletion of the legacy handler.

---

## 2. Request flow — where your GET actually goes

Let's trace a real request: `GET /api/v2/accounts/acct-123/balance`.

```
  HTTP request (port 8080, via nginx)
           │
           ▼
  axum Router            (src/bootstrap.rs — `build_router`)
           │
           │ .nest_service("/api/v2/accounts", accounts_router)
           ▼
  accounts::api::router  (src/modules/accounts/api/mod.rs)
           │
           │ .route("/{account_number}/balance", get(handlers::get_balance))
           ▼
  accounts::api::handlers::get_balance
           │                             (src/modules/accounts/api/handlers.rs)
           │
           │ 1. validate_account_number("acct-123")   — HTTP-shape check
           │ 2. deps.cache.get(...)                   — Redis cache peek
           │ 3. deps.service.get_balance(&AccountId(...)) ──┐
           │                                                 │
           │                                                 ▼
           │                              application::GetBalanceService
           │                                                 │  (src/modules/accounts/application/mod.rs)
           │                                                 │
           │                                                 │ delegates to
           │                                                 ▼
           │                              domain::AccountRepository (trait)
           │                                                 │  (declared in src/modules/accounts/domain/mod.rs)
           │                                                 │
           │                                                 │ actually runs
           │                                                 ▼
           │                              infrastructure::SqlxAccountRepository
           │                                                 │  (src/modules/accounts/infrastructure/repository.rs)
           │                                                 │
           │                                                 │ SELECT ... FROM users
           │                                                 │ via ShardRouter.reader(shard)
           │                                                 │ wrapped in retry_transient
           │                                                 ▼
           │                                         PostgreSQL
           │                                                 │
           │ ◄───────────────────────────────────────────────┘ UsersRow → Account
           │                                                    → Balance (port DTO)
           │
           │ 4. Balance → BalanceResponse (HTTP DTO)
           │ 5. deps.cache.set(...)                   — write-through (30 s TTL)
           │ 6. ApiResponse::success(response)        — wrap in standard envelope
           ▼
  axum Response → HTTP/1.1 200 OK with JSON
```

The **direction of dependencies** always points inward toward pure
domain code: the HTTP handler depends on the port, the port depends
on the domain, and the infrastructure implements the domain's
repository trait from the outside. This is what keeps `domain/`
free of `sqlx`, `axum`, and `redis` types — the rule that makes
modules independently testable.

---

## 3. The files, in the order you should read them

The layout inside `src/modules/accounts/` is deliberately the same
as every other module. If you learn it once you can navigate the
others blind.

### 3.1 `ports.rs` — THE public contract

> **Read this first.** Everything a caller needs to know about
> `accounts` is in one ~100-line file.

Defines:

- **DTOs**: `AccountId`, `AccountStatus`, `Balance`. These are plain
  Rust types with `Serialize`/`Deserialize` derives, nothing fancy.
- **Error type**: `AccountError` with three variants —
  `NotFound`, `Validation`, `Infra`. All operational failures are
  collapsed into `Infra(String)` so other modules don't need to
  import `sqlx::Error` or `redis::RedisError`.
- **Service trait**: `AccountService` with one async method
  `get_balance`. The trait is **object-safe** (no `Self`-returning
  methods) and declared `Send + Sync + 'static` so it flows across
  async task boundaries.
- **Alias**: `DynAccountService = Arc<dyn AccountService>`. Always
  use this alias in constructor signatures — it's the shape
  Phase 2's `transactions` module will inject.

**The Rule**: a file outside `src/modules/accounts/` may import
from `accounts::ports` and from nowhere else inside this tree.
Breaking that rule is what rots a modular monolith back into a
tangled one.

### 3.2 `domain/mod.rs` — pure business concepts

- **`Account`** struct: the domain's richer view of an account.
  Has `full_name`, `email` and other fields that the port DTO
  does not expose.
- **`DomainError`** enum: module-private error type. Maps to the
  port's `AccountError` through a `From` impl.
- **`AccountRepository`** trait: the port the infrastructure must
  satisfy. Declared **inside `domain/`**, not `infrastructure/`,
  because domain defines what it needs and infra implements it —
  classic dependency inversion. This is why `domain/` has no
  `sqlx` imports: the trait returns `Result<Option<Account>,
  String>` with the error as an opaque string.

Rules enforced here by convention:

- **No I/O crates.** Run `rg '^use' src/modules/accounts/domain/`
  and confirm you see no `sqlx`, `redis`, `axum`, `reqwest`,
  `tokio::net`, or `tokio::fs`.
- **No other-module imports.** A cross-module need belongs in
  `application/`, not here.

### 3.3 `application/mod.rs` — use-case orchestration

Right now contains exactly one type: `GetBalanceService`.

- Takes an `Arc<dyn AccountRepository>` in its constructor —
  everything it needs comes in as a trait object.
- `impl AccountService for GetBalanceService` — this is where the
  public port trait is satisfied. Notice the function body is
  almost boring: validate, call repo, map errors. That's the
  point — keep orchestration readable.
- Holds the `From<DomainError> for AccountError` impl. It lives
  here rather than in `ports.rs` because `DomainError` is
  module-private; if it were defined in `ports.rs` it would have
  to be `pub`, which leaks domain internals.

**Why one use case per struct?** When Phase 2 adds `credit`,
`debit`, `create_account`, each becomes its own file
(`credit.rs`, `debit.rs`, etc.) with its own fake-able injection
point. Tests substitute fakes at the trait boundary without any
of them interfering.

### 3.4 `infrastructure/repository.rs` — the only SQL in this module

- `UsersRow`: a `#[derive(FromRow)]` struct that projects only
  the columns we need. Adding a column here is harmless; removing
  one is a migration.
- `SqlxAccountRepository`: a plain struct that holds a
  `ShardRouter` clone. The router itself is cheap to clone (it
  wraps `Arc`s internally), so no `Arc<ShardRouter>` wrapping.
- `impl AccountRepository for SqlxAccountRepository`:
  - Uses the same `ShardRouter::shard_for(account_number)` that
    the legacy handler uses → identical shard routing.
  - Uses the same `retry_transient(...)` wrapper → identical
    retry behaviour on transient errors (notably during a
    Patroni promotion; see
    [ha-architecture.md](../ha-architecture.md) §2).
  - Maps `sqlx::Error` to `String` at the trait boundary so the
    domain never sees an infra-specific type.

**Why `retry_transient` inside the repo and not in the use case?**
Because "how we survive a promotion" is an infrastructure
concern, not a business rule. If we later swap sqlx for
something else, the application layer doesn't care.

### 3.5 `infrastructure/mod.rs` — the wiring entry point

- `AccountsDeps { service, cache }`: the bundle the bootstrap
  hands to the router. Holds one `DynAccountService` (the thing
  `ports.rs` advertised) plus a concrete `RedisCache` handle
  because caching the **HTTP response body** is an api-layer
  concern that doesn't need a bespoke trait abstraction.
- `pub fn init(shards, cache) -> AccountsDeps`: called once at
  startup by `bootstrap::build_router`. This is the single
  public wiring entry point for the whole module; everything
  else is `pub(crate)` or tighter.

**Why is `RedisCache` not hidden behind a trait?** Because the
`shared_kernel::cache` facade (future home) is itself the
abstraction — every module sees the same `RedisCache` type, and
swapping it is a shared_kernel change that rebuilds everything.
Not worth a per-module trait.

### 3.6 `api/mod.rs` and `api/handlers.rs` — HTTP gateway

- `api/mod.rs` exposes one public function: `router(deps)`. The
  bootstrap mounts its result under `/api/v2/accounts`. Everything
  else in this directory is `pub(crate)`.
- `api/handlers.rs` contains:
  - The `get_balance` handler — trivial extract → port call →
    map result shape.
  - `BalanceResponse` (the JSON shape) kept **byte-identical**
    to the legacy handler's response so integration tests can
    diff v1 and v2.
  - `From<AccountError> for AppError` — the bridge between the
    module's port error type and the existing app-wide error
    framework. This bridge only exists because Phase 1 reuses
    `AppError`/`AppResult`; Phase 4 (crate split) will replace
    it with the module's own `IntoResponse` impl.

**Why does the response shape preserve the wire contract
exactly?** So the cutover is a single router change — not an
API versioning exercise.

### 3.7 `mod.rs` — the module seal

```rust
pub mod ports;
pub(crate) mod domain;
pub(crate) mod application;
pub(crate) mod infrastructure;
pub(crate) mod api;

pub use api::router;
pub use infrastructure::init;
```

Four of the five submodules are `pub(crate)`. Only `ports` is
fully `pub`. The two public re-exports (`router`, `init`) are
the bootstrap's handles.

**This file is the compiler-enforced half of the rule in §3.1.**
Every "I need to import accounts::domain::Account from somewhere
else" attempt will fail to compile, and that failure is the seal
working as designed.

---

## 4. Where this hooks into the rest of the app

Two changes outside the module, deliberately small:

### 4.1 `src/main.rs`

```rust
mod modules;
```

Adds one line. The `modules` module then re-declares `accounts`
(and, later, its siblings).

### 4.2 `src/bootstrap.rs` — inside `build_router`

```rust
let accounts_deps = crate::modules::accounts::init(
    state.shard_router.clone(),
    state.cache.clone(),
);
let accounts_router = crate::modules::accounts::router(accounts_deps);

Router::new()
    .nest("/api/v1", api_routes)                         // legacy
    .nest_service("/api/v2/accounts", accounts_router)   // NEW
    .route("/health", get(...))
    .route("/metrics", get(...))
    .with_state(state)
```

Two lines of wiring, one new `.nest_service` call. That's it.

**Why `nest_service` and not `nest`?** Because
`accounts::router` applies its own state (`AccountsDeps`) and thus
returns a `Router<()>`, while the parent router carries
`AppState`. `nest_service` accepts any `Service`, which bridges
the state-type mismatch without forcing `AccountsDeps` to
implement `FromRef<AppState>`. The comment in `bootstrap.rs`
explains this right next to the code.

---

## 5. What we deliberately left for later

Phase 1 **partial** means we took the shortest path that proves
the shape. These gaps are tracked in
[migration-plan.md §Phase 1 exit criteria](./migration-plan.md):

1. **No middleware parity.** The v2 route skips auth, rate
   limiting, circuit breaker, and backpressure middleware. Fine
   for a proof-of-shape; MUST be fixed before v1 is removed.
2. **Legacy handler not deleted.** `src/api/handlers::get_balance`
   still exists and still serves `/api/v1/users/.../balance`.
   This is intentional — it lets us diff production behaviour
   between the two paths.
3. **No new tests.** The module shape is verified by `cargo
   check`; adding a real sqlx-backed integration test (spinning
   up a Postgres testcontainer and hitting `/api/v2/accounts/...`)
   is a follow-up.
4. **Still one crate.** Phase 4 of the migration splits modules
   into separate crates so the compiler enforces the dependency
   rules. Phase 1 relies on convention + `pub(crate)` visibility,
   which works well enough when the team is small.
5. **No events published yet.** The README for this module lists
   `AccountBalanceChanged` and `AccountStatusChanged` as
   planned events — they appear once `credit`/`debit`/
   `set_status` use cases land in Phase 2.

If you are picking up the migration: start with one of those
gaps, pick the smallest (probably #3, the integration test), and
get a second proof of the pattern before tackling the bigger
items.

---

## 6. How to add a second module (preview)

The shape you see under `accounts/` is the shape you copy:

```bash
cp -r src/modules/_template src/modules/transactions
```

Then:

1. Rewrite `transactions/README.md` (what tables, what ports,
   what events).
2. Define public types in `transactions/ports.rs`.
3. Fill in `domain/` with pure business types.
4. Add use cases in `application/`, injecting both the module's
   own repo trait AND `Arc<dyn AccountService>` if cross-module
   coordination is needed.
5. Implement the repo in `infrastructure/`, wire with `init()`.
6. Add HTTP handlers in `api/`, export the `router(deps)`.
7. Declare the module in `src/modules/mod.rs`.
8. Add the two lines of bootstrap wiring in
   `src/bootstrap.rs`.
9. `cargo check`, then write the integration test.

The per-phase time estimate is in
[migration-plan.md](./migration-plan.md) — but more importantly,
nothing above is surprising if you read `accounts/` first.

---

## 7. Common questions

**Q: Why is there a separate `domain/` at all if the logic is so
simple?**
Because the moment you add a second use case (say, `credit`), the
domain layer becomes the place where multi-entity invariants
live ("you cannot debit below zero"). Starting with an empty
`domain/` directory is cheaper than introducing one later.

**Q: Can I just add a `pub` to something in `domain/` and use it
elsewhere?**
No. The moment you do that, the seal breaks and the module
becomes coupled to the outside. The compiler errors you hit
when trying to cross the boundary are the design working. The
question to ask is "should this be in `ports.rs`?" — if yes,
promote it; if no, the caller has no business reaching in.

**Q: Why is the module's error type an enum with `Infra(String)`
instead of `Infra(sqlx::Error)`?**
To keep `ports.rs` free of infrastructure crate imports. Other
modules can import `AccountError` without also importing sqlx.
When a richer root cause is needed for logging, the sqlx error
is logged from inside `infrastructure/repository.rs` before it
gets flattened.

**Q: Where does metrics instrumentation go?**
For now, in `api/handlers.rs` alongside the cache-hit counters,
same as the legacy handler. When we formalise
`shared_kernel::observability` in a later phase we will hoist
the "per-port call" metrics into a generic layer.

**Q: Can two modules share a table?**
No. If they both need to write to `users`, then one of them is
the owner and the other calls through the owner's
`ports.rs`. See [dependency-rules.md §Rule 1](./dependency-rules.md).

**Q: How do I know which module should own a table?**
The one whose business concern the table's data serves. If both
modules share a concern, they are one module — merge them.
