# Module Anatomy — File-by-File Layout

Every module under `src/modules/` follows the **exact same internal
structure**. Uniformity is the point — anyone who has read one module
can navigate another without reorientation, and tooling (grep, rename,
migrations) works the same way everywhere.

Use `src/modules/_template/` as a copy-this starting point for any new
module.

---

## 1. Directory layout

```
src/modules/<name>/
  mod.rs                   — module entry; wires submodules together,
                             exposes nothing by default
  ports.rs                 — THE public contract: traits + data
                             types other modules may depend on
  domain/
    mod.rs                 — entities, value objects, domain errors;
                             pure Rust, no sqlx/redis/HTTP imports
    error.rs               — module-specific error type
  application/
    mod.rs                 — use-case orchestration; receives ports
                             of other modules by injection
  infrastructure/
    mod.rs                 — adapters: DB repo, external API clients
    repository.rs          — sqlx-backed impl of the domain Repository
                             trait declared in domain/
  api/
    mod.rs                 — axum sub-router this module owns
    handlers.rs            — HTTP handlers; translate HTTP ↔ ports
  README.md                — one page: what this module does, what
                             tables it owns, what ports it exposes
```

Each level has a single responsibility and a single direction of
dependency — always inward, never outward. The compiler enforces this
as long as you don't re-export module internals through `mod.rs`.

---

## 2. What goes in each file

### `mod.rs` (module root)

Declares the submodules and nothing else is public by default. The
only things this file exports to the rest of the crate are:

- `pub mod ports;`                 — the public contract
- `pub use api::router;`           — the axum sub-router constructor
- `pub use infrastructure::init;`  — the DI wiring used by bootstrap

Everything else is `pub(crate)` or tighter. This is the *sealing*
mechanism: internal types leak nowhere.

### `ports.rs`

The **only** file other modules (and tests) are allowed to import
from. Define traits for each externally-callable behaviour, plus the
data types those traits take and return.

Rules of thumb:

- Traits here must be object-safe (`dyn TraitName` works).
- Return types must be `Send + 'static` so they flow across async
  boundaries.
- Data types must be simple DTOs — no sqlx row types, no axum extracts.
  If a domain entity needs to leak out, define a public DTO in
  `ports.rs` and convert in `infrastructure/`.

Example shape:

```rust
use std::sync::Arc;
use async_trait::async_trait;

pub struct AccountId(pub String);
pub struct Balance { pub amount_cents: i64, pub currency: String }

#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("account not found: {0}")]
    NotFound(String),
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("infrastructure: {0}")]
    Infra(#[from] crate::shared_kernel::errors::InfraError),
}

#[async_trait]
pub trait AccountService: Send + Sync + 'static {
    async fn get_balance(&self, id: &AccountId) -> Result<Balance, AccountError>;
    async fn debit(&self, id: &AccountId, amount_cents: i64)
        -> Result<(), AccountError>;
}

pub type DynAccountService = Arc<dyn AccountService>;
```

### `domain/`

Pure business logic. No `sqlx`, no `redis`, no `axum`, no I/O at all.
If a type in here imports something from `infrastructure/` or `api/`,
that is a smell and a code-review blocker.

Contents typically:

- **Entities**: `Transaction`, `Account` — things with identity and a
  lifecycle.
- **Value objects**: `Money`, `Currency`, `AccountNumber` —
  immutable, validated on construction.
- **Domain services**: pure functions that span multiple entities
  (e.g. `can_transfer(from: &Account, amount: &Money) -> Result<…>`).
- **Repository traits**: defined here, implemented in
  `infrastructure/`. This is the dependency inversion that lets
  domain stay pure.
- **Domain errors**: `error.rs` with the module's error enum.

A reader who only wants to understand "what this module does" should
be able to read `domain/` front-to-back and never see a SQL query or
an HTTP header.

### `application/`

Use-case orchestration. Each file is typically one use case
(`transfer_money.rs`, `credit_account.rs`) exposing one struct/fn.

Application services:

- Take their dependencies by constructor injection (the traits
  defined in `domain/` or in another module's `ports.rs`).
- Orchestrate the flow: fetch, validate, mutate, persist, emit event.
- Return port-shaped DTOs, not domain entities, to the caller (api
  handlers should not need to know about domain internals).

This layer is the most testable part of a module — every dependency
is a trait and every test substitutes a fake.

### `infrastructure/`

Adapters that connect the module to the outside world:

- `repository.rs` — sqlx-backed implementation of the
  `domain::Repository` trait. Shards / pool selection resolved via
  `shared_kernel::db`.
- `cache.rs` — redis-backed reads, if relevant.
- `external/` — HTTP clients for third-party APIs.
- `events.rs` — RabbitMQ publisher bound to the module's outbound
  events.
- `init.rs` — a public `pub fn init(state: &SharedState) -> DynXxxService`
  function that wires concrete impls together and returns the port
  type, for the top-level bootstrap to consume.

Infrastructure is the only place SQL lives, the only place Redis keys
are named, and the only place HTTP clients are configured. Swapping a
backend (e.g. Postgres → something else) is confined to this
directory.

### `api/`

HTTP layer, axum-based.

- `handlers.rs` — one handler per endpoint. Each handler:
  1. Extracts the request.
  2. Calls a port trait (usually `application::UseCase`).
  3. Maps the result to an HTTP response or error.
  The entire handler body should be shorter than its docstring.
- `mod.rs` — exports `pub fn router(deps: ModuleDeps) -> axum::Router`
  that the top-level bootstrap mounts under the module's prefix.
- `dto.rs` (optional) — request / response types with serde
  derivations, kept separate from `ports.rs` DTOs if HTTP shape
  diverges from the internal port shape.

The api layer should contain **no business logic**. If a handler
conditionally decides to call repo-A vs repo-B, that decision belongs
in `application/`, not here.

### `README.md`

One page. Target length: fits on a laptop screen without scrolling.
Sections:

1. **What this module is for** — one paragraph.
2. **Tables owned** — e.g. `transactions`, `transactions_events`.
3. **Ports exposed** — `AccountService`, list the trait methods.
4. **Ports consumed** — e.g. "depends on `accounts::ports::AccountService`".
5. **Events published** — shared_kernel event names, payloads.
6. **Events consumed** — same.
7. **Operational notes** — anything special about migrations,
   backfills, or rollbacks for this module.

The README is the first thing someone reads when they're told "go
change something in transactions". Keep it current.

---

## 3. Visibility reference

| Symbol location                      | Visibility               | Who can import? |
|--------------------------------------|--------------------------|-----------------|
| `modules/<name>/ports.rs`            | `pub`                    | Anyone          |
| `modules/<name>/domain/...`          | `pub(crate)` or tighter  | Same module only |
| `modules/<name>/application/...`     | `pub(crate)` or tighter  | Same module only |
| `modules/<name>/infrastructure/...`  | `pub(crate)` or tighter  | Same module only |
| `modules/<name>/api/handlers.rs`     | `pub(crate)` or tighter  | Same module only |
| `modules/<name>/api::router`         | `pub`                    | Bootstrap       |
| `modules/<name>/infrastructure::init`| `pub`                    | Bootstrap       |

Anything else marked `pub` from a module is a bug.

---

## 4. What NOT to do

- **Don't put DB queries in `application/`.** That's infrastructure.
  Application calls a repo trait.
- **Don't put HTTP types in `domain/`.** axum extracts are ports at
  the api boundary, not domain concepts.
- **Don't let `domain/` depend on another module's `ports.rs`.**
  That's application-level orchestration.
- **Don't skip `ports.rs` by making internals `pub` "just for tests".**
  Use `#[cfg(test)] pub(crate)` if a test inside the same module
  needs deeper access; never broaden visibility for a test in another
  module.
- **Don't re-export infrastructure types through ports.** If a
  consumer sees `sqlx::PgRow` leak out of a port, the seal is broken.

---

## 5. Why this much ceremony

Because the biggest predictor of whether a module can later become an
independent service is whether, today, its neighbours can only reach
it through the door we expect them to. Every rule above exists to
keep that door the only door.

A well-shaped module is one where, when we decide to lift it into its
own crate or its own service, the change is:

1. Move the directory.
2. Rewrite `infrastructure/init.rs` to construct the client instead
   of the local impl.
3. Rewrite the tests that substituted the local impl to substitute the
   client.

Nothing else. If the move is harder than that, the module boundary
was wrong — go back and fix it before cutting the service.
