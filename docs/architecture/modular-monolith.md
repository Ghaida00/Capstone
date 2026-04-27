# Modular Monolith — Core Design

> **Status (post-Phase 4 + Step A):** the design described here is the
> running shape of the codebase. Phases 1–4 are merged plus the
> Phase-2 follow-up consumer rewire (Step A in
> [cutover-readiness.md](./cutover-readiness.md#2-gates-for-step-a-consumer-rewire-done-on-this-branch)).
> The legacy v1 HTTP surface is the only remaining piece in
> `crates/app/`; Step B (the v1 cull) clears it. Pre-cull caller
> catalogue: [`v1-caller-inventory.md`](./v1-caller-inventory.md).
>
> **Path mapping**: this document still uses the pre-Phase-4 paths
> (`src/modules/<name>/`, `src/shared_kernel/`, `src/api/`, etc.)
> because they are easier to read narratively. After Phase 4 those
> are now:
>
> | Old path                  | New path                                                       |
> |---------------------------|----------------------------------------------------------------|
> | `src/modules/<name>/`     | `crates/<name>/src/`                                           |
> | `src/shared_kernel/`      | `crates/shared_kernel/src/`                                    |
> | `src/api/`                | `crates/app/src/api/` (legacy v1 only)                         |
> | `src/db/` (shard, etc.)   | `crates/shared_kernel/src/db/`                                 |
> | `src/db/models.rs`        | `crates/app/src/db/models.rs` (legacy)                         |
> | `src/cache/`              | `crates/shared_kernel/src/cache/`                              |
> | `src/queue/producer.rs`   | `crates/shared_kernel/src/queue/producer.rs`                   |
> | `src/queue/consumer.rs`   | `crates/transactions/src/infrastructure/consumer.rs` (Step A)  |
> | `src/middleware/`         | `crates/app/src/middleware/`                                   |
> | `src/error.rs`            | `crates/shared_kernel/src/error.rs`                            |
> | `src/api/responses.rs`    | `crates/shared_kernel/src/responses.rs`                        |
>
> See [phase4-workspace-walkthrough.md](./phase4-workspace-walkthrough.md)
> for the file-by-file tour.

---

## 1. Why modular monolith (and why not microservices yet)

The current codebase is a **single-process Rust binary** served behind
Nginx across two replicas. That is a monolith — fine for today's load
but it leaks across business concerns: `src/api/handlers.rs` talks
directly to `src/db/pool.rs` which queries any table in any shard,
and there is no compile-time fence between, say, *transactions* and
*accounts* logic. Any change can touch any part of any query.

The endgame we care about is a set of **small independently-deployable
services** communicating over RabbitMQ and (occasionally) HTTP. Going
there in one jump is expensive and risky. The proven cheaper path is:

```
   monolith ──► modular monolith ──► microservices
   (today)     (this groundwork)    (future, per-module-as-needed)
```

The **modular monolith** step gives us:

- **Bounded contexts** at the source level: each business domain lives
  in its own module, owns its own tables, and exposes a narrow public
  interface.
- **Independent testability**: module B can be tested with module A
  stubbed out, and vice versa.
- **Compile-time isolation**: a careless edit in `transactions` cannot
  accidentally read from `accounts` internals — the compiler rejects
  it.
- **Cheap microservices extraction later**: when a module earns its
  own service, we lift the folder into a new crate / binary, swap its
  transport from in-process function calls to HTTP/AMQP, and keep the
  rest of the monolith running unchanged.

We are deliberately NOT building microservices now. Our load is
modest, our ops budget is small, and every extra service pays in
network round-trips, deployment complexity, and distributed-systems
debugging. Modular monolith first; extract when the data proves the
split is worth it.

---

## 2. The dependency contract

Three modules are planned, chosen to stress-test the rules by having
an explicit dependency relationship:

- **`accounts`** — user accounts, balances, status. **Leaf**: depends
  on nothing else module-wise.
- **`transactions`** — money movement. **Depends on `accounts::ports`**
  to debit / credit / read balance.
- **`notifications`** — sends out-of-band alerts for interesting
  events. **Independent of both**: it subscribes to events via
  `shared_kernel::events`, it does not call into either module.

The user-stated invariants translate directly:

> "Changes to A do not affect B and C."  
> `transactions` (A) internals have no readers. Changing its
> implementation never forces a rebuild of `accounts` or
> `notifications`.

> "Changes to B affect only A, not C."  
> `accounts` (B) is only imported by `transactions` (A). Changing
> its internals forces an A rebuild but never a `notifications` (C)
> rebuild. Changing its **public ports** (the trait definitions)
> forces both A and the module's own internals to recompile, but
> still not C.

Enforced as a directed acyclic graph:

```
             ┌────────────────┐
             │  notifications │  ◄── C (independent)
             └─────┬──────────┘
                   │
                   │ consumes events
                   ▼
             ┌────────────────┐
             │ shared_kernel  │  ◄── infra only: db, cache, events
             └────────▲───────┘
                      │
            ┌─────────┴─────────┐
            │                   │
   ┌────────┴───────┐   ┌───────┴────────┐
   │  transactions  │──►│   accounts     │  (via accounts::ports)
   │       (A)      │   │      (B)       │
   └────────────────┘   └────────────────┘
```

Edges go toward what a module depends on. `transactions → accounts`
means "transactions uses accounts". `notifications` has no edge to
either business module — it only sees events.

---

## 3. The rules

These are enforced by convention + code review for now; Phase 4 of
the migration (see [migration-plan.md](./migration-plan.md) §5)
hoists them into the build system via workspace crate boundaries.

### Rule 1 — Each module owns its tables

No two modules share a table. `transactions` owns the `transactions`
table; `accounts` owns `users`. If a query needs data from another
module, it goes through that module's public ports, which means a
Rust function call — never a JOIN across module boundaries.

Enforcement during the monolith era: grep for table names in foreign
module dirs during review. Enforcement after microservices
extraction: the other module's tables live in a different database
and the JOIN is physically impossible.

### Rule 2 — Only `ports` is public

Each module exposes one file — `ports.rs` — containing traits and
data types other modules are allowed to depend on. Everything else
(`domain/`, `application/`, `infrastructure/`) is module-private
(`pub(crate)`-sealed to the module and, during the microservices
step, to the crate).

Cross-module calls go through dyn-dispatched trait objects, which:

- Lets tests swap in fakes without touching production code.
- Makes the microservices lift-out mechanical: replace the
  in-process impl with an HTTP/AMQP client that implements the same
  trait.

### Rule 3 — Modules never import each other's internals

A compile error is the feedback. If module A wants something from
module B that isn't in `b::ports`, the answer is one of:

1. **Add it to `b::ports`** (if it's genuinely a public concept of B).
2. **Model it inside A** (if it's really A's concern, not B's).
3. **Move it to `shared_kernel`** (if it's infrastructure common to
   multiple modules — db types, errors, ids, etc.).

Never "just make it `pub`".

### Rule 4 — `shared_kernel` is infrastructure, not business logic

`shared_kernel/` contains:

- DB pool + shard router (current `src/db/`).
- Redis pool (current `src/cache/`).
- Cross-cutting error types and response helpers.
- Idempotency machinery (a cross-cutting middleware concern, not a
  business rule).
- Domain-neutral event bus.

`shared_kernel` must never contain business logic. "A user has a
balance" is business logic and belongs in `accounts`. "A connection
pool dies and we retry" is infrastructure and belongs in
`shared_kernel`.

### Rule 5 — Events, not calls, for fire-and-forget

When module X needs to tell module Y "this happened", and Y does not
need to block X's response, use `shared_kernel::events` rather than
a direct port call. Current RabbitMQ plumbing becomes the carrier.
`notifications` is the canonical consumer of events.

This rule exists to keep modules loosely coupled for the eventual
microservices split — an event bus crossing service boundaries is
easier to reason about than a sprawl of synchronous RPCs.

### Rule 6 — The API layer is a thin gateway

`src/modules/<name>/api/` defines the HTTP handlers **for that
module's endpoints only**. A top-level router (today: `src/api/`,
tomorrow: `src/bootstrap/router.rs`) mounts each module's sub-router
at its prefix. Handlers only translate HTTP ↔ module ports; all
business logic lives in `application/` or `domain/`.

### Rule 7 — Tests live with the code they test

Unit tests for `transactions::domain::Transaction` live next to
`Transaction`, not in a central `tests/` dir. Integration tests that
cross module boundaries live in the top-level `tests/` directory.

---

## 4. What this is NOT

- **Not hexagonal architecture in full.** We use the ports/adapters
  idea to isolate DB and external-service dependencies, but we do not
  insist on pure-domain crates with zero external imports. Pragmatism
  wins; the goal is independent deployability, not architectural
  purity.
- **Not DDD's full tactical pattern library.** We borrow "bounded
  context" because it's load-bearing for module boundaries; we are
  not introducing aggregates, value objects, or repositories as a
  dogma. Where they help a specific module, use them; where they
  don't, don't.
- **Not Clean Architecture's strict layering.** `domain/` is pure;
  `application/` orchestrates; `infrastructure/` adapts; `api/`
  presents. We enforce the direction of dependencies (inwards only),
  but we do not require four separate crates per module.
- **Not a ticket to over-abstract.** Three slashes of indirection for
  a five-line function is still worse than an inline five-line
  function. Abstract at module boundaries; keep things concrete
  inside.

---

## 5. Current state of the codebase (2026-04)

- Existing code under `src/api/`, `src/db/`, `src/cache/`,
  `src/middleware/`, `src/queue/`, etc. is **unchanged**. The
  monolith still works exactly the same way.
- The groundwork added in this iteration is:
  - `docs/architecture/` (this directory) with the design docs.
  - `src/modules/` with a `_template/` and three empty
    skeletons (`accounts/`, `transactions/`, `notifications/`).
  - `src/shared_kernel/` README placeholder.
- **Nothing is wired into `lib.rs` yet.** The skeleton will not be
  picked up by the compiler until Phase 1 of the migration pulls in
  the first real module.

The expected next step (owner: the team, not this session) is
Phase 1 in [migration-plan.md](./migration-plan.md) — moving the
`accounts` concern out of `src/api/handlers.rs` + `src/models/` into
`src/modules/accounts/`, with no behaviour change, as the canonical
worked example.
