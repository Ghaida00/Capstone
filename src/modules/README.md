# `src/modules/` — bounded-context modules

This directory is the **target home** of all business logic in the
peakload codebase. Each subdirectory is one **module** (one bounded
context, in DDD terms) and follows the exact same internal layout.

> **Current state:** groundwork only. Neither `src/lib.rs` nor
> `src/bootstrap.rs` pulls this tree in yet. The compiler will not
> build these files until Phase 1 of the migration plan starts.
> All existing code under `src/api/`, `src/db/`, `src/cache/`,
> `src/middleware/`, `src/queue/`, etc. remains the source of truth
> for the running binary.

---

## 1. Read order for new contributors

1. [../../docs/architecture/modular-monolith.md](../../docs/architecture/modular-monolith.md) — what this is and why.
2. [../../docs/architecture/module-anatomy.md](../../docs/architecture/module-anatomy.md) — how a single module is laid out.
3. [../../docs/architecture/dependency-rules.md](../../docs/architecture/dependency-rules.md) — what imports what.
4. [../../docs/architecture/migration-plan.md](../../docs/architecture/migration-plan.md) — how we get from "current monolith" to this tree.
5. [`_template/`](./_template/) — copy-this starting point for a new module.

Then look at one of the three populated skeletons:

- [`accounts/`](./accounts/) — leaf module, no outbound module deps.
- [`transactions/`](./transactions/) — depends on `accounts::ports`.
- [`notifications/`](./notifications/) — independent; event-driven.

## 2. Dependency graph among these modules

```
      notifications          (C — independent, event-driven)
           ▲
           │ consumes events via shared_kernel
           │
    transactions  ────────►  accounts
       (A)                     (B — leaf)
```

- A change to `transactions` rebuilds only `transactions`.
- A change to `accounts::ports` rebuilds `accounts` and
  `transactions`; never `notifications`.
- A change to `accounts` internals rebuilds only `accounts`.

See [../../docs/architecture/dependency-rules.md §5](../../docs/architecture/dependency-rules.md) for the full matrix.

## 3. Adding a new module

1. `cp -r _template your_module`.
2. Rewrite `your_module/README.md` — what it's for, which tables
   it owns, what ports it exposes, what events it publishes /
   consumes.
3. Define the public trait(s) in `your_module/ports.rs`.
4. Fill in `domain/`, `application/`, `infrastructure/`, `api/` in
   that order (inside-out).
5. Wire it into the bootstrap once you're ready to swap the old
   code path.

## 4. What does NOT live here

- **Infrastructure that is truly cross-cutting** — DB pools, Redis
  pools, shared error types, event bus, idempotency middleware —
  lives in [`../shared_kernel/`](../shared_kernel/).
- **HTTP plumbing that is not a business endpoint** — CORS,
  rate-limit middleware, auth middleware — lives in
  `src/middleware/` (staying there through the migration).
- **Bootstrap wiring** — goes in `src/bootstrap.rs` (or a split
  `src/bootstrap/` once the graph grows).
