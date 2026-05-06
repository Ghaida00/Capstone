# 0003 — Port-and-adapter shape per module

**Status:** Accepted
**Date:** 2026-02-15

## Context

Once we committed to modular monolith ([ADR-0001](0001-modular-monolith.md))
we needed a uniform internal shape for every module so:

- A reader who has navigated one module can navigate any other.
- Tooling (grep, rename, schema migration) works the same way
  everywhere.
- Domain logic is isolated from sqlx/redis/HTTP, which is what makes
  testing and future service extraction cheap.

## Decision

Every module crate (`crates/<name>/src/`) follows the same layout:

```
lib.rs                  crate entry; wires submodules
ports.rs                THE public contract: traits + DTOs other modules import
domain/                 entities, value objects, domain errors. Pure Rust.
                        No sqlx, redis, HTTP, AMQP imports here.
application/            use-case orchestration. Receives ports of OTHER
                        modules by injection (Arc<dyn ...>).
infrastructure/         adapters: sqlx repository, AMQP producer/consumer,
                        external API clients. Implements domain traits.
api/                    axum sub-router this module owns. Translates
                        HTTP ↔ ports. No business logic here.
```

**Layer dependency rule:** dependencies point **inward**. `api` and
`infrastructure` may import from `application` and `domain`; the reverse
is forbidden. `domain` imports nothing from this module's own
`infrastructure` or `api`.

**Cross-module rule:** module A may depend on module B **only**
through `B::ports`. Never by reading B's tables, never by importing
B's domain types.

A copy-able starting point lives at
[`docs/architecture/module-template/`](../architecture/module-template/).

## Consequences

- **Domain is testable without a database.** `cargo test -p accounts --lib`
  needs no Docker.
- **Swapping infrastructure is a localised change.** Replacing the
  sqlx repository with a different driver, or the in-process event
  bus with AMQP ([ADR-0004](0004-in-process-event-bus.md)), edits one
  file in `infrastructure/` and nothing else.
- **Cross-layer rules are review-only.** Cargo enforces *cross-module*
  edges (ADR-0002) but cannot enforce "domain ↛ infra" inside a single
  crate. Reviewers cite this ADR when blocking such PRs.
- **Some boilerplate.** A trivial CRUD handler still has to thread
  through `api → application → domain → infrastructure`. That is the
  cost; the benefit is that there is exactly one place to look when
  any of those layers misbehaves.

## Alternatives considered

- **Free-form per-module structure.** Each module organises however
  the author prefers. Rejected: the navigation cost compounds and
  cross-module changes lose the regularity that makes them mechanical.
- **Skip `ports.rs`, expose `application::` types directly.** Couples
  consumers to the implementation crate, defeats the swap-for-service
  story in ADR-0001.
