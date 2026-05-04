# 0002 — Cargo workspace split for compiler-enforced module boundaries

**Status:** Accepted
**Date:** 2026-03-10

## Context

Phases 1–3 of the modular-monolith refactor (per [ADR-0001](0001-modular-monolith.md))
moved each business concern into `src/modules/<name>/` with the
shape defined in [ADR-0003](0003-port-adapter-shape.md). Boundaries
were enforced by **convention**: `pub(crate)` visibility, grep-based
review, and "do not import this from there" rules in a docs file.

That worked but it was fragile. A reviewer who missed a forbidden
`use crate::modules::accounts::domain::Account` from inside
`transactions::infrastructure` would let it land. The fence was
aspirational, not mechanical.

## Decision

Promote each module to its own Cargo crate inside a workspace. The
dependency graph becomes a fact in `Cargo.toml`, not a paragraph in
a markdown file.

```
Cargo.toml                workspace root
crates/
  app/                    binary (composition root only)
  shared_kernel/          cross-cutting infra (db, cache, queue, events, error)
  accounts/               leaf module
  transactions/           depends on accounts (via ports)
  notifications/          depends only on shared_kernel
```

A forbidden cross-module import is now a compile error
(`unresolved crate transactions`) rather than a review comment.

## Consequences

- **Compiler-enforced fences.** A module that grows an undeclared
  dependency on another module fails `cargo build`. Adding such a
  dependency requires editing `Cargo.toml`, which is reviewable.
- **Per-crate parallel builds.** `cargo build -p accounts` skips
  the rest. Cold workspace build ≈ 44s on the dev machine.
- **`crates/app/` is now a pure composition root** (~200 LOC):
  `main.rs`, `app.rs`, `bootstrap.rs`, `config.rs`, `health.rs`,
  `middleware/`. No business logic.
- **Shared infrastructure had to migrate up.** Module crates cannot
  depend on `app` (would be a cycle), so cross-cutting code (db
  pool, cache, queue producer, error type, response helpers) lives
  in `shared_kernel`.
- **Constructor signatures changed.** Where module init used to take
  `&Config`, it now takes a kernel-local config slice
  (`ShardRouterConfig`, `RedisCacheConfig`, `&str` amqp_url) so the
  kernel does not pull in env-loading concerns.
- **Rollback**: revert the workspace-split PR. Source files stay where
  they are; only the Cargo manifests are added.

## Alternatives considered

- **Stay single-crate, enforce by convention.** Tried in Phases 1–3.
  Worked but did not scale to a fourth contributor.
- **Use a Rust-side architecture-test crate** (e.g. `cargo-modules`,
  `cargo-deny` with custom rules). Adds tooling without removing
  the convention layer; chose Cargo's native crate boundaries instead.
