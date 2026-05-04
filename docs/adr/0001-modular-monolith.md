# 0001 — Modular monolith first, microservices later

**Status:** Accepted
**Date:** 2026-02-15

## Context

The original codebase was a single Rust binary with a flat layout:
one `api/handlers.rs` talked directly to a single `db/pool.rs` that
queried any table in any shard. There was no compile-time fence
between business concerns. Any change could touch any part of any
query.

The endgame we care about is a set of small independently-deployable
services. Going there in one jump is expensive: distributed-systems
debugging, deploy complexity, observability spend. None of these
costs are justified by current load — we are at one host, two replicas.

## Decision

Take the **incremental path**: refactor to a modular monolith first,
extract microservices only when load, deploy cadence, or ownership
forces it.

A modular monolith is one binary with hard internal boundaries:
each business concern is a separate crate, cross-module calls go
through a `ports` trait, and the dependency graph is acyclic and
enforced by Cargo (see [ADR-0002](0002-cargo-workspace-split.md)).

## Consequences

- **Cheap today**: one process, one deploy, one log stream. No service
  mesh, no distributed tracing primitives required beyond what we
  already have.
- **Cheap tomorrow**: extracting a module into its own service is a
  Cargo plumbing change, not a refactor. The port trait stays the
  same; the implementation behind it switches from in-process to
  HTTP/AMQP client.
- **Discipline cost**: developers must respect module boundaries even
  when bypassing them would be locally faster. The compiler enforces
  most of this (ADR-0002), but cross-layer rules (e.g. domain ↛ infra)
  remain review-only.
- **Rollback**: trivial — every phase ships behind a feature flag or
  a new route prefix. The old layout is in git history.

## Alternatives considered

- **Stay flat.** Cheap until the first cross-team coordination event
  or a hot-path scaling crunch, at which point untangling costs more
  than gradual extraction.
- **Jump straight to microservices.** Would have cost weeks on
  scaffolding (service discovery, distributed tracing, RPC contracts)
  before shipping a single feature, with no current load to justify it.
