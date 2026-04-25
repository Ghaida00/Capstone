# Dependency Rules — Who Can Import What

This is the enforcement document. Every rule here is a line a reviewer
can cite when blocking a PR. It complements
[modular-monolith.md](./modular-monolith.md) (the why) and
[module-anatomy.md](./module-anatomy.md) (the per-file shape) by
spelling out the **directional** rules between layers and modules.

---

## 1. The big rule, once

> A file may import from another file only if the arrow between them
> points **inward** (toward pure domain) and **never** crosses a
> module boundary except through a `ports.rs`.

Everything below is a specialisation of that rule.

---

## 2. Layer dependency matrix

Rows are the importer, columns are the importee. A `✔` means "allowed";
a `✘` means "disallowed". Left-to-right reading: "X may / may not
import from Y".

|                      | api | application | domain | infrastructure | ports (same mod) | shared_kernel | other-mod ports |
|----------------------|-----|-------------|--------|----------------|------------------|---------------|-----------------|
| **api**              |  —  |      ✔      |    ✔*  |        ✘       |        ✔         |       ✔       |        ✘        |
| **application**      |  ✘  |      —      |    ✔   |        ✘       |        ✔         |       ✔       |        ✔        |
| **domain**           |  ✘  |      ✘      |    —   |        ✘       |        ✘         |     ✔**       |        ✘        |
| **infrastructure**   |  ✘  |      ✘      |    ✔   |        —       |        ✔         |       ✔       |        ✘        |
| **ports (same mod)** |  ✘  |      ✘      |    ✔   |        ✘       |        —         |     ✔**       |        ✘        |
| **shared_kernel**    |  ✘  |      ✘      |    ✘   |        ✘       |        ✘         |       —       |        ✘        |

\* api may import from domain **only** for data-type re-export; business
logic calls must go through application.

\*\* domain and ports may import **only** pure types from
shared_kernel — never infrastructure facades (no `shared_kernel::db`,
no `shared_kernel::cache`).

Quick read:

- **api ← application ← domain** (outer depends on inner, not vice versa).
- **infrastructure ← domain** (infrastructure implements traits
  declared in domain).
- **ports** is importable by anyone, imports only domain + pure
  shared_kernel types.
- **shared_kernel imports from NOBODY**. It is the leaf of the graph.

---

## 3. Module-to-module rules

There are exactly **three** allowed ways for module A to depend on
module B:

1. **Port trait call.** A's application service holds a
   `Arc<dyn B::ports::BService>` injected at wiring time and calls
   trait methods on it.
2. **Port DTO type.** A constructs or deconstructs a DTO type defined
   in `B::ports` (e.g. `AccountId`, `Money`).
3. **Event subscription.** A publishes / consumes events through
   `shared_kernel::events` — the other module's identity never
   appears in the import graph.

Anything else is forbidden. Specifically:

- ✘ `use crate::modules::b::domain::...`
- ✘ `use crate::modules::b::application::...`
- ✘ `use crate::modules::b::infrastructure::...`
- ✘ `use crate::modules::b::api::...`

A quick grep check during review:

```
# Any import from another module that is NOT ...::ports is a violation.
git diff --name-only origin/main... | \
  xargs grep -nE 'use crate::modules::[a-z_]+::(domain|application|infrastructure|api)'
```

---

## 4. Concrete example — the three canonical modules

### accounts (B, leaf)

- Exposes: `accounts::ports::{AccountService, AccountId, Balance,
  AccountError}`.
- Imports from other modules: **none**.
- Imports from shared_kernel: `db`, `errors`, `events`.

### transactions (A, depends on B)

- Exposes: `transactions::ports::{TransactionService, Transaction,
  TransactionError}`.
- Imports from other modules: `accounts::ports::AccountService` (as
  `Arc<dyn AccountService>` injected into
  `transactions::application::TransferMoney`).
- Imports from shared_kernel: `db`, `cache`, `errors`, `events`,
  `idempotency`.

### notifications (C, independent)

- Exposes: `notifications::ports::{NotificationDispatcher}` (so other
  modules can trigger a notification synchronously if ever needed —
  expected usage is event-driven though).
- Imports from other modules: **none**.
- Imports from shared_kernel: `events` (the main inbound), `errors`.

---

## 5. Who rebuilds when X changes

These fall out automatically from the import graph in §2 and §3, but
worth stating directly because the user explicitly asked for them.

| Change in…                          | Forces recompilation of…                         |
|-------------------------------------|--------------------------------------------------|
| `accounts::domain` (pure internals) | `accounts`                                       |
| `accounts::application`             | `accounts`                                       |
| `accounts::infrastructure`          | `accounts`                                       |
| `accounts::api`                     | `accounts` + bootstrap (router registration)     |
| `accounts::ports` **(trait shape!)**| `accounts` **+ transactions**                    |
| `transactions::*` (anything)        | `transactions` only                              |
| `notifications::*` (anything)       | `notifications` only                             |
| `shared_kernel::*`                  | Everything                                       |

Reads directly onto the user's stated requirements:

> *"Changes to A will not affect B and C."*  
> Any change in `transactions` (A): neither `accounts` nor
> `notifications` rebuilds. ✔

> *"Changes to B will only affect A and not C."*  
> Change in `accounts::ports` (B): `transactions` (A) rebuilds,
> `notifications` (C) does not. ✔  
> Change in `accounts::domain/application/infrastructure`
> internals: `accounts` alone rebuilds (not even A). ✔ — which is
> stronger than the stated rule.

---

## 6. Enforcement roadmap

**Today (Phase 0 — groundwork).** These rules are enforced by:

- Convention, stated here.
- Code review, citing this file.
- Visibility modifiers in the skeleton (`pub(crate)` everywhere
  except `ports.rs` and the bootstrap wiring exports).

**Phase 1 — the first real module.** We add a lint CI check:

```
cargo +nightly clippy -- -D warnings
```

plus a small shell script that greps for forbidden import patterns
(see §3) and fails the build on any hit. Add to CI as
`scripts/check-module-boundaries.sh`.

**Phase 2 — workspace crates.** Each module becomes its own crate in
a Cargo workspace. At that point the compiler does the enforcement
for us — a forbidden import is a compile error, not a convention.
Shared_kernel is a crate too; modules list it (plus any needed peer
modules' ports crates) under `[dependencies]` in their `Cargo.toml`.

The groundwork in this iteration does not yet split crates; that
happens in Phase 4 of
[migration-plan.md](./migration-plan.md). But the file layout inside
`src/modules/` is deliberately shaped so the split is a directory
move, not a rewrite.

---

## 7. Exceptions (there are none)

There are no legitimate exceptions to these rules. If you hit a case
where a rule seems to force an awkward shape, the module boundary is
probably wrong — not the rule. Examples of apparent exceptions and
their resolutions:

| Tempting shortcut                                      | Correct fix                                                                 |
|--------------------------------------------------------|-----------------------------------------------------------------------------|
| "I want `transactions` to read a column from `users`." | Add a method to `accounts::ports` that returns what you need.               |
| "I want to share a sqlx row struct between modules."   | Define the DTO in the port; convert in infrastructure.                      |
| "The two modules really are tightly coupled."          | They are one module. Merge them.                                            |
| "I just want a one-off util used by both."             | It's infrastructure: put it in `shared_kernel`.                             |
| "Test X in module A needs to poke module B's state."   | Write it as an integration test in `tests/`, which may use both port APIs.  |
| "A circular dependency is actually what I want."       | No. Break it with events.                                                   |

If none of these fit, raise it in review with a concrete code snippet
— we'll update this file before we bend it.
