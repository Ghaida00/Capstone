# `_template` — copy this to start a new module

This is the canonical shape. Copy the whole directory to a new name,
rewrite this README, then fill in the code from the inside out
(`domain/` → `application/` → `infrastructure/` → `api/`).

The port-adapter shape this template encodes is documented in
[ADR-0003](../../adr/0003-port-adapter-shape.md). This README focuses
on the **per-module checklist** you should be able to answer before
the first PR lands.

---

## 1. What this module is for

*(One short paragraph. Example:*
> *Owns everything related to customer accounts: balances, status
> transitions (active/inactive/blocked), and the `users` table. It
> does not know about transactions; transactions call into this
> module through `accounts::ports::AccountService` when they need to
> read or mutate a balance.)*

## 2. Tables owned

- `table_name_1` — one sentence on what it is.
- `table_name_2` — same.

No other module reads from or writes to these tables. Any cross-
module data need is satisfied through `ports.rs`.

## 3. Ports exposed

`your_module::ports` contains:

- `YourService` trait (methods: …)
- DTOs: …
- Errors: `YourError`

These are the **only** types another module may import from here.

## 4. Ports consumed

- `other_module::ports::OtherService` — used in
  `application::SomeUseCase` to do X.

If the answer is "none", say "none".

## 5. Events published

Via `shared_kernel::events`:

- `YourModuleEvent { … }` — emitted when X happens. Consumers: …

## 6. Events consumed

- `SomeoneElsesEvent { … }` — handled in
  `infrastructure::inbound_events` to update Y.

## 7. Operational notes

Anything special about migrations, backfills, rollbacks, or known
operational gotchas for this module. Leave empty (`— none —`) if
boringly standard.
