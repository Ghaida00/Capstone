.PHONY: fmt clippy test check all

## ── CI / Check-in Policy ──────────────────────────────────────
## Run `make check` before pushing any code. All three gates
## (fmt, clippy, test) must pass.

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets -- -D warnings

test:
	cargo test

check: fmt clippy test
	@echo "✅ All checks passed."
