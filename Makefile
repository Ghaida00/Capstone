.PHONY: fmt clippy test check all up down build logs

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

## ── Docker ────────────────────────────────────────────────────

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f --tail=100

restart:
	docker compose down && docker compose up -d --build
