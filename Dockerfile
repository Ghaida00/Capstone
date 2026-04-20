# syntax=docker/dockerfile:1.7
# ============================================================
# Base — pinned Rust + cargo-chef + system build deps (shared)
# ============================================================
FROM rust:1.95-slim-bookworm AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked

WORKDIR /app

# ============================================================
# Planner — generate dependency recipe
# ============================================================
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ============================================================
# Builder — cook deps (cached), then compile app
# ============================================================
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Cache the cargo registry across builds (BuildKit)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --locked --bin peakload-capstone

# ============================================================
# Runtime — distroless (nonroot baked in: UID 65532)
# ============================================================
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

WORKDIR /app
COPY --from=builder /app/target/release/peakload-capstone /app/peakload-capstone

EXPOSE 3000

# No HEALTHCHECK here — distroless has no shell.
# docker-compose performs health probes via direct binary exec.

ENTRYPOINT ["/app/peakload-capstone"]
