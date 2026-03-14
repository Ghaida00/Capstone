# ============================================================
# Stage 1: Planner — generate the dependency recipe
# ============================================================
FROM rust:slim-bookworm AS planner

RUN cargo install cargo-chef --locked

WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ============================================================
# Stage 2: Cacher — build only dependencies from the recipe
# ============================================================
FROM rust:slim-bookworm AS cacher

RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# ============================================================
# Stage 3: Builder — compile the actual application source
# ============================================================
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Re-use the cached dependency artifacts from the cacher stage
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo

COPY . .
RUN cargo build --release

# ============================================================
# Stage 4: Runtime — Distroless production image
# ============================================================
FROM gcr.io/distroless/cc-debian12 AS runtime

COPY --from=builder /app/target/release/gn-backend /app/gn-backend

# Distroless ships with a built-in nonroot user (UID 65534)
USER nonroot:nonroot

EXPOSE 3000

# No HEALTHCHECK here — distroless has no shell.
# Health probes are defined in docker-compose.yml using the
# container's exposed HTTP endpoint via service-level checks.

CMD ["/app/gn-backend"]
