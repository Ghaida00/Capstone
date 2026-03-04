# ============================================================
# Stage 1: Build the Rust application
# ============================================================
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency builds
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf src

# Build actual application
COPY src/ src/
RUN touch src/main.rs && cargo build --release

# ============================================================
# Stage 2: Minimal runtime image
# ============================================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/gn-backend /app/gn-backend

# Non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser
USER appuser

EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

CMD ["/app/gn-backend"]
