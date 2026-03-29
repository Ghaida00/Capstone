# Project: Exploding User Data Scalabilities (Topik B.4)
Repository ini berisi arsitektur dan implementasi prototype sistem manajemen beban puncak (Peak Load) yang mampu menangani lonjakan trafik secara efisien dan aman.

🛡️ Key Features:
High Performance: Backend menggunakan Rust (Axum & Tokio) untuk efisiensi memori dan latensi rendah.

Resilience: Proteksi berlapis dengan Rate Limiting, Circuit Breaker, dan Dead Letter Queue (DLQ).

Data Integrity: Mengimplementasikan Idempotency Key dan Asynchronous Processing via RabbitMQ.

High Availability: Database PostgreSQL dengan konfigurasi 1 Primary & 2 Replicas serta mekanisme Failover/Promotion.

Full Observability: Monitoring real-time menggunakan Prometheus & Grafana (Metrics, Logs, Tracing).

📊 SLO Targets:

Availability: 99.0%

Latency: P95 < 500ms

Error Budget: 100 Failed Transactions per 1M Request

## Development

### Prerequisites

- **Rust** (stable toolchain)
- **Docker** & **Docker Compose** (for integration tests and full stack)

### Check-in Policy

Run all quality gates locally before pushing:

```bash
make check   # Runs: cargo fmt --check → cargo clippy → cargo test
```

### Running Tests

```bash
# Unit + property-based tests (no Docker required)
cargo test
```

### Building the Docker Image

```bash
docker build -t gn-backend:latest .
```

The Dockerfile uses a 4-stage `cargo-chef` pipeline (planner → cacher → builder → runtime) for optimized build caching.

### New Configuration Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DB_QUERY_TIMEOUT_SECS` | `5` | Database query timeout (must be < `API_TIMEOUT_SECS`) |
| `REDIS_COMMAND_TIMEOUT_SECS` | `3` | Redis command timeout (must be < `API_TIMEOUT_SECS`) |
| `API_TIMEOUT_SECS` | `30` | Top-level API request timeout |

<img width="8192" height="4971" alt="Rust API Cluster Load-2026-03-01-174806" src="https://github.com/user-attachments/assets/f5e6f21d-baef-4d1c-9804-a12f1f3040e9" />
