Project: Exploding User Data Scalabilities (Topik B.4)
"Built for Speed, Designed for Resilience."
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

<img width="8192" height="4971" alt="Rust API Cluster Load-2026-03-01-174806" src="https://github.com/user-attachments/assets/f5e6f21d-baef-4d1c-9804-a12f1f3040e9" />
