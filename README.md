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

## 🚀 Panduan Menjalankan Project (Tutorial Lengkap)

Bagian ini akan memandu Anda dari awal untuk menjalankan project ini secara lokal beserta dengan semua infrastrukturnya (Database, Redis, RabbitMQ, Prometheus, Grafana, dll).

### 1️⃣ Persiapan Kebutuhan (Prerequisites)
Pastikan Anda sudah menginstal alat-alat berikut di sistem Anda:
- **Rust** (stable toolchain) - [Panduan Install](https://www.rust-lang.org/tools/install)
- **Docker** & **Docker Compose** - [Panduan Install](https://docs.docker.com/get-docker/)

### 2️⃣ Clone Repository
Pertama, _clone_ repository ini:
```bash
git clone <URL-REPOSITORY>
cd Capstone
```

### 3️⃣ Setup Environment Variables
Salin file konfigurasi _.env.example_ menjadi _.env_ untuk digunakan oleh container dan aplikasi backend.
```bash
cp .env.example .env
```

### 4️⃣ Menjalankan Sistem Secara Full Stack
Silakan jalankan perintah Docker Compose di bawah ini untuk memulai seluruh infrastruktur (Backend 2 Replicas, PostgreSQL Sharding, Redis HA Sentinel, RabbitMQ, serta Monitoring tools):
```bash
docker-compose up -d --build
```
Tunggu hingga proses build selesai dan status seluruh kontainer menjadi `healthy` atau `Up`. Anda bisa mengeceknya dengan:
```bash
docker-compose ps
```

### 5️⃣ Mengakses Layanan
Setelah semuanya berjalan tanpa error, Anda dapat mengakses platform beserta alat _monitoring_ di _port_ berikut:
- **API Backend**: `http://localhost:8080` (di-load balance otomatis oleh NGINX ke backend replicas)
- **Monitoring Grafana**: `http://localhost:3001` (Username `admin`, Password `admin`)
- **Prometheus UI**: `http://localhost:9090`
- **RabbitMQ Management**: `http://localhost:15672` (Username `gn_user`, Password `gn_secure_pass`)

### 6️⃣ Testing dan Development Lanjutan
Untuk kepentingan pengembangan (_development_) serta pengujian aplikasi (_tests_):
- **Cek Code Quality**: Menjalankan formatter dan clippy secara otomatis sesuai _check-in policy_. 
  ```bash
  make check
  ```
- **Menjalankan Tests**: Menjalankan _unit test_ dan _property-based test_.
  ```bash
  cargo test
  ```

### 🛑 Menghentikan Layanan
Ketika Anda sudah selesai, matikan environment infrastrukturnya dengan perintah:
```bash
docker-compose down
```
_Gunakan perintah `docker-compose down -v` jika Anda ingin ikut menghapus semua volume/data dari Database, Redis, dsb yang tersimpan._

<img width="8192" height="4971" alt="Rust API Cluster Load-2026-03-01-174806" src="https://github.com/user-attachments/assets/f5e6f21d-baef-4d1c-9804-a12f1f3040e9" />
