# Project: Exploding User Data Scalabilities (Topik B.4)
Repository ini berisi arsitektur dan implementasi prototype sistem manajemen beban puncak (Peak Load) yang mampu menangani lonjakan trafik secara efisien dan aman.

🛡️ Key Features:
- **High Performance**: Backend menggunakan Rust (Axum & Tokio) untuk efisiensi memori dan latensi rendah.
- **Horizontal Scalability**: Load balancing menggunakan Nginx ke 2 replicas application server.
- **Database Sharding**: PostgreSQL dengan konfigurasi **2 active Shards** (shard 2 dideklarasikan di kode router namun container-nya di-disable di [docker-compose.yml](docker-compose.yml) untuk kapasitas capstone; lihat baris 7 dan 54), masing-masing menggunakan pola **1 Primary & 1 Replica** untuk skalabilitas data (Total 4 DB instances di compose). Sharding logic mendukung 3 shards penuh; re-enable shard 2 cukup dengan uncomment block-nya di compose.
- **Resilience**: Proteksi berlapis dengan Rate Limiting, Circuit Breaker, Retries, dan mekanisme Backpressure.
- **Data Integrity**: Menjamin konsistensi data dengan Idempotency Key dan pemrosesan asinkron via RabbitMQ.
- **Full Observability**: Monitoring real-time menggunakan Prometheus, Grafana, dan cAdvisor (Metrics, Logs, Tracing).

📊 SLO Targets:

Availability: 99.9%

Latency: P95 < 500ms

Error Budget: 100 Failed Transactions per 1M Request

## 🚀 Panduan Menjalankan Project (Tutorial Lengkap)

Bagian ini akan memandu Anda dari awal untuk menjalankan project ini secara lokal beserta dengan semua infrastrukturnya (Database, Redis, RabbitMQ, Prometheus, Grafana, dll).

### 1️⃣ Persiapan Kebutuhan (Prerequisites)
Pastikan Anda sudah menginstal alat-alat berikut di sistem Anda:
- **Rust** - [Panduan Install](https://www.rust-lang.org/tools/install)
- **Docker** & **Docker Compose** - [Panduan Install](https://docs.docker.com/get-docker/)
- **Make** - [Panduan Install](https://github.com/chocolatey/choco/releases/tag/2.7.1)

### 2️⃣ Clone Repository
Pertama, _clone_ repository ini:
```bash
git clone https://github.com/Ghaida00/Capstone
cd Capstone
```

### 3️⃣ Setup Environment Variables
Salin file konfigurasi _.env.example_ menjadi _.env_ untuk digunakan oleh container dan aplikasi backend. Gunakan perintah sesuai sistem operasi Anda:

- **macOS / Linux / WSL / Git Bash:**
  ```bash
  cp .env.example .env
  ```
- **Windows (PowerShell):**
  ```powershell
  Copy-Item .env.example .env
  ```
- **Windows (cmd):**
  ```cmd
  copy .env.example .env
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

### 6️⃣ Testing dan Development Lanjutan
Untuk kepentingan pengembangan (_development_) serta pengujian aplikasi (_tests_):
- **Cek Code Quality**: Menjalankan formatter, clippy (linting), dan unit tests secara otomatis.
  ```bash
  make check
  ```
- **Menjalankan Unit Tests**: Menjalankan seluruh _unit test_ dan _property-based test_ secara manual.
  ```bash
  cargo test
  ```
- **Performance/Load Test (k6)**: Menjalankan pengujian beban tinggi (High Load Scenario) sesuai target SLO.
  ```bash
  k6 run k6/load-test-1m.js
  ```

### 🛑 Menghentikan Layanan
Ketika Anda sudah selesai, matikan environment infrastrukturnya dengan perintah:
```bash
docker-compose down
```
_Gunakan perintah `docker-compose down -v` jika Anda ingin ikut menghapus semua volume/data dari Database, Redis, dsb yang tersimpan._

## 🗺️ Arsitektur Sistem

![Diagram arsitektur sistem Peakload](docs/architecture/system-overview.png)
