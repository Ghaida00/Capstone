# Peakload — Konten Diseminasi WordPress

**Judul posting:** Optimasi Skalabilitas Transaksi Perbankan untuk Mengatasi Exploding User Data melalui Peak Load Management dan Read/Write Separation Berbasis Rust

**Brand:** Peakload · **Tim:** FullTIm · **Topik:** B.4 · **Kelompok:** 8 · **Tag:** capstone

---

## 1. Hero

**H1:** Optimasi Skalabilitas Transaksi Perbankan untuk Mengatasi Exploding User Data

**Subjudul:** Peakload — Peak Load Management & Read/Write Separation Berbasis Rust

**Tagline:** Backend Rust yang menahan lonjakan **1 juta transaksi/jam** dengan latensi P95 **4,43 ms** dan error rate **0,00%**.

**CTA:** Lihat Demo · [Repositori GitHub](https://github.com/Ghaida00/Capstone) · Dokumentasi API

**Counter metrics:** 1M txn/jam · 4,43 ms P95 · 0,00% error · 23 services · 152 tests

---

## 2. Tim Kami — FullTIm

### Ghaida Nayla — 235150700111001
**Peran:** Technical Lead & System Architect (34%)

Menentukan tech stack dan arsitektur sistem. Infrastruktur Docker Compose full-stack dengan 4 profil skala (1–3 shard). Read/write separation via HAProxy /primary & /replica. Database HA Patroni & tuning performa consumer. Membangun demo ops dashboard web secara penuh. Quality assurance.

**Email:** ghaidanayla@student.ub.ac.id

### Lovely Ito Panjaitan — 235150700111012
**Peran:** Backend API Engineer (33%)

Implementasi endpoint API Rust: GET balance dengan Redis caching, GET status shard-aware, POST transactions dengan idempotency, consumer batch debit-credit. POST `/api/v2/accounts` dengan validasi, auto-generate account number, HTTP 201/409/400.

**Email:** lovelyito@student.ub.ac.id

### Verda Aulia Setri — 235150701111014
**Peran:** Observability & Performance Engineer (33%)

Perancang dan eksekutor k6 load testing simulasi 1 juta txn/jam. Analisis latensi, throughput, cross-shard consistency. Validasi poison message isolation, DLQ, fail-fast validation. Monitoring infrastruktur Docker. Design banner proyek.

**Email:** verdaaulia@student.ub.ac.id

### Dosen Pembimbing

**Ir. Lutfi Fanani, S.Kom., M.T., M.Sc.** — Departemen Teknik Informatika, FILKOM UB  
Email: lutfifanani@ub.ac.id · [Profil dosen](https://filkom.ub.ac.id/profile/dosen/lutfi.fanani)

---

## 3. Mitra — Bank X (Studi Kasus)

Bank X adalah mitra studi kasus perbankan digital yang mengalami **kegagalan operasional kritis** saat lonjakan transaksi hingga **1 juta per jam**, dengan latensi P95 **> 10 detik** — dikategorikan sebagai kegagalan layanan total pada standar perbankan digital.

**Akar masalah yang dikonfirmasi mitra:**
- Arsitektur monolitik tanpa pemisahan jalur read/write → connection pool exhaustion & lock contention
- Tidak ada backpressure, rate limiting, dan queueing → cascading failure
- Visibilitas rendah — hanya CPU/memori, tanpa metrik per-endpoint

**Tindak lanjut tim FullTIm:** Setiap masukan mitra diterjemahkan menjadi SLO terukur dan diimplementasikan (read/write separation, middleware resilience, Transactional Outbox, observability penuh, demo dashboard).

---

## 4. Ringkasan Proyek

Bank X mengalami kegagalan operasional saat lonjakan transaksi hingga 1 juta per jam, dengan latensi P95 melampaui 10 detik akibat arsitektur monolitik tanpa pemisahan read/write dan minim observability. Tim **FullTIm** membangun **Peakload**: backend Rust (Axum + Tokio) sebagai 2 replika stateless di belakang Nginx, dengan read/write separation nyata (tulis via pgBouncer → HAProxy /primary; baca via /replica per-shard), PostgreSQL multi-shard Patroni HA, Redis Sentinel, RabbitMQ dengan Transactional Outbox, middleware resilience berlapis, serta demo ops dashboard web.

Pengujian k6 (300 VU, 15 menit) membuktikan throughput ~1 juta txn/jam, P95 4,43 ms, error rate 0,00%. Sistem siap dijalankan penuh via `docker compose up`.

**Target pengguna:** Tim engineering & operasional Bank X (pengguna langsung backend dan ops dashboard); nasabah sebagai penerima manfaat akhir transaksi yang andal dan cepat.

---

## 5. Pain Points

| Masalah | Dampak |
|---------|--------|
| Lonjakan 1 juta txn/jam | Downtime kritis, P95 > 10 detik |
| Monolit tanpa read/write split | Pool exhaustion, lock contention di DB |
| Tanpa backpressure & observability | Cascading failure; mitigasi terlambat |

---

## 6. Solusi

1. **Async write path** — HTTP 202 → Transactional Outbox → RabbitMQ → batch consumer (≤200 msg, flush 250 ms)
2. **Read/Write Separation** — HAProxy /primary vs /replica; P50 balance read 0,796 ms
3. **Resilience berlapis** — backpressure → circuit breaker → rate limit (64-shard) → JWT auth
4. **Observability + demo dashboard** — Prometheus, Grafana, Jaeger, React ops UI di `/dashboard/`

---

## 7. Cara Kerja Sistem

### Write path
1. Client POST → Nginx (LB + edge rate limit)
2. Protection stack
3. Dual idempotency (Redis Tier-1 + PG Tier-2, SHA-256)
4. Atomic INSERT outbox + debit (PG primary)
5. publish_outbox → RabbitMQ
6. Batch consumer apply debit/credit
7. Event bus → notifications + cache invalidation
8. Client poll GET `/api/v2/transactions/status/{ref}` → completed

### Read path
GET `/api/v2/accounts/{id}/balance` → moka L1 → Redis L2 → HAProxy /replica

**Arsitektur interaktif:** [`docs/architecture/architecture.html`](../architecture/architecture.html)

---

## 8. Pemetaan Kebutuhan vs Fitur

| # | Kebutuhan | Fitur | Status |
|---|-----------|-------|--------|
| 1 | Transfer dana | POST `/api/v2/transactions` | Ya |
| 2 | Status transaksi | GET `/status/{ref}` | Ya |
| 3 | Riwayat transaksi | GET `/api/v2/transactions` | Ya |
| 4 | Cek saldo cepat | GET `/accounts/{id}/balance` | Ya |
| 5 | Registrasi akun | POST `/api/v2/accounts` | Ya |
| 6 | Anti double-spend | Dual idempotency | Ya |
| 7 | Tahan lonjakan | Middleware resilience | Ya |
| 8 | Read/write separation | HAProxy /primary & /replica | Ya |
| 9 | HA database | Patroni multi-shard | Ya |
| 10 | Monitoring real-time | Prometheus + Grafana + dashboard | Ya |

---

## 9. Fitur Utama (15 fitur)

**Core Transaction:** Create async (202), Registrasi akun, Inquiry saldo (2-tier cache), Cross-shard transfer, Dual idempotency

**Resilience & HA:** Transactional Outbox, RabbitMQ batch consumer, Middleware stack, Multi-shard Patroni (profil A–D), Redis Sentinel, Graceful shutdown

**Ops & Observability:** Demo ops dashboard, Prometheus/Grafana/Jaeger, k6 suite (load/soak/e2e), Disaster recovery (Patroni failover + outbox durability)

---

## 10. Teknologi

Rust · Axum · Tokio · PostgreSQL 18 · Patroni · etcd · pgBouncer · HAProxy · Redis Sentinel · RabbitMQ · Nginx · Docker Compose · Prometheus · Grafana · Jaeger · OpenTelemetry · k6 · mimalloc

Preferensi mitra: Rust (memory safety + performa tinggi)

---

## 11. Arsitektur

| Lapisan | Komponen |
|---------|----------|
| Edge | Nginx, CSP headers, edge rate limit |
| Application | Rust modular monolith ×2: app, accounts, transactions, notifications, shared_kernel |
| Data plane | PG sharded Patroni, Redis Sentinel, RabbitMQ, profil skala A–D |

---

## 12. Hasil & Evaluasi

### Load test utama (k6, 300 VU, 15 menit)

| Metrik | Hasil | Target |
|--------|-------|--------|
| Throughput | ~1 juta txn/jam | 1 juta txn/jam |
| HTTP P95 | 4,43 ms | < 500 ms |
| Balance P95 | 4,68 ms | < 10 ms |
| Error rate | 0,00% (0/512.104) | < 5% |
| Unit tests | 152 pass | — |

### Sertifikasi multi-profil (A–D)

| Profil | tx/s | Balance p95 | E2E p99 | Error |
|--------|------|-------------|---------|-------|
| A (2-shard) | ~260 | 2,71 ms | 3,66 s | 0% |
| B (1-shard) | ~260 | 2,69 ms | 3,40 s | 0% |
| C (2-shard hi-mem) | ~260 | 5,38 ms | 2,57 s | 0% |
| D (3-shard) | ~260 | 3,75 ms | 4,28 s | 0,008% |

### Validasi mitra — skor 4/4 kesesuaian solusi

### Keterbatasan
- Belum TLS/HTTPS production
- Deploy cloud belum dilakukan (fokus Docker Compose lokal)

---

## 13. Repositori & Demo

- **GitHub:** https://github.com/Ghaida00/Capstone
- **Quick start:** `git clone` → `cp .env.example .env` → `docker compose up -d --build`
- **API:** http://localhost:8080 · **Grafana:** :3001 · **Dashboard:** /dashboard/
- **Akun uji:** ACC_0000001, ACC_0000002 (100.000 akun seed)

---

## 14. Refleksi & Kontak

**Refleksi:**
- Kompetensi teknis: Rust async, Transactional Outbox, sharding, Patroni, k6 soak
- Kolaborasi: Git PR workflow 11 minggu, peran komplementer 3 anggota
- Ke depan: TLS, cloud deploy, validasi mitra lebih awal

**Rekomendasi:** TLS Nginx (1–2 hari) · Cloud/K8s staging (1–2 minggu)

**Kontak tim:** lihat section 2 · **FILKOM UB:** Jl. Veteran No.10-11, Malang 65145
