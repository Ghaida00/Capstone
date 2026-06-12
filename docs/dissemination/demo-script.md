# Demo Script — Peakload Capstone

## Versi lengkap (18 langkah — LK4 §F.2)

| # | Langkah | Input | Output yang diharapkan |
|---|---------|-------|------------------------|
| 1 | Jalankan sistem lengkap | `docker compose up -d --build` | 23 container healthy, 100.000 akun ter-seed |
| 2 | Verifikasi sehat | GET `/health` | status healthy, semua services true |
| 3 | Registrasi akun baru | POST `/api/v2/accounts` | HTTP 201 + account_number auto-generated |
| 4 | Uji duplikasi akun | POST akun sama 2× | HTTP 409 conflict |
| 5 | Cek saldo (jalur baca) | GET balance 2× berturut | HTTP 200; request ke-2 lebih cepat (cache) |
| 6 | Buat transaksi transfer | POST `/api/v2/transactions` | HTTP 202 + reference_id |
| 7 | Polling status | GET `/status/{ref}` tiap 1 detik | accepted → completed < 8 detik |
| 8 | Verifikasi saldo berubah | GET balance pengirim & penerima | Saldo sesuai amount transfer |
| 9 | Uji idempotency | POST payload identik 2× | Kedua response 202 identik |
| 10 | Uji rate limiting | Burst GET balance (RATE_LIMIT_BURST=5) | Campuran 200 dan 429 |
| 11 | Uji Transactional Outbox | Stop RabbitMQ → POST → start broker | 202 saat broker mati; completed setelah hidup |
| 12 | Cross-shard transaction | POST shard-0 → shard-1 | processing → completed |
| 13 | Patroni failover | Stop pg-shard0-node-a → POST tx | Failover ~15s; tx tetap completed |
| 14 | Pantau Prometheus | Buka :9090 | Targets UP |
| 15 | Pantau Grafana | Buka :3001 dashboard Peakload | RPS, P95, error rate real-time |
| 16 | Jaeger tracing | Buka :16686 | Distributed traces per request |
| 17 | Demo ops dashboard | Buka `/dashboard/` | UI live, burst load, metrik |
| 18 | k6 load test | `k6 run k6/load-test-1m.js` | P95 4,7ms ✔, error 0% ✔, ~1M txn/jam ✔ |

---

## Versi ringkas video (3–4 menit)

| Waktu | Scene | Narasi singkat |
|-------|-------|----------------|
| 0:00–0:10 | Title card Peakload + FullTIm | Judul capstone + tagline 1M txn/jam |
| 0:10–0:30 | Terminal: docker compose up + curl /health | "23 layanan healthy dalam satu perintah" |
| 0:30–1:00 | Dashboard: submit transaksi + reference_id | "Write path async — HTTP 202" |
| 1:00–1:25 | Poll status completed + saldo berubah | "Consumer RabbitMQ commit dalam < 8 detik" |
| 1:25–2:05 | Dashboard burst + grafik RPS/P95 live | "Ops dashboard untuk stakeholder non-teknis" |
| 2:05–2:35 | Grafana SLO panel | "Observability penuh — Prometheus + Grafana" |
| 2:35–3:00 | Terminal k6 summary | "1 juta txn/jam, P95 4,7 ms, 0% error" |
| 3:00–3:10 | GitHub repo + closing | Link github.com/Ghaida00/Capstone |

---

## Placeholder YouTube embed

Setelah upload, ganti `VIDEO_ID` di `wordpress-blocks.html`:

```html
<iframe src="https://www.youtube.com/embed/VIDEO_ID" title="Peakload Demo" allowfullscreen></iframe>
```
