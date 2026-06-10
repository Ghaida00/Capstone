# Exhibition Readiness — Profile Matrix & Regression Hunting

Panduan ini menggantikan Fase 0 “full 15m × 4 sel” yang memakan **>1 jam**.  
Gunakan **tier bertingkat**: cepat untuk berburu regresi harian, penuh hanya untuk profil demo sebelum pameran.

---

## Profil & kebutuhan host

| Profil | Shard | RAM host (min) | Modular switch |
|--------|-------|----------------|----------------|
| **A** | 2 | ~5 GB | `cp .env.profile-a.example .env` |
| **B** | 1 | ~5 GB | `cp .env.profile-b.example .env` |
| **C** | 2 (high-mem) | ~7 GB | `cp .env.profile-c.example .env` |
| **D** | 3 | ~9 GB | `cp .env.profile-d.example .env` |

**Modular = benar:** ganti `.env` + `docker compose up`.  
**Wajib `down -v`** hanya saat **jumlah shard berubah** (B↔A↔D). Antar A dan C cukup ganti `.env` (sama 2-shard).

---

## Tier testing (pilih sesuai waktu)

| Tier | Perintah | Durasi/profil | Kapan |
|------|----------|---------------|-------|
| **T0 — Post-reboot** | health + `k6/setup-probe.js` | ~3 menit | Setiap nyalakan laptop |
| **T1 — Gate** | `K6_GATE=1` (lihat bawah) | ~8 menit | Matrix A/B/C/D, berburu regresi |
| **T2 — Full cert** | `load-test-1m-sample-e2e.js` tanpa gate | ~18 menit | 1× profil demo sebelum pameran |
| **T3 — Soak** | `k6/soak-test-1m-sample-e2e.js` | 8 jam | Opsional; bukan gate pameran |

### Gate mode (~5 menit k6)

Script yang sama, fase sustained dipendekkan (3m vs 13m). Threshold SLO **identik** dengan full run.

```powershell
k6 run -e K6_GATE=1 k6/load-test-1m-sample-e2e.js
```

---

## Setelah restart laptop (T0 + T1 profil demo)

Profil demo default: **B** (1-shard, paling stabil di Docker Desktop).

```powershell
cd "C:\Users\Ikhsa\Downloads\MINE\Semester 6\Gh\Capstone"

# 1. Pastikan port 8080 tidak diblokir Windows
netsh interface ipv4 show excludedportrange protocol=tcp

# 2. Matrix gate profil demo saja (~10 menit total)
.\diag\profile-matrix.ps1 -Profiles B -Tier gate

# Atau manual:
cp .env.profile-b.example .env
docker compose up -d --wait
curl.exe -s http://localhost:8080/health
k6 run k6/setup-probe.js
k6 run -e K6_GATE=1 k6/load-test-1m-sample-e2e.js
```

**PASS jika:** semua threshold hijau, **nol** `no live upstreams` di log nginx.

```powershell
docker logs peakload-nginx 2>&1 | findstr "no live upstreams"
```

---

## Matrix 4 profil (~45–70 menit, bukan 4×15m)

Orchestrator otomatis: salin `.env`, cold boot hanya saat topology berubah, simpan log.

```powershell
# Gate semua profil (disarankan sebelum pameran, 1–2 hari sebelumnya)
.\diag\profile-matrix.ps1 -Profiles A,B,C,D -Tier gate

# Hanya verifikasi ulang B setelah fix AI lain
.\diag\profile-matrix.ps1 -Profiles B -Tier gate

# Sertifikasi penuh profil demo (15m)
.\diag\profile-matrix.ps1 -Profiles B -Tier full
```

Output: `diag/matrix-<timestamp>/summary.csv` + log per profil.

**Urutan otomatis:** A → B → C → D (cold boot di B dan D karena ganti shard count).

---

## Checklist production-ready / plug-and-play

Dicentang **2026-06-10** dari T2 full matrix `diag/matrix-20260610-101440`
(commit 4f948cb, CPU limit baru `PG_HAPROXY_CPU_LIMIT=0.5` /
`PGBOUNCER_CPU_LIMIT=0.3`, laptop 16-core/16 GB):

### Modular deploy
- [x] `cp .env.profile-<X>.example .env` + ganti password — stack naik tanpa edit compose (4 profil, cold boot via orchestrator)
- [x] `docker compose up -d --wait` — semua healthy tanpa retry manual (`health=OK` di summary.csv keempat sel)
- [x] `k6/setup-probe.js` — POST + consumer e2e OK (setiap sel matrix)

### SLO gate (T1 gate atau T2 full)
- [x] `http_req_failed{scenario:sustained_1m_per_hour}` < 5% — A/B/C 0.00%, D 0.008%
- [x] `transaction_e2e_ms` p95 < 3s, p99 < 5s — p95 1.62–2.33s, p99 2.57–4.28s
- [x] `transactions_e2e_timeout` di bawah batas script — A=3, B=0, C=2, D=0 (batas full: 55)
- [x] POST p99 < 150ms, balance p95 < 10ms — semua profil; balance p95 2.69–5.38ms

### Edge & pipeline
- [x] nginx: **0** baris `no live upstreams` selama k6 (`nginx_upstream_errors=0` keempat sel)
- [x] RabbitMQ `transactions.process` → 0 setelah load (e2e sampel terminal semua; intake pending max=1)
- [x] `transactions_intake_pending` turun ke ~0 setelah load — snapshot Prometheus per sel: max 1

### Regresi silang profil (T1 matrix + T2 full)
- [x] Profile B: 0% http error (baseline demo) — balance p95 2.69ms, e2e p99 3.40s
- [x] Profile A/C: throughput ~260 tx/s, error 0% — ~234 rb tx per sel @260/s
- [x] Profile D: **lulus full cert** di host 16 GB (e2e p99 3.98s gate / 4.28s full, 0 timeout) — label “lab only” tidak diperlukan lagi

### Hasil sertifikasi T2 full (matrix-20260610-101440, 15 menit/sel)

| Profil | tx dibuat | balance p95 | e2e p50 / p95 / p99 | e2e timeout | error |
|--------|-----------|-------------|----------------------|-------------|-------|
| A (2-shard) | 234.262 @260/s | 2,71 ms | 1,12 s / 2,33 s / 3,66 s | 3 | 0 |
| B (1-shard) | 234.324 @260/s | 2,69 ms | 1,01 s / 1,62 s / 3,40 s | 0 | 0 |
| C (2-shard hi-mem) | 234.324 @260/s | 5,38 ms | 1,04 s / 1,95 s / 2,57 s | 2 | 0 |
| D (3-shard) | 234.195 @260/s | 3,75 ms | 1,22 s / 2,24 s / 4,28 s | 0 | 41 (0,008%) |

Catatan D: 41 error = satu burst ~1,5 detik (stall level host / memory
pressure Docker Desktop) yang dilepas bersih oleh pool-acquire timeout 1 s —
tanpa kaskade, nginx 0 upstream error. Lihat risk register untuk detail.

---

## Interpretasi regresi

| Gejala | Kemungkinan | Bukti |
|--------|-------------|-------|
| k6 5% fail, create 94% | nginx 502 | `docker logs peakload-nginx \| findstr upstream` |
| e2e p95 > 3s, timeout sedikit | intake/consumer backlog | `transactions_intake_pending`, queue RabbitMQ |
| Tail latency menumpuk di ~100 ms (atau kelipatannya) | CFS throttling — `cpus:` limit < demand kontainer | `nr_throttled` di `/sys/fs/cgroup/cpu.stat` kontainer; probe direct-vs-proxy `diag/read-tail-probe.sh` |
| Burst 500 ~1 detik serentak lintas kontainer | host pause (memory pressure Docker Desktop) | log app "pool timed out" + Patroni conn-reset di detik yang sama |
| Port 8080 bind error | Windows excluded range | `netsh … excludedportrange` |

---

## Estimasi waktu

| Skenario | Waktu |
|----------|-------|
| Post-reboot T0+T1 profil B | **~10 menit** |
| Gate A+B+C+D (script) | **~45–70 menit** |
| Full 15m × 4 profil (lama) | **~2+ jam** ← hindari kecuali audit formal |
| Full 15m × profil demo saja | **~20 menit** |

---

## Referensi

- Plan regresi detail: `docs/superpowers/plans/2026-06-08-profile-b-regression.md`
- Perbandingan B vs C: `diag/B-vs-C-comparison.md`
- k6 thresholds: `k6/load-test-1m-sample-e2e.js`
- Sampling saat load: `diag/sample-stats.sh` (Git Bash/WSL)
