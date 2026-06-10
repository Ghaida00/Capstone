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

Centang sebelum pameran (profil demo = B):

### Modular deploy
- [ ] `cp .env.profile-<X>.example .env` + ganti password — stack naik tanpa edit compose
- [ ] `docker compose up -d --wait` — semua healthy tanpa retry manual
- [ ] `k6/setup-probe.js` — POST + consumer e2e OK

### SLO gate (T1 gate atau T2 full)
- [ ] `http_req_failed{scenario:sustained_1m_per_hour}` < 5%
- [ ] `transaction_e2e_ms` p95 < 3s, p99 < 5s
- [ ] `transactions_e2e_timeout` di bawah batas script
- [ ] POST p99 < 150ms, balance p95 < 10ms (Profile B)

### Edge & pipeline
- [ ] nginx: **0** baris `no live upstreams` selama k6
- [ ] RabbitMQ `transactions.process` → 0 setelah load
- [ ] `transactions_intake_pending` turun ke ~0 setelah load (Grafana `:3001`)

### Regresi silang profil (T1 matrix)
- [ ] Profile B: 0% http error (baseline demo)
- [ ] Profile A/C: throughput ~260 tx/s, error < 5%
- [ ] Profile D: lulus gate jika RAM host ≥ 12 GB; jika tidak, dokumentasikan “lab only”

---

## Interpretasi regresi

| Gejala | Kemungkinan | Bukti |
|--------|-------------|-------|
| k6 5% fail, create 94% | nginx 502 | `docker logs peakload-nginx \| findstr upstream` |
| e2e p95 > 3s, timeout sedikit | intake/consumer backlog | `transactions_intake_pending`, queue RabbitMQ |
| C/D e2e lambat, B OK | fsync disk shared (laptop) | `diag/sample-stats.sh`, B-vs-C doc |
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
