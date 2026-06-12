# Screenshot Manifest — Peakload Dissemination

## Curated assets (`assets/curated/`)

| File | Sumber PDF | Penggunaan di halaman |
|------|------------|----------------------|
| `dashboard-metrics.png` | LK4 p.27 img01 | Fitur Core — ops dashboard live |
| `dashboard-submit.png` | LK4 p.28 img01 | Demo — manual send transaction |
| `postman-202.png` | LK4 p.29 img01 | Cara kerja — POST 202 Accepted |
| `status-completed.png` | LK4 p.29 img02 | Cara kerja — GET status completed |
| `docker-containers.png` | LK4 p.30 img02 | Fitur Resilience / Arsitektur — Docker |
| `grafana-dashboard.png` | LK4 p.30 img01 | Fitur Ops / Hasil — Grafana panels |
| `prometheus-targets.png` | LK4 p.31 img01 | Arsitektur — Prometheus targets |
| `github-repo.png` | LK4 p.31 img02 | Repositori — GitHub tree |
| `demo-overview-1.png` | LK4 p.40 | Lampiran demo |
| `demo-overview-2.png` | LK4 p.41 | Lampiran demo |
| `repository-docs.png` | LK4 p.43 | Dokumentasi repo |
| `repository-docs-2.png` | LK4 p.44 | Dokumentasi repo |

Regenerate curated set:

```bash
python docs/dissemination/scripts/curate_lk4_images.py
```

## Raw extracts (`assets/lk4/`)

102 images extracted from full PDF. Use `curate_lk4_images.py` or pick manually by page prefix (`page25-*`, etc.).

## Still needed (manual)

| Asset | Action |
|-------|--------|
| `hero-peakload.svg` | Included in `assets/hero-peakload.svg` |
| `architecture-overview` | Open `docs/architecture/architecture.html` or render `system-overview.mmd` |
| Video demo | Record per `demo-script.md`, upload YouTube |
| Team photos | Optional — `assets/team/` |

## WordPress upload

Upload `assets/curated/*.png` to Media Library. Replace relative paths in HTML with WordPress media URLs after upload.
