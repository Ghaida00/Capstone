"""Curate LK4 PDF extracts into named dissemination assets."""
from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LK4 = ROOT / "assets" / "lk4"
OUT = ROOT / "assets" / "curated"

# Explicit source file per curated name (do not use "largest on page" — pages mix screenshots).
EXPLICIT: dict[str, str] = {
    "dashboard-metrics.png": "page27-img01.png",   # ops dashboard, live charts
    "dashboard-submit.png": "page28-img01.png",    # manual actions — send tx
    "postman-202.png": "page29-img01.png",         # POST /transactions → 202
    "status-completed.png": "page29-img02.png",    # GET status → completed
    "docker-containers.png": "page30-img02.png",   # Docker Desktop containers
    "grafana-dashboard.png": "page30-img01.png",   # Grafana throughput/latency
    "prometheus-targets.png": "page31-img01.png",  # Prometheus targets UP
    "github-repo.png": "page31-img02.png",         # GitHub repo tree
    "demo-overview-1.png": "page41-img01.png",
    "demo-overview-2.png": "page41-img02.png",
    "repository-docs.png": "page43-img01.png",
    "repository-docs-2.png": "page43-img02.png",
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    copied: list[tuple[str, str, int]] = []
    missing: list[str] = []

    for name, src_name in EXPLICIT.items():
        src = LK4 / src_name
        if not src.is_file():
            missing.append(f"{name} <= {src_name}")
            continue
        dst = OUT / name
        shutil.copy2(src, dst)
        copied.append((name, src_name, src.stat().st_size))

    print(f"Copied {len(copied)} curated images to {OUT}")
    for name, src_name, size in copied:
        print(f"  {name} <= {src_name} ({size // 1024} KB)")
    if missing:
        print("Missing sources:")
        for line in missing:
            print(f"  ! {line}")


if __name__ == "__main__":
    main()
