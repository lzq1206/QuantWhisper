from __future__ import annotations

import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

# Optional override for local use:
#   QUANTWHISPER_SOURCE_DIR=/path/to/new/source python scripts/sync_source_snapshot.py
# In GitHub Actions this is typically left unset, so the committed snapshot is used.

REQUIRED = [
    "best_config.json",
    "comparison_metrics.csv",
    "monthly_returns_best.csv",
    "daily_nav_vs_benchmark_near2y_available.csv",
    "holdings_snapshots_best.csv",
]


def main() -> None:
    source_dir = os.environ.get("QUANTWHISPER_SOURCE_DIR", "").strip()
    if not source_dir:
        print("QUANTWHISPER_SOURCE_DIR not set; using committed snapshot.")
        return

    src = Path(source_dir).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Source dir does not exist: {src}")

    DATA.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED:
        shutil.copy2(src / name, DATA / name)
    print(f"Synced snapshot from {src} -> {DATA}")


if __name__ == "__main__":
    main()
