from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO_REPORTS = ROOT.parent / "project" / "reports" / "EXP-0004"
SOURCE = ROOT / "data"
SOURCE_ASSETS = ROOT / "assets"
SITE = ROOT / "site"
SITE_DATA = SITE / "data"
SITE_ASSETS = SITE / "assets"

FILES_TO_COPY = [
    "best_config.json",
    "comparison_metrics.csv",
    "monthly_returns_best.csv",
    "daily_nav_current.csv",
    "daily_nav_vs_benchmark_near2y_available.csv",
    "holdings_snapshots_best.csv",
    "trade_ledger.csv",
    "trade_latest_rebalance.csv",
    "trade_latest_rebalance.json",
    "arxiv_papers.json",
    "algorithms.json",
    "paper_backtests.json",
]

ASSETS_TO_COPY = [
    "daily_nav_vs_benchmark_near2y_available.png",
    "daily_nav_compare_exp0003v2_exp0004_same_chart.png",
    "daily_nav_consistent_with_monthly_EXP0004_2024_2025.png",
    "daily_nav_vs_benchmark_2024_2025_partial.png",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def copy_files() -> None:
    ensure_dir(SITE_DATA)
    ensure_dir(SITE_ASSETS)
    for name in FILES_TO_COPY:
        src = SOURCE / name
        if src.exists():
            shutil.copy2(src, SITE_DATA / name)
    for name in ASSETS_TO_COPY:
        src = REPO_REPORTS / name
        if not src.exists():
            src = SOURCE_ASSETS / name
        if src.exists():
            shutil.copy2(src, SITE_ASSETS / name)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def compute_latest_holdings(rows: list[dict[str, str]], top_n: int = 20) -> dict:
    latest_date = max(r["date"] for r in rows)
    latest = [r for r in rows if r["date"] == latest_date]
    latest_sorted = sorted(latest, key=lambda r: float(r.get("weight", 0) or 0), reverse=True)[:top_n]
    return {
        "date": latest_date,
        "top_n": top_n,
        "count": len(latest),
        "holdings": [
            {
                "stkcd": r["stkcd"],
                "weight": float(r["weight"]),
            }
            for r in latest_sorted
        ],
    }


def build_summary() -> None:
    best_config = json.loads((SOURCE / "best_config.json").read_text(encoding="utf-8"))
    metrics = load_csv(SOURCE / "comparison_metrics.csv")
    monthly = load_csv(SOURCE / "monthly_returns_best.csv")
    nav_path = SOURCE / "daily_nav_current.csv"
    if not nav_path.exists():
        nav_path = SOURCE / "daily_nav_vs_benchmark_near2y_available.csv"
    nav = load_csv(nav_path)
    holdings = load_csv(SOURCE / "holdings_snapshots_best.csv")
    market_snapshot = None
    market_snapshot_path = SITE_DATA / "market_snapshot.json"
    if market_snapshot_path.exists():
        try:
            market_snapshot = json.loads(market_snapshot_path.read_text(encoding="utf-8"))
        except Exception:
            market_snapshot = None

    trade_latest = []
    trade_latest_path = SOURCE / "trade_latest_rebalance.csv"
    if trade_latest_path.exists():
        trade_latest = load_csv(trade_latest_path)
    trade_history_rows = []
    trade_history_path = SOURCE / "trade_ledger.csv"
    if trade_history_path.exists():
        trade_history_rows = load_csv(trade_history_path)
    paper_payload = {}
    if (SOURCE / "arxiv_papers.json").exists():
        paper_payload = json.loads((SOURCE / "arxiv_papers.json").read_text(encoding="utf-8"))
    algo_payload = {}
    if (SOURCE / "algorithms.json").exists():
        algo_payload = json.loads((SOURCE / "algorithms.json").read_text(encoding="utf-8"))
    backtest_payload = {}
    if (SOURCE / "paper_backtests.json").exists():
        backtest_payload = json.loads((SOURCE / "paper_backtests.json").read_text(encoding="utf-8"))

    summary = {
        "generated_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "project": "QuantWhisper",
        "strategy": best_config,
        "comparison_metrics": metrics,
        "monthly_points": len(monthly),
        "nav_points": len(nav),
        "latest_holdings": compute_latest_holdings(holdings),
        "market_snapshot": market_snapshot,
        "latest_trade_rebalance": trade_latest[:10],
        "trade_ledger_rows": len(trade_history_rows),
        "automation": {
            "pages_url": "https://lzq1206.github.io/QuantWhisper/",
            "repo_url": "https://github.com/lzq1206/QuantWhisper",
            "mode": "static snapshot + scheduled rebuild hook",
        },
        "algorithm_hub": {
            "papers_count": paper_payload.get("count", 0),
            "algorithms_count": algo_payload.get("count", 0),
            "backtests_count": backtest_payload.get("count", 0),
            "papers_generated_at": paper_payload.get("generated_at"),
            "algorithms_generated_at": algo_payload.get("generated_at"),
            "backtests_generated_at": backtest_payload.get("generated_at"),
        },
        "notes": [
            "GitHub Pages dashboard for the EXP-0004 virtual portfolio.",
            "Data are copied from project/reports/EXP-0004.",
            "If QUANTWHISPER_SOURCE_DIR is set locally, sync_source_snapshot.py can refresh the snapshot before build.",
            "Latest行情会优先从 AkShare 抓取，失败时自动降级到 Baostock。",
        ],
    }
    summary_json = json.dumps(summary, ensure_ascii=False, indent=2)
    (SITE_DATA / "summary.json").write_text(summary_json, encoding="utf-8")
    # Also write to data/ so GitHub Pages can serve it directly from main branch.
    (SOURCE / "summary.json").write_text(summary_json, encoding="utf-8")

    # Convenience compact table of the latest 20 holdings.
    latest = summary["latest_holdings"]
    holdings_rows = [
        {"stkcd": h["stkcd"], "weight": h["weight"]}
        for h in latest["holdings"]
    ]
    holdings_csv_lines: list[str] = []
    import io
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["stkcd", "weight"])
    w.writeheader()
    w.writerows(holdings_rows)
    holdings_csv_text = buf.getvalue()
    with (SITE_DATA / "latest_holdings.csv").open("w", encoding="utf-8", newline="") as f:
        f.write(holdings_csv_text)
    # Also write to data/ for direct serving.
    with (SOURCE / "latest_holdings.csv").open("w", encoding="utf-8", newline="") as f:
        f.write(holdings_csv_text)

    # Build a small changelog-style manifest.
    manifest = {
        "files": FILES_TO_COPY + ["latest_holdings.csv", "summary.json"],
        "assets": ASSETS_TO_COPY,
        "generated_at": summary["generated_at"],
    }
    (SITE / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ensure_dir(SITE)
    copy_files()
    build_summary()


if __name__ == "__main__":
    main()
