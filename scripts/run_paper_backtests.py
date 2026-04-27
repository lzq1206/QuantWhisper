from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SITE_DATA = ROOT / "site" / "data"
ALGOS_PATH = DATA / "algorithms.json"
OUTPUT = "paper_backtests.json"
TURNOVER_COST = 0.0005


def ensure_dirs() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    SITE_DATA.mkdir(parents=True, exist_ok=True)


def iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def load_nav_rows() -> list[dict[str, str]]:
    nav_path = DATA / "daily_nav_current.csv"
    if not nav_path.exists():
        nav_path = DATA / "daily_nav_vs_benchmark_near2y_available.csv"
    if not nav_path.exists():
        return []
    with nav_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def rolling_mean(values: list[float], end_idx: int, n: int) -> float:
    start = max(0, end_idx - n + 1)
    chunk = values[start : end_idx + 1]
    return sum(chunk) / len(chunk) if chunk else 0.0


def rolling_std(values: list[float], end_idx: int, n: int) -> float:
    start = max(0, end_idx - n + 1)
    chunk = values[start : end_idx + 1]
    if len(chunk) <= 1:
        return 0.0
    m = sum(chunk) / len(chunk)
    var = sum((x - m) ** 2 for x in chunk) / (len(chunk) - 1)
    return math.sqrt(max(var, 0.0))


def signal_for_family(family: str, rets: list[float], i: int) -> float:
    prev = rets[i - 1] if i > 0 else 0.0
    if family == "momentum":
        return 1.0 if rolling_mean(rets, i, 5) > 0 else 0.0
    if family == "mean_reversion":
        return 1.0 if rolling_mean(rets, i, 3) < 0 else 0.0
    if family == "vol_breakout":
        vol = rolling_std(rets, i, 20)
        if abs(prev) < vol:
            return 0.0
        return 1.0 if prev > 0 else -1.0
    if family == "rl_timing":
        return 1.0 if rolling_mean(rets, i, 20) > 0 else -1.0
    if family == "transformer_factor":
        trend = rolling_mean(rets, i, 10)
        noise = rolling_std(rets, i, 10)
        return 1.0 if trend > noise * 0.2 else 0.0
    score = rolling_mean(rets, i, 10) - rolling_std(rets, i, 10) * 0.5
    return 1.0 if score > 0 else 0.0


def calc_metrics(nav: list[float], daily_rets: list[float], positions: list[float]) -> dict:
    if not nav:
        return {}
    total_ret = nav[-1] - 1.0
    n = len(nav)
    ann = (nav[-1] ** (252 / max(n, 1))) - 1 if nav[-1] > 0 else -1.0
    peak = nav[0]
    max_dd = 0.0
    for v in nav:
        peak = max(peak, v)
        if peak > 0:
            max_dd = min(max_dd, (v / peak) - 1.0)
    mean_r = sum(daily_rets) / len(daily_rets) if daily_rets else 0.0
    if len(daily_rets) > 1:
        var = sum((r - mean_r) ** 2 for r in daily_rets) / (len(daily_rets) - 1)
        std = math.sqrt(max(var, 0.0))
    else:
        std = 0.0
    sharpe = (mean_r / std * math.sqrt(252)) if std > 0 else 0.0
    win_rate = (sum(1 for r in daily_rets if r > 0) / len(daily_rets)) if daily_rets else 0.0
    turnover = sum(abs(positions[i] - positions[i - 1]) for i in range(1, len(positions)))
    return {
        "total_return": total_ret,
        "annual_return": ann,
        "max_drawdown": max_dd,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "turnover": turnover,
    }


def run_single_backtest(family: str, dates: list[str], bench_rets: list[float], bench_nav: list[float]) -> dict:
    nav = [1.0]
    day_rets = []
    positions = [0.0]
    pos_prev = 0.0
    for i in range(1, len(bench_rets)):
        pos = signal_for_family(family, bench_rets, i)
        trade_cost = TURNOVER_COST * abs(pos - pos_prev)
        ret = pos * bench_rets[i] - trade_cost
        nav.append(nav[-1] * (1.0 + ret))
        day_rets.append(ret)
        positions.append(pos)
        pos_prev = pos
    series = [
        {"date": dates[i], "strategy_nav": nav[i], "benchmark_nav": bench_nav[i]}
        for i in range(min(len(nav), len(dates), len(bench_nav)))
    ]
    return {"metrics": calc_metrics(nav, day_rets, positions), "series": series}


def main() -> int:
    ensure_dirs()
    if not ALGOS_PATH.exists():
        payload = {"generated_at": iso_now(), "count": 0, "backtests": [], "errors": ["algorithms.json not found"]}
        txt = json.dumps(payload, ensure_ascii=False, indent=2)
        (DATA / OUTPUT).write_text(txt, encoding="utf-8")
        (SITE_DATA / OUTPUT).write_text(txt, encoding="utf-8")
        return 0

    rows = load_nav_rows()
    if len(rows) < 3:
        payload = {"generated_at": iso_now(), "count": 0, "backtests": [], "errors": ["insufficient nav rows"]}
        txt = json.dumps(payload, ensure_ascii=False, indent=2)
        (DATA / OUTPUT).write_text(txt, encoding="utf-8")
        (SITE_DATA / OUTPUT).write_text(txt, encoding="utf-8")
        return 0

    dates = [str(r.get("date", "")) for r in rows]
    bench_nav = [float(r.get("bench_nav", 1) or 1) for r in rows]
    bench_rets = [0.0]
    for i in range(1, len(bench_nav)):
        prev = bench_nav[i - 1]
        now = bench_nav[i]
        bench_rets.append((now / prev - 1.0) if prev else 0.0)

    algorithms_payload = json.loads(ALGOS_PATH.read_text(encoding="utf-8"))
    algorithms = algorithms_payload.get("algorithms", [])
    backtests: list[dict] = []
    for a in algorithms:
        family = str(a.get("family", "multi_factor"))
        result = run_single_backtest(family, dates, bench_rets, bench_nav)
        backtests.append(
            {
                "algorithm_id": a.get("algorithm_id"),
                "paper_id": a.get("paper_id"),
                "algorithm_name": a.get("algorithm_name"),
                "family": family,
                "window": {"start": dates[0], "end": dates[-1], "points": len(dates)},
                "metrics": result["metrics"],
                "series": result["series"],
                "updated_at": iso_now(),
            }
        )

    payload = {"generated_at": iso_now(), "count": len(backtests), "errors": [], "backtests": backtests}
    txt = json.dumps(payload, ensure_ascii=False, indent=2)
    (DATA / OUTPUT).write_text(txt, encoding="utf-8")
    (SITE_DATA / OUTPUT).write_text(txt, encoding="utf-8")
    print(json.dumps({"count": len(backtests)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
