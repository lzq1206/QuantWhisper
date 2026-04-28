from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUMMARY = ROOT / "site" / "data" / "summary.json"
MARKET = ROOT / "site" / "data" / "market_snapshot.json"
TRADE = ROOT / "site" / "data" / "trade_latest_rebalance.csv"
ALGOS = ROOT / "site" / "data" / "algorithms.json"


def pct(v):
    try:
        return f"{float(v) * 100:.2f}%"
    except Exception:
        return "—"


def num(v):
    try:
        return f"{float(v):.2f}"
    except Exception:
        return "—"


def main() -> None:
    data = json.loads(SUMMARY.read_text(encoding="utf-8"))
    s = data["strategy"]
    holdings = data["latest_holdings"]["holdings"][:5]
    market = {}
    if MARKET.exists():
        try:
            market = json.loads(MARKET.read_text(encoding="utf-8"))
        except Exception:
            market = {}

    trade_rows = []
    if TRADE.exists():
        try:
            import csv
            with TRADE.open('r', encoding='utf-8-sig', newline='') as f:
                trade_rows = list(csv.DictReader(f))
        except Exception:
            trade_rows = []

    algo_hub = {}
    if ALGOS.exists():
        try:
            algo_hub = json.loads(ALGOS.read_text(encoding="utf-8"))
        except Exception:
            algo_hub = {}

    lines = [
        "QuantWhisper 日更虚拟盘已更新",
        f"策略：{s['strategy']}",
        f"年化收益：{pct(s['annual_return'])} | 年化超额：{pct(s['annual_excess_return'])}",
        f"最大回撤：{pct(s['max_drawdown'])} | 夏普：{num(s['sharpe'])} | IR：{num(s['information_ratio'])}",
        f"最新持仓日期：{data['latest_holdings']['date']}（Top {len(holdings)}）",
    ]
    if market:
        provider = market.get("provider", "unknown")
        count = market.get("count", 0)
        bench = market.get("benchmark", {}) or {}
        bench_name = bench.get("name", "基准")
        bench_chg = pct(bench.get("pct_chg")) if bench else "—"
        lines += [
            f"最新行情源：{provider}（{count} 条）",
            f"基准：{bench_name} {bench_chg}",
        ]
    if trade_rows:
        latest_trade_date = trade_rows[0].get('rebalance_date')
        lines += [
            f"最新调仓：{latest_trade_date}（Top {min(5, len(trade_rows))}）",
            "最新调仓明细：",
        ]
        for row in trade_rows[:5]:
            lines.append(
                f"- {row.get('action')} {row.get('stkcd')} | Δw={float(row.get('weight_change', 0)) * 100:.3f}% | 价={row.get('close_price', '—')} | 量={row.get('trade_volume', '—')}"
            )
    if algo_hub.get("algorithms"):
        algos = algo_hub["algorithms"]
        lines += [
            "",
            f"📚 算法研究库 （{len(algos)} 个策略）：",
        ]
        for a in algos[:3]:
            lines.append(f"- {a.get('algorithm_name','—')}：{a.get('paper_title','—')[:40]}")
    lines += ["", "Top 持仓："]
    for h in holdings:
        lines.append(f"- {h['stkcd']}: {float(h['weight']) * 100:.3f}%")

    lines += [
        "",
        "看板： https://lzq1206.github.io/QuantWhisper/",
        "仓库： https://github.com/lzq1206/QuantWhisper",
    ]
    print("\n".join(lines))


if __name__ == "__main__":
    main()
