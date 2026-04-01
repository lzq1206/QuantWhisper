from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUMMARY = ROOT / "site" / "data" / "summary.json"


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

    lines = [
        "QuantWhisper 日更虚拟盘已更新",
        f"策略：{s['strategy']}",
        f"年化收益：{pct(s['annual_return'])} | 年化超额：{pct(s['annual_excess_return'])}",
        f"最大回撤：{pct(s['max_drawdown'])} | 夏普：{num(s['sharpe'])} | IR：{num(s['information_ratio'])}",
        f"最新持仓日期：{data['latest_holdings']['date']}（Top {len(holdings)}）",
        "",
        "Top 持仓：",
    ]
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
