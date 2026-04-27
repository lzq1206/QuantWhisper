from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SITE_DATA = ROOT / "site" / "data"
PAPERS_PATH = DATA / "arxiv_papers.json"
OUTPUT = "algorithms.json"


def ensure_dirs() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    SITE_DATA.mkdir(parents=True, exist_ok=True)


def iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def slugify(text: str) -> str:
    t = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip().lower())
    return re.sub(r"-{2,}", "-", t).strip("-") or "algo"


def infer_family(title: str, summary: str) -> tuple[str, str]:
    blob = f"{title}\n{summary}".lower()
    if "reinforcement" in blob or "rl" in blob:
        return "rl_timing", "强化学习择时策略"
    if "transformer" in blob or "attention" in blob:
        return "transformer_factor", "Transformer 序列因子策略"
    if "mean reversion" in blob or "reversion" in blob:
        return "mean_reversion", "均值回复策略"
    if "momentum" in blob or "trend" in blob:
        return "momentum", "动量趋势策略"
    if "volatility" in blob or "garch" in blob:
        return "vol_breakout", "波动率突破策略"
    return "multi_factor", "多因子评分策略"


def build_steps(name: str) -> list[str]:
    return [
        f"定义 {name} 的可交易标的池（股票/期货）与样本区间",
        "按论文描述构建核心因子或状态变量并标准化",
        "设置信号阈值与仓位映射（含手续费与滑点）",
        "在滚动窗口中训练/校准并进行样本外验证",
        "输出净值、回撤、夏普与换手率并持续监控稳定性",
    ]


def build_next_actions() -> list[str]:
    return [
        "补充数据清洗规则（停牌、涨跌停、缺失值）",
        "加入交易成本敏感性分析（双边费率/滑点）",
        "增加风控开关（最大回撤阈值、波动率限仓）",
        "扩展到多市场对比（沪深股票与股指/商品期货）",
    ]


def main() -> int:
    ensure_dirs()
    if not PAPERS_PATH.exists():
        payload = {"generated_at": iso_now(), "count": 0, "algorithms": [], "errors": ["arxiv_papers.json not found"]}
        txt = json.dumps(payload, ensure_ascii=False, indent=2)
        (DATA / OUTPUT).write_text(txt, encoding="utf-8")
        (SITE_DATA / OUTPUT).write_text(txt, encoding="utf-8")
        return 0

    papers_payload = json.loads(PAPERS_PATH.read_text(encoding="utf-8"))
    papers = papers_payload.get("papers", [])
    algorithms: list[dict] = []
    for p in papers:
        paper_id = str(p.get("paper_id", "")).strip() or slugify(p.get("title", "paper"))
        family, algo_name = infer_family(str(p.get("title", "")), str(p.get("summary", "")))
        algo_id = f"{paper_id}-{family}"
        algorithms.append(
            {
                "algorithm_id": algo_id,
                "paper_id": paper_id,
                "paper_title": p.get("title", ""),
                "paper_url": p.get("url", ""),
                "paper_published_at": p.get("published_at", ""),
                "algorithm_name": algo_name,
                "family": family,
                "thesis": f"基于论文《{p.get('title', '')}》提炼的可执行量化框架。",
                "inputs": ["日频价格序列", "收益率序列", "交易成本参数", "风险约束参数"],
                "execution_steps": build_steps(algo_name),
                "risk_controls": ["单标的权重上限", "组合最大回撤限制", "极端波动降杠杆"],
                "next_actions": build_next_actions(),
            }
        )

    payload = {
        "generated_at": iso_now(),
        "source_papers_generated_at": papers_payload.get("generated_at"),
        "count": len(algorithms),
        "errors": [],
        "algorithms": algorithms,
    }
    txt = json.dumps(payload, ensure_ascii=False, indent=2)
    (DATA / OUTPUT).write_text(txt, encoding="utf-8")
    (SITE_DATA / OUTPUT).write_text(txt, encoding="utf-8")
    print(json.dumps({"count": len(algorithms)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
