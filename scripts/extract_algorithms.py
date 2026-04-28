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


FAMILY_STEPS: dict[str, list[str]] = {
    "rl_timing": [
        "设计状态空间：价格动量、波动率、持仓状态等特征向量",
        "设计奖励函数：差分夏普比或风险调整收益，避免短视行为",
        "搭建 Actor-Critic / PPO 网络并在历史数据中预训练",
        "在滚动扩张窗口中离线回测并评估动作频次与成本敏感性",
        "上线后持续监控状态分布漂移，触发再训练阈值自动重训",
    ],
    "momentum": [
        "计算多周期（5/20/60日）价格动量与成交量确认信号",
        "截面标准化信号并设定多空分位数阈值（如前20%/后20%）",
        "按信号强度分配权重，加入换手成本惩罚项平滑调仓",
        "在滚动1年窗口外样本验证并测试不同市场环境稳健性",
        "监控信号自相关与因子拥挤度，及时调整持仓上限",
    ],
    "mean_reversion": [
        "用协整检验（ADF/Johansen）筛选高相关配对或篮子",
        "构建价差 Z-Score，设定入场（±2σ）与止损（±3σ）边界",
        "动态估算半衰期，自适应调整持仓周期与再平衡频率",
        "压力测试配对相关性断裂场景（如政策冲击、停牌事件）",
        "监控价差持续偏离时间，超过均值回复窗口则强制平仓",
    ],
    "vol_breakout": [
        "用 GARCH(1,1) 或已实现波动率估算当日预期波动区间",
        "价格突破波动率倍数带（如 ±1.5σ）时触发方向性入场",
        "以 ATR 为止损基准，动态调整仓位使组合波动率锚定目标",
        "在高波动期间（VIX>30 或市场下跌>5%）切换为防御仓位",
        "回测不同波动率制度（低/中/高）下策略表现并分段报告",
    ],
    "transformer_factor": [
        "构造输入序列：过去 N 日（价格、成交量、财务因子）面板",
        "设计 Transformer 编码器提取跨时序与截面注意力特征",
        "在训练集上端到端训练预测收益率排名并用 IC 评估因子",
        "用 Purged K-Fold 避免标签泄漏，严格样本外验证",
        "定期重训（季度/月度）并监控因子 IC 衰减与注意力权重漂移",
    ],
    "multi_factor": [
        "从价值、动量、质量、低波动四大维度构建原始因子集",
        "用 IC_IR 加权或机器学习打分合成综合 Alpha 因子",
        "截面排序分组回测，验证因子单调性与分组收益差异显著性",
        "加入因子中性化（行业/市值）减少系统风险暴露",
        "定期更新因子权重并监控因子有效期，淘汰衰减因子",
    ],
}

FAMILY_NEXT_ACTIONS: dict[str, list[str]] = {
    "rl_timing": [
        "对比 PPO、SAC、TD3 三类算法在该市场的样本外表现",
        "引入交易成本感知奖励函数，减少过度交易",
        "加入风险预算约束，防止单日最大亏损超限",
        "扩展多智能体框架，同时管理多个标的的仓位决策",
    ],
    "momentum": [
        "测试时序动量与截面动量组合叠加效果",
        "加入成交量加权动量（VWAP momentum）提升信号稳定性",
        "引入动量崩溃（momentum crash）风控：高β时降杠杆",
        "扩展到行业/指数增强层面，与个股选择策略结合",
    ],
    "mean_reversion": [
        "扩展配对至行业内所有股票，用机器学习自动筛选配对",
        "引入高频微观结构特征，提高价差预测精度",
        "对比基于协整的线性配对与机器学习配对效果",
        "增加价差序列状态机（趋势/均值回归）自动切换逻辑",
    ],
    "vol_breakout": [
        "对比简单历史波动率与 GARCH 模型在不同市场的预测效果",
        "加入隐含波动率（期权数据）进一步提升信号质量",
        "测试不同突破阈值（1σ/1.5σ/2σ）对胜率与盈亏比的影响",
        "扩展到商品期货跨品种，利用波动率协方差优化组合权重",
    ],
    "transformer_factor": [
        "对比 LSTM、GRU 与 Transformer 在该因子任务上的 IC",
        "引入基本面文本特征（财报摘要 embedding）增强多模态输入",
        "探索稀疏注意力与局部注意力机制降低计算开销",
        "加入对抗训练提升因子对市场制度变换的鲁棒性",
    ],
    "multi_factor": [
        "补充数据清洗规则：停牌、涨跌停、缺失值填充",
        "加入交易成本敏感性分析（双边费率/滑点）",
        "增加风控开关：最大回撤阈值、波动率限仓",
        "扩展到多市场对比（沪深股票与股指/商品期货）",
    ],
}


def build_steps(family: str, name: str) -> list[str]:
    return FAMILY_STEPS.get(family, [
        f"定义 {name} 的可交易标的池（股票/期货）与样本区间",
        "按论文描述构建核心因子或状态变量并标准化",
        "设置信号阈值与仓位映射（含手续费与滑点）",
        "在滚动窗口中训练/校准并进行样本外验证",
        "输出净值、回撤、夏普与换手率并持续监控稳定性",
    ])


def build_next_actions(family: str) -> list[str]:
    return FAMILY_NEXT_ACTIONS.get(family, [
        "补充数据清洗规则（停牌、涨跌停、缺失值）",
        "加入交易成本敏感性分析（双边费率/滑点）",
        "增加风控开关（最大回撤阈值、波动率限仓）",
        "扩展到多市场对比（沪深股票与股指/商品期货）",
    ])


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
                "execution_steps": build_steps(family, algo_name),
                "risk_controls": ["单标的权重上限", "组合最大回撤限制", "极端波动降杠杆"],
                "next_actions": build_next_actions(family),
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
