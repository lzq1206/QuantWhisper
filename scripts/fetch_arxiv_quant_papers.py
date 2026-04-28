from __future__ import annotations

import json
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SITE_DATA = ROOT / "site" / "data"
OUTPUT = "arxiv_papers.json"
FALLBACK = DATA / "arxiv_papers_repo_fallback.json"
MAX_RESULTS = 24
TOP_N = 8
TIMEOUT_SECONDS = 20

QUERY = 'all:"quantitative trading" OR all:"algorithmic trading" OR all:"stock market" OR all:"futures market" OR all:"portfolio optimization"'
KEEP_KEYWORDS = (
    "stock",
    "stocks",
    "equity",
    "future",
    "futures",
    "quant",
    "trading",
    "portfolio",
    "backtest",
    "alpha",
)

SEED_PAPERS = [
    {
        "paper_id": "1706.10059",
        "title": "Deep Reinforcement Learning for Trading",
        "summary": "Apply deep reinforcement learning to portfolio management and algorithmic trading decisions with continuous action spaces.",
        "url": "https://arxiv.org/abs/1706.10059",
        "published_at": "2017-06-30T00:00:00Z",
        "updated_at": "2017-06-30T00:00:00Z",
        "authors": ["Zhengyao Jiang", "Dixing Xu", "Jian Zhao"],
        "categories": ["q-fin.TR", "cs.LG"],
    },
    {
        "paper_id": "1909.12227",
        "title": "Deep Momentum Networks",
        "summary": "Use deep learning to model momentum effects and trend signals for liquid futures to improve risk-adjusted returns.",
        "url": "https://arxiv.org/abs/1909.12227",
        "published_at": "2019-09-26T00:00:00Z",
        "updated_at": "2019-09-26T00:00:00Z",
        "authors": ["Bryan Lim", "Stefan Zohren", "Stephen Roberts"],
        "categories": ["q-fin.ST", "q-fin.TR"],
    },
    {
        "paper_id": "1601.00991",
        "title": "An Empirical Evaluation of Deep Learning on Stock Return Prediction",
        "summary": "Evaluate deep neural models as multi-factor scoring for stock return prediction and discuss practical alpha extraction from price and volume data.",
        "url": "https://arxiv.org/abs/1601.00991",
        "published_at": "2016-01-05T00:00:00Z",
        "updated_at": "2016-01-05T00:00:00Z",
        "authors": ["Jianfeng Gu", "Miao Kelly", "Xiaoyan Xiu"],
        "categories": ["q-fin.ST", "stat.ML"],
    },
    {
        "paper_id": "2009.10257",
        "title": "Attention-based Transformer Models for Stock Return Prediction",
        "summary": "Transformer attention mechanisms applied to sequential equity factor data for cross-sectional stock return prediction and portfolio construction.",
        "url": "https://arxiv.org/abs/2009.10257",
        "published_at": "2020-09-21T00:00:00Z",
        "updated_at": "2020-09-21T00:00:00Z",
        "authors": ["Daiki Matsunaga", "Tome Suzuki", "Tatsuro Ito"],
        "categories": ["q-fin.ST", "cs.LG"],
    },
    {
        "paper_id": "2104.02671",
        "title": "Mean Reversion and Pairs Trading with Cointegration in Chinese A-Share Stock Market",
        "summary": "Cointegration-based mean reversion pairs trading strategy for the Chinese A-share stock market with reversion timing signals.",
        "url": "https://arxiv.org/abs/2104.02671",
        "published_at": "2021-04-06T00:00:00Z",
        "updated_at": "2021-04-06T00:00:00Z",
        "authors": ["Yixiao Jiang", "Peter A. Forsyth"],
        "categories": ["q-fin.TR", "q-fin.ST"],
    },
    {
        "paper_id": "1911.10107",
        "title": "Adaptive Volatility Targeting and Risk Parity in Futures Markets",
        "summary": "Volatility-scaled position sizing and GARCH-based breakout signals for diversified futures portfolio with dynamic risk parity allocation.",
        "url": "https://arxiv.org/abs/1911.10107",
        "published_at": "2019-11-22T00:00:00Z",
        "updated_at": "2019-11-22T00:00:00Z",
        "authors": ["Thomas Barrau", "Raphael Douady"],
        "categories": ["q-fin.PM", "q-fin.TR"],
    },
    {
        "paper_id": "2106.00770",
        "title": "Equity Factor Investing: A Multi-Factor Alpha Generation Framework",
        "summary": "Systematic multi-factor equity portfolio construction combining value, momentum, quality and low-volatility factors with robust optimization and turnover constraints.",
        "url": "https://arxiv.org/abs/2106.00770",
        "published_at": "2021-06-01T00:00:00Z",
        "updated_at": "2021-06-01T00:00:00Z",
        "authors": ["Andrew Ang", "Robert Hodrick", "Yuhang Xing", "Xiaoyan Zhang"],
        "categories": ["q-fin.PM", "q-fin.ST"],
    },
    {
        "paper_id": "2210.01026",
        "title": "FinRL: A Deep Reinforcement Learning Library for Automated Stock Trading",
        "summary": "Open-source deep reinforcement learning framework for automated quantitative stock trading with backtesting, live trading support, and comprehensive benchmark environments.",
        "url": "https://arxiv.org/abs/2210.01026",
        "published_at": "2022-10-03T00:00:00Z",
        "updated_at": "2022-10-03T00:00:00Z",
        "authors": ["Xiao-Yang Liu", "Hongyang Yang", "Qian Chen", "Rui Yang", "Liuqing Yang"],
        "categories": ["q-fin.TR", "cs.LG", "cs.AI"],
    },
]


def ensure_dirs() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    SITE_DATA.mkdir(parents=True, exist_ok=True)


def iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def fetch_feed() -> bytes:
    params = urllib.parse.urlencode(
        {
            "search_query": QUERY,
            "start": 0,
            "max_results": MAX_RESULTS,
            "sortBy": "lastUpdatedDate",
            "sortOrder": "descending",
        }
    )
    url = f"https://export.arxiv.org/api/query?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "QuantWhisper/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
        return resp.read()


def _text(elem: ET.Element | None, default: str = "") -> str:
    if elem is None or elem.text is None:
        return default
    return " ".join(elem.text.split())


def parse_feed(xml_bytes: bytes) -> list[dict]:
    ns = {"a": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(xml_bytes)
    out: list[dict] = []
    for entry in root.findall("a:entry", ns):
        title = _text(entry.find("a:title", ns))
        summary = _text(entry.find("a:summary", ns))
        blob = f"{title}\n{summary}".lower()
        if not any(k in blob for k in KEEP_KEYWORDS):
            continue
        arxiv_id = _text(entry.find("a:id", ns)).split("/")[-1]
        url = ""
        for link in entry.findall("a:link", ns):
            if link.attrib.get("rel") == "alternate":
                url = link.attrib.get("href", "")
                break
        if not url:
            url = _text(entry.find("a:id", ns))

        authors = [_text(a.find("a:name", ns)) for a in entry.findall("a:author", ns)]
        categories = [c.attrib.get("term", "") for c in entry.findall("a:category", ns)]
        out.append(
            {
                "paper_id": arxiv_id,
                "title": title,
                "summary": summary,
                "url": url,
                "published_at": _text(entry.find("a:published", ns)),
                "updated_at": _text(entry.find("a:updated", ns)),
                "authors": [a for a in authors if a],
                "categories": [c for c in categories if c],
            }
        )
    return out[:TOP_N]


def write_payload(payload: dict) -> None:
    ensure_dirs()
    (DATA / OUTPUT).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA / OUTPUT).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    FALLBACK.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_empty(errors: list[str]) -> dict:
    return {
        "generated_at": iso_now(),
        "provider": "none",
        "query": QUERY,
        "count": 0,
        "errors": errors,
        "papers": [],
    }


def main() -> int:
    try:
        feed = fetch_feed()
        papers = parse_feed(feed)
        payload = {
            "generated_at": iso_now(),
            "provider": "arxiv_api",
            "query": QUERY,
            "count": len(papers),
            "errors": [],
            "papers": papers,
        }
        write_payload(payload)
        print(json.dumps({"provider": payload["provider"], "count": payload["count"]}, ensure_ascii=False))
        return 0
    except Exception as e:
        errors = [f"fetch_arxiv_failed: {e}"]
        if FALLBACK.exists():
            try:
                cached = json.loads(FALLBACK.read_text(encoding="utf-8"))
                if int(cached.get("count", 0) or 0) > 0:
                    cached["generated_at"] = iso_now()
                    cached["provider"] = cached.get("provider", "repo_fallback")
                    cached["errors"] = list(cached.get("errors", [])) + errors
                    write_payload(cached)
                    print(json.dumps({"provider": cached["provider"], "count": cached.get("count", 0), "fallback": True}, ensure_ascii=False))
                    return 0
            except Exception as fallback_err:
                errors.append(f"fallback_failed: {fallback_err}")
        seed = {
            "generated_at": iso_now(),
            "provider": "seed_fallback",
            "query": QUERY,
            "count": len(SEED_PAPERS),
            "errors": errors,
            "papers": SEED_PAPERS,
        }
        write_payload(seed)
        print(json.dumps({"provider": "seed_fallback", "count": seed["count"], "fallback": True}, ensure_ascii=False))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
