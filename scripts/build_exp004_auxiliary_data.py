from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT.parent / "project" / "reports" / "EXP-0004"
EXT = ROOT.parent / "outputs" / "exp004_v2_p0_20260324"
DATA = ROOT / "data"
CSMAR_DIR = Path("/mnt/nas_share/OpenClaw/QA/CSMAR")

NAV_OLD = PROJECT / "daily_nav_vs_benchmark_near2y_available.csv"
NAV_EXT = EXT / "exp004_tradeability_boardaware_daily_compare_v1_20260324.csv"
HOLDINGS = PROJECT / "holdings_snapshots_best.csv"

CSMAR_FILES = [
    CSMAR_DIR / "TRD_Dalyr_2020_Ashare.csv",
    CSMAR_DIR / "TRD_Dalyr_2021_Ashare.csv",
    CSMAR_DIR / "TRD_Dalyr_2022_Ashare.csv",
    CSMAR_DIR / "TRD_Dalyr_2023_Ashare.csv",
    CSMAR_DIR / "TRD_Dalyr_2024_Ashare.csv",
    CSMAR_DIR / "TRD_Dalyr_2025_Ashare.csv",
    CSMAR_DIR / "TRD_Dalyr_2026Q1_Ashare.csv",
]

AUM_REF = 1_000_000.0


@dataclass
class TradeRow:
    rebalance_date: str
    action: str
    stkcd: str
    weight_change: float
    new_weight: float
    old_weight: float
    ref_price: float | None
    ref_trade_notional: float
    ref_trade_shares: float | None
    market_volume: float | None
    market_amount: float | None
    source: str


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_current_nav() -> Path:
    old_rows = load_csv(NAV_OLD)
    ext_rows = load_csv(NAV_EXT)
    if not old_rows or not ext_rows:
        raise RuntimeError("NAV source files are empty")

    last_old = old_rows[-1]
    prev_strat = float(last_old["strategy_nav"])
    prev_bench = float(last_old["bench_nav"])

    combined = []
    for r in old_rows:
        combined.append({
            "date": r["date"],
            "strategy_ret": float(r["strategy_ret"]),
            "bench_ret": float(r["bench_ret"]),
            "strategy_nav": float(r["strategy_nav"]),
            "bench_nav": float(r["bench_nav"]),
            "segment": "mainline_2024_2025",
        })

    # The 2026 extension is built from exp004 tradeability compare data.
    for r in ext_rows:
        date = r["date"]
        strat_ret = float(r["ret_exp004"])
        bench_ret = float(r["ret_sf_plain"])
        prev_strat *= (1 + strat_ret)
        prev_bench *= (1 + bench_ret)
        combined.append({
            "date": date,
            "strategy_ret": strat_ret,
            "bench_ret": bench_ret,
            "strategy_nav": prev_strat,
            "bench_nav": prev_bench,
            "segment": "extension_2026Q1",
        })

    out = DATA / "daily_nav_current.csv"
    ensure_dir(DATA)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date", "strategy_ret", "bench_ret", "strategy_nav", "bench_nav", "segment"])
        w.writeheader()
        w.writerows(combined)
    return out


def load_holdings_snapshots() -> list[dict[str, str]]:
    rows = load_csv(HOLDINGS)
    rows.sort(key=lambda r: (r["date"], r["stkcd"]))
    return rows


def build_trade_ledger() -> Path:
    rows = load_holdings_snapshots()
    if not rows:
        raise RuntimeError("No holdings snapshots found")

    by_date: dict[str, dict[str, float]] = defaultdict(dict)
    dates = sorted({r["date"] for r in rows})
    for r in rows:
        by_date[r["date"]][str(r["stkcd"]).zfill(6)] = float(r["weight"])

    changes: list[tuple[str, str, str, float, float, float]] = []
    prev_weights: dict[str, float] = {}
    for d in dates:
        cur = by_date[d]
        symbols = set(prev_weights) | set(cur)
        for sym in sorted(symbols):
            old = prev_weights.get(sym, 0.0)
            new = cur.get(sym, 0.0)
            delta = new - old
            if abs(delta) < 1e-12:
                continue
            action = "BUY" if delta > 0 else "SELL"
            changes.append((d, action, sym, delta, new, old))
        prev_weights = cur

    needed_dates = {d for d, *_ in changes}
    needed_symbols = {sym for _, _, sym, *_ in changes}
    quote_index: dict[tuple[str, str], dict[str, str]] = {}

    for p in CSMAR_FILES:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d = row.get("Trddt", "")
                sym = str(row.get("Stkcd", "")).zfill(6)
                if d in needed_dates and sym in needed_symbols and (d, sym) not in quote_index:
                    quote_index[(d, sym)] = row

    out_rows: list[TradeRow] = []
    for d, action, sym, delta, new, old in changes:
        q = quote_index.get((d, sym), {})
        shares = q.get("Dnshrtrd")
        amount = q.get("Dnvaltrd")
        price = None
        volume = None
        market_amount = None
        if shares not in (None, "", "0"):
            try:
                volume = float(shares)
            except Exception:
                volume = None
        if amount not in (None, ""):
            try:
                market_amount = float(amount)
            except Exception:
                market_amount = None
        if volume and market_amount is not None and volume > 0:
            price = market_amount / volume
        elif q.get("Dretwd") is not None:
            price = None
        ref_notional = abs(delta) * AUM_REF
        ref_shares = ref_notional / price if price and price > 0 else None
        out_rows.append(TradeRow(
            rebalance_date=d,
            action=action,
            stkcd=sym,
            weight_change=delta,
            new_weight=new,
            old_weight=old,
            ref_price=price,
            ref_trade_notional=ref_notional,
            ref_trade_shares=ref_shares,
            market_volume=volume,
            market_amount=market_amount,
            source=q.get("source_file", "CSMAR_TRD_Dalyr"),
        ))

    out_csv = DATA / "trade_ledger.csv"
    out_json = DATA / "trade_ledger.json"
    ensure_dir(DATA)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "rebalance_date", "action", "stkcd", "weight_change", "new_weight", "old_weight",
            "ref_price", "ref_trade_notional", "ref_trade_shares", "market_volume", "market_amount", "source"
        ])
        w.writeheader()
        for r in out_rows:
            w.writerow(asdict(r))

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "aum_reference": AUM_REF,
        "rebalance_events": len(dates),
        "trade_rows": len(out_rows),
        "latest_rebalance_date": max(dates),
        "sample_recent_trades": [asdict(r) for r in out_rows[-20:]],
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_csv


def build_market_fallback() -> tuple[Path, Path]:
    rows = load_holdings_snapshots()
    latest_date = max(r["date"] for r in rows)
    latest = [r for r in rows if r["date"] == latest_date]
    latest_symbols = [str(r["stkcd"]).zfill(6) for r in sorted(latest, key=lambda r: float(r["weight"]), reverse=True)[:20]]
    display_symbols = ["000001"] + [s for s in latest_symbols if s != "000001"]
    needed = set(display_symbols)
    latest_quote: dict[str, dict[str, str]] = {}
    latest_trade_date = None

    for p in CSMAR_FILES:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = str(row.get("Stkcd", "")).zfill(6)
                if sym not in needed:
                    continue
                d = row.get("Trddt", "")
                if latest_trade_date is None or d > latest_trade_date:
                    latest_trade_date = d
                old = latest_quote.get(sym)
                if old is None or (d and d > old.get("Trddt", "")):
                    latest_quote[sym] = row

    out_csv = DATA / "market_snapshot_fallback.csv"
    out_json = DATA / "market_snapshot_fallback.json"
    ensure_dir(DATA)
    out_rows = []
    for sym in display_symbols:
        row = latest_quote.get(sym)
        if not row:
            continue
        amount = float(row["Dnvaltrd"]) if row.get("Dnvaltrd") else None
        volume = float(row["Dnshrtrd"]) if row.get("Dnshrtrd") else None
        price = amount / volume if amount and volume else None
        out_rows.append({
            "symbol": sym,
            "name": sym,
            "last": price,
            "prev_close": None,
            "pct_chg": float(row["Dretwd"]) if row.get("Dretwd") else None,
            "open": None,
            "high": None,
            "low": None,
            "volume": volume,
            "amount": amount,
            "trade_date": row.get("Trddt", latest_date),
            "source": "csmar_fallback",
        })

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "name", "last", "prev_close", "pct_chg", "open", "high", "low", "volume", "amount", "trade_date", "source"])
        w.writeheader()
        w.writerows(out_rows)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "provider": "csmar_fallback",
        "fallback_used": True,
        "errors": [],
        "count": len(out_rows),
        "benchmark": out_rows[0] if out_rows else {},
        "rows": out_rows,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_csv, out_json


def main() -> None:
    ensure_dir(DATA)
    nav = build_current_nav()
    trades = build_trade_ledger()
    fallback_csv, fallback_json = build_market_fallback()
    print(nav)
    print(trades)
    print(fallback_csv)
    print(fallback_json)


if __name__ == "__main__":
    main()
