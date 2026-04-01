from __future__ import annotations

import csv
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

import baostock as bs

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT.parent / "project" / "reports" / "EXP-0004"
DATA = ROOT / "data"
HOLDINGS = PROJECT / "holdings_snapshots_best.csv"
TOP_N_PER_REBALANCE = 10
AUM_REF = 1_000_000.0


@dataclass
class TradeRow:
    rebalance_date: str
    action: str
    stkcd: str
    weight_change: float
    new_weight: float
    old_weight: float
    close_price: float | None
    trade_volume: float | None
    trade_amount: float | None
    ref_trade_notional: float
    ref_trade_shares: float | None
    source: str


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def compute_changes() -> list[dict]:
    rows = load_csv(HOLDINGS)
    rows.sort(key=lambda r: (r["date"], r["stkcd"]))
    by_date: dict[str, dict[str, float]] = {}
    for r in rows:
        by_date.setdefault(r["date"], {})[str(r["stkcd"]).zfill(6)] = float(r["weight"])

    dates = sorted(by_date)
    prev: dict[str, float] = {}
    out: list[dict] = []
    for d in dates:
        cur = by_date[d]
        symbols = set(prev) | set(cur)
        day_rows = []
        for sym in symbols:
            old = prev.get(sym, 0.0)
            new = cur.get(sym, 0.0)
            delta = new - old
            if abs(delta) < 1e-12:
                continue
            day_rows.append({
                "rebalance_date": d,
                "action": "BUY" if delta > 0 else "SELL",
                "stkcd": sym,
                "weight_change": delta,
                "new_weight": new,
                "old_weight": old,
            })
        day_rows.sort(key=lambda r: abs(r["weight_change"]), reverse=True)
        out.extend(day_rows[:TOP_N_PER_REBALANCE])
        prev = cur
    return out


def fetch_quotes(symbols: list[str], start: str, end: str) -> dict[tuple[str, str], dict]:
    rs_map: dict[tuple[str, str], dict] = {}
    login = bs.login()
    if getattr(login, "error_code", "0") != "0":
        raise RuntimeError(f"baostock login failed: {getattr(login, 'error_msg', 'unknown')}")

    for i, sym in enumerate(symbols, 1):
        code = ("sh" if sym.startswith("6") else "sz") + f".{sym}"
        try:
            rs = bs.query_history_k_data_plus(
                code,
                "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="3",
            )
            while rs.next():
                row = rs.get_row_data()
                rs_map[(row[0], sym)] = {
                    "date": row[0],
                    "code": sym,
                    "close": float(row[5]) if row[5] else None,
                    "volume": float(row[7]) if row[7] else None,
                    "amount": float(row[8]) if row[8] else None,
                    "source": "baostock",
                }
        except Exception:
            continue
    bs.logout()
    return rs_map


def main() -> None:
    ensure_dir(DATA)
    changes = compute_changes()
    if not changes:
        raise RuntimeError("No trade changes computed")

    symbols = sorted({r["stkcd"] for r in changes})
    start = min(r["rebalance_date"] for r in changes)
    end = max(r["rebalance_date"] for r in changes)
    quotes = fetch_quotes(symbols, start, end)

    out_rows: list[TradeRow] = []
    for r in changes:
        q = quotes.get((r["rebalance_date"], r["stkcd"]))
        close_price = q.get("close") if q else None
        volume = q.get("volume") if q else None
        amount = q.get("amount") if q else None
        notional = abs(r["weight_change"]) * AUM_REF
        shares = notional / close_price if close_price and close_price > 0 else None
        out_rows.append(TradeRow(
            rebalance_date=r["rebalance_date"],
            action=r["action"],
            stkcd=r["stkcd"],
            weight_change=r["weight_change"],
            new_weight=r["new_weight"],
            old_weight=r["old_weight"],
            close_price=close_price,
            trade_volume=volume,
            trade_amount=amount,
            ref_trade_notional=notional,
            ref_trade_shares=shares,
            source=(q or {}).get("source", "baostock"),
        ))

    out_csv = DATA / "trade_ledger_top.csv"
    out_json = DATA / "trade_ledger_top.json"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "rebalance_date", "action", "stkcd", "weight_change", "new_weight", "old_weight",
            "close_price", "trade_volume", "trade_amount", "ref_trade_notional", "ref_trade_shares", "source"
        ])
        w.writeheader()
        for r in out_rows:
            w.writerow(asdict(r))

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "aum_reference": AUM_REF,
        "top_n_per_rebalance": TOP_N_PER_REBALANCE,
        "rows": len(out_rows),
        "latest_rebalance_date": max(r.rebalance_date for r in out_rows),
        "sample_last_30": [asdict(r) for r in out_rows[-30:]],
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_csv)
    print(out_json)


if __name__ == "__main__":
    main()
