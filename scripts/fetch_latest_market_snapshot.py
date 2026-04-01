from __future__ import annotations

import csv
import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SITE_DATA = ROOT / "site" / "data"
WATCHLIST_FILE = DATA / "holdings_snapshots_best.csv"
FALLBACK_CSV = DATA / "market_snapshot_fallback.csv"
FALLBACK_JSON = DATA / "market_snapshot_fallback.json"


@dataclass
class QuoteRow:
    symbol: str
    name: str
    last: float | None
    prev_close: float | None
    pct_chg: float | None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None
    amount: float | None = None
    trade_date: str | None = None
    source: str = ""


def ensure_dirs() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    SITE_DATA.mkdir(parents=True, exist_ok=True)


def norm_code(code: str) -> str:
    raw = str(code).strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return raw
    return digits.zfill(6)


def load_watchlist(top_n: int = 20) -> list[str]:
    if not WATCHLIST_FILE.exists():
        return []
    rows: list[dict[str, str]] = []
    with WATCHLIST_FILE.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return []
    latest_date = max(r["date"] for r in rows)
    latest = [r for r in rows if r["date"] == latest_date]
    latest.sort(key=lambda r: float(r.get("weight", 0) or 0), reverse=True)
    return [norm_code(r["stkcd"]) for r in latest[:top_n]]


def _pick_col(df, *candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_float(v):
    try:
        if v is None:
            return None
        s = str(v).strip().replace(",", "")
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def fetch_with_akshare(symbols: list[str]) -> tuple[list[QuoteRow], dict]:
    import akshare as ak  # type: ignore

    fetched_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    rows: list[QuoteRow] = []
    meta = {"provider": "akshare", "fetched_at": fetched_at, "fallback_used": False, "errors": []}

    spot_df = None
    try:
        spot_df = ak.stock_zh_a_spot_em()
    except Exception as e:
        meta["errors"].append(f"spot_em_failed: {e}")

    code_col = None
    name_col = None
    if spot_df is not None and not spot_df.empty:
        code_col = _pick_col(spot_df, "代码", "symbol", "code")
        name_col = _pick_col(spot_df, "名称", "name")

    today = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

    for sym in symbols:
        row = None
        if spot_df is not None and code_col is not None:
            hit = spot_df[spot_df[code_col].astype(str).str.zfill(6) == sym]
            if not hit.empty:
                r = hit.iloc[0].to_dict()
                row = QuoteRow(
                    symbol=sym,
                    name=str(r.get(name_col, sym)) if name_col else sym,
                    last=_to_float(r.get(_pick_col(hit, "最新价"))) if False else _to_float(r.get("最新价") or r.get("最新价格")),
                    prev_close=_to_float(r.get("昨收")),
                    pct_chg=_to_float(r.get("涨跌幅")),
                    open=_to_float(r.get("今开")),
                    high=_to_float(r.get("最高")),
                    low=_to_float(r.get("最低")),
                    volume=_to_float(r.get("成交量")),
                    amount=_to_float(r.get("成交额")),
                    trade_date=today,
                    source="akshare_spot_em",
                )
        if row is None:
            try:
                hist = ak.stock_zh_a_hist(symbol=sym, period="daily", start_date=start, end_date=today, adjust="qfq")
                if hist is not None and not hist.empty:
                    last_row = hist.iloc[-1].to_dict()
                    row = QuoteRow(
                        symbol=sym,
                        name=sym,
                        last=_to_float(last_row.get("收盘")),
                        prev_close=_to_float(last_row.get("开盘")) if False else None,
                        pct_chg=_to_float(last_row.get("涨跌幅")),
                        open=_to_float(last_row.get("开盘")),
                        high=_to_float(last_row.get("最高")),
                        low=_to_float(last_row.get("最低")),
                        volume=_to_float(last_row.get("成交量")),
                        amount=_to_float(last_row.get("成交额")),
                        trade_date=str(last_row.get("日期") or today),
                        source="akshare_hist_fallback",
                    )
            except Exception as e:
                meta["errors"].append(f"{sym}: {e}")
        if row is not None:
            rows.append(row)

    if not rows:
        raise RuntimeError("AkShare returned no usable quote rows")
    return rows, meta


def _bs_exchange(symbol: str) -> str:
    return "sh" if str(symbol).startswith("6") else "sz"


def fetch_with_baostock(symbols: list[str]) -> tuple[list[QuoteRow], dict]:
    import baostock as bs  # type: ignore

    fetched_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    meta = {"provider": "baostock", "fetched_at": fetched_at, "fallback_used": True, "errors": []}
    login = bs.login()
    if getattr(login, "error_code", "0") != "0":
        meta["errors"].append(f"login_failed: {getattr(login, 'error_msg', 'unknown')}")

    rows: list[QuoteRow] = []
    start = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")
    end = datetime.now().strftime("%Y%m%d")

    for sym in symbols:
        bs_code = f"{_bs_exchange(sym)}.{sym}"
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="3",
            )
            data = []
            while rs.next():
                data.append(rs.get_row_data())
            if data:
                last = data[-1]
                rows.append(
                    QuoteRow(
                        symbol=sym,
                        name=sym,
                        last=_to_float(last[5]),
                        prev_close=_to_float(last[6]),
                        pct_chg=_to_float(last[9]),
                        open=_to_float(last[2]),
                        high=_to_float(last[3]),
                        low=_to_float(last[4]),
                        volume=_to_float(last[7]),
                        amount=_to_float(last[8]),
                        trade_date=last[0],
                        source="baostock_hist",
                    )
                )
        except Exception as e:
            meta["errors"].append(f"{sym}: {e}")

    bs.logout()
    if not rows:
        raise RuntimeError("Baostock returned no usable quote rows")
    return rows, meta


def build_index_quote(source: str, rows: list[QuoteRow]) -> dict:
    # Try to pick a representative benchmark from the quotes.
    # Prefer 上证指数 if present in fetch results, otherwise use the first row.
    candidates = [r for r in rows if r.symbol == "000001"]
    bench = candidates[0] if candidates else (rows[0] if rows else None)
    if bench is None:
        return {}
    return {
        "symbol": bench.symbol,
        "name": "上证指数" if bench.symbol == "000001" else bench.name,
        "last": bench.last,
        "pct_chg": bench.pct_chg,
        "trade_date": bench.trade_date,
        "source": source,
    }


def load_repo_fallback() -> tuple[list[QuoteRow], dict] | None:
    if not (FALLBACK_CSV.exists() and FALLBACK_JSON.exists()):
        return None
    try:
        payload = json.loads(FALLBACK_JSON.read_text(encoding="utf-8"))
        rows = []
        with FALLBACK_CSV.open("r", encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                rows.append(QuoteRow(
                    symbol=r.get("symbol", ""),
                    name=r.get("name", ""),
                    last=_to_float(r.get("last")),
                    prev_close=_to_float(r.get("prev_close")),
                    pct_chg=_to_float(r.get("pct_chg")),
                    open=_to_float(r.get("open")),
                    high=_to_float(r.get("high")),
                    low=_to_float(r.get("low")),
                    volume=_to_float(r.get("volume")),
                    amount=_to_float(r.get("amount")),
                    trade_date=r.get("trade_date"),
                    source=r.get("source", "csmar_fallback"),
                ))
        meta = {
            "provider": payload.get("provider", "csmar_fallback"),
            "fetched_at": payload.get("generated_at") or datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "fallback_used": True,
            "errors": ["AkShare/Baostock unavailable; using committed fallback snapshot."],
        }
        return rows, meta
    except Exception:
        return None


def write_outputs(rows: list[QuoteRow], meta: dict) -> None:
    ensure_dirs()
    csv_path = SITE_DATA / "market_snapshot.csv"
    json_path = SITE_DATA / "market_snapshot.json"

    fieldnames = [
        "symbol",
        "name",
        "last",
        "prev_close",
        "pct_chg",
        "open",
        "high",
        "low",
        "volume",
        "amount",
        "trade_date",
        "source",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(asdict(r))

    payload = {
        "generated_at": meta["fetched_at"],
        "provider": meta["provider"],
        "fallback_used": meta.get("fallback_used", False),
        "errors": meta.get("errors", []),
        "count": len(rows),
        "benchmark": build_index_quote(meta["provider"], rows),
        "rows": [asdict(r) for r in rows],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {csv_path}")
    print(f"Wrote {json_path}")
    print(json.dumps({"provider": meta["provider"], "count": len(rows), "fallback_used": meta.get("fallback_used", False)}, ensure_ascii=False))


def main() -> int:
    symbols = load_watchlist(top_n=20)
    if not symbols:
        print("No watchlist symbols found; exiting without market snapshot.")
        return 0
    if "000001" not in symbols:
        symbols.append("000001")

    try:
        rows, meta = fetch_with_akshare(symbols)
        write_outputs(rows, meta)
        return 0
    except Exception as ak_err:
        print(f"AkShare failed: {ak_err}", file=sys.stderr)
        try:
            rows, meta = fetch_with_baostock(symbols)
            write_outputs(rows, meta)
            return 0
        except Exception as bs_err:
            print(f"Baostock failed: {bs_err}", file=sys.stderr)
            fallback = load_repo_fallback()
            if fallback is not None:
                rows, meta = fallback
                write_outputs(rows, meta)
                return 0
            # Write a minimal status file so the site can show the error.
            ensure_dirs()
            fail = {
                "generated_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                "provider": "none",
                "fallback_used": True,
                "errors": [str(ak_err), str(bs_err)],
                "count": 0,
                "benchmark": {},
                "rows": [],
            }
            (SITE_DATA / "market_snapshot.json").write_text(json.dumps(fail, ensure_ascii=False, indent=2), encoding="utf-8")
            with (SITE_DATA / "market_snapshot.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["symbol", "name", "last", "prev_close", "pct_chg", "open", "high", "low", "volume", "amount", "trade_date", "source"])
                writer.writeheader()
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
