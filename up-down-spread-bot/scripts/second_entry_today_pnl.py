#!/usr/bin/env python3
"""Stats for second_entry trades on a given calendar day."""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG = ROOT / "logs" / "late_v3_btc" / "trades.jsonl"


def pnl(row: dict) -> float:
    return float(row.get("pnl_usd", row.get("pnl", 0)) or 0)


def row_date(row: dict) -> str:
    ts = row.get("entry_timestamp") or ""
    if len(ts) >= 10:
        return ts[:10]
    et = float(row.get("entry_time") or 0)
    if et > 0:
        return datetime.fromtimestamp(et).strftime("%Y-%m-%d")
    return ""


def main() -> None:
    day = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    log_path = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_LOG
    if not log_path.exists():
        print(f"File not found: {log_path}")
        sys.exit(1)

    rows = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    second = [
        r
        for r in rows
        if (r.get("entry_reason") or "") == "second_entry" and row_date(r) == day
    ]

    closed = [r for r in second if not r.get("is_open") and not r.get("settlement_pending")]
    open_rows = [r for r in second if r.get("is_open")]
    pending = [r for r in second if r.get("settlement_pending")]

    closed_pnl = sum(pnl(r) for r in closed)
    all_pnl = sum(pnl(r) for r in second)
    wins = [r for r in closed if pnl(r) > 0]
    losses = [r for r in closed if pnl(r) < 0]
    zeros = [r for r in closed if pnl(r) == 0]
    invested = sum(float(r.get("size_usd") or r.get("total_cost") or 0) for r in closed)

    print("=" * 64)
    print(f"Second entry PnL — {day} ({log_path.parent.name})")
    print("=" * 64)
    print(f"Count: {len(second)}")
    print(f"Settled: {len(closed)} | Open: {len(open_rows)} | Pending: {len(pending)}")
    print(f"Settled PnL: ${closed_pnl:+.2f}")
    print(f"All rows PnL (incl. open): ${all_pnl:+.2f}")
    if closed:
        wr = len(wins) / len(closed) * 100
        roi = closed_pnl / invested * 100 if invested else 0
        print(
            f"W/L/Flat: {len(wins)}/{len(losses)}/{len(zeros)} | "
            f"Win rate {wr:.1f}% | Invested ${invested:.2f} | ROI {roi:.1f}%"
        )

    by_result: dict[str, list] = defaultdict(list)
    for r in closed:
        by_result[r.get("bet_result_label") or "?"].append(r)
    if by_result:
        print("\nBy result:")
        for label, sub in sorted(by_result.items(), key=lambda x: -sum(pnl(r) for r in x[1])):
            print(f"  {label}: {len(sub)} trades, ${sum(pnl(r) for r in sub):+.2f}")

    by_side = Counter((r.get("bet_side") or "?").upper() for r in closed)
    if closed:
        print("\nBy side:")
        for side in ("UP", "DOWN"):
            sub = [r for r in closed if (r.get("bet_side") or "").upper() == side]
            if sub:
                print(f"  {side}: {len(sub)} trades, ${sum(pnl(r) for r in sub):+.2f}")

    print("\nSettled detail:")
    for r in sorted(closed, key=lambda x: x.get("entry_timestamp") or ""):
        ts = (r.get("entry_timestamp") or "")[:16]
        slug = (r.get("market_slug") or "")[-12:]
        ask = float(r.get("token_ask") or r.get("entry_ask") or 0)
        cost = float(r.get("size_usd") or r.get("total_cost") or 0)
        print(
            f"  {ts} | {r.get('bet_side', '?'):4} @ {ask:.2f} | "
            f"${cost:.2f} | {pnl(r):+.2f} | {r.get('bet_result_label', '')} | ...{slug}"
        )

    unfinished = open_rows + pending
    if unfinished:
        print("\nOpen / pending:")
        for r in unfinished:
            ts = (r.get("entry_timestamp") or "")[:16]
            print(
                f"  {ts} | {r.get('bet_side')} | mark ${pnl(r):+.2f} | "
                f"open={r.get('is_open')} pending={r.get('settlement_pending')}"
            )


if __name__ == "__main__":
    main()
