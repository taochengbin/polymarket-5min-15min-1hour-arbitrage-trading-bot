#!/usr/bin/env python3
"""Win rate by entry price (token_ask) bucket."""
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "logs" / "late_v3_btc" / "trades.jsonl"


def load(since: str | None) -> list:
    rows = []
    start = datetime.strptime(since, "%Y-%m-%d %H:%M:%S") if since else None
    for line in LOG.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        ts = (r.get("entry_timestamp") or "")[:19]
        if not ts:
            continue
        if start and datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") < start:
            continue
        rows.append(r)
    return rows


def is_win(r: dict) -> bool | None:
    if r.get("bet_result_label") == "押中":
        return True
    if r.get("bet_result_label") == "未中":
        return False
    if r.get("bet_won") is True:
        return True
    if r.get("bet_won") is False:
        return False
    if r.get("is_open"):
        return None
    pnl = float(r.get("pnl") or 0)
    if r.get("exit_reason") == "settlement":
        return pnl > 0
    return None


def bucket_stats(rows: list, step: float = 0.05) -> list:
    groups: dict = defaultdict(list)
    for r in rows:
        ask = float(r.get("token_ask") or 0)
        if ask <= 0:
            continue
        lo = int(ask / step) * step
        hi = lo + step
        key = (lo, hi)
        groups[key].append(r)

    out = []
    for (lo, hi), sub in sorted(groups.items()):
        decided = [r for r in sub if is_win(r) is not None]
        wins = sum(1 for r in decided if is_win(r))
        losses = len(decided) - wins
        pending = len(sub) - len(decided)
        pnl = sum(float(r.get("pnl") or 0) for r in sub)
        wr = wins / len(decided) * 100 if decided else 0
        out.append(
            {
                "range": f"{lo:.2f}-{hi:.2f}",
                "n": len(sub),
                "decided": len(decided),
                "wins": wins,
                "losses": losses,
                "pending": pending,
                "wr": wr,
                "pnl": pnl,
            }
        )
    return out


def side_bucket(rows: list) -> dict:
    by = {"UP": defaultdict(list), "DOWN": defaultdict(list)}
    for r in rows:
        side = r.get("bet_side")
        ask = float(r.get("token_ask") or 0)
        if side not in by or ask <= 0:
            continue
        lo = int(ask / 0.05) * 0.05
        by[side][(lo, lo + 0.05)].append(r)
    return by


def main() -> None:
    since = sys.argv[1] if len(sys.argv) > 1 else "2026-05-23 06:00:00"
    rows = load(since)
    if not rows:
        print("no rows")
        return

    first = min(r.get("entry_timestamp", "") for r in rows)[:16]
    last = max(r.get("entry_timestamp", "") for r in rows)[:16]
    print(f"窗口: {since} ~ {last}")
    print(f"实际首笔: {first}  共 {len(rows)} 笔\n")

    print("=== 买入价(token_ask) 胜率 ===")
    print(f"{'区间':<12} {'总笔':>4} {'已决':>4} {'胜':>3} {'负':>3} {'胜率':>7} {'PnL':>9}")
    print("-" * 50)
    for s in bucket_stats(rows):
        if s["decided"] == 0 and s["pending"] == 0:
            continue
        wr_s = f"{s['wr']:.1f}%" if s["decided"] else "—"
        print(
            f"{s['range']:<12} {s['n']:>4} {s['decided']:>4} {s['wins']:>3} {s['losses']:>3} "
            f"{wr_s:>7} {s['pnl']:>+9.2f}"
        )

    # wider buckets
    print("\n=== 合并区间 (更易读) ===")
    wide = [(0.60, 0.70), (0.70, 0.75), (0.75, 0.80), (0.80, 0.85), (0.85, 0.90), (0.90, 0.95)]
    print(f"{'区间':<12} {'总笔':>4} {'胜率':>7} {'PnL':>9} {'均PnL':>8}")
    for lo, hi in wide:
        sub = [r for r in rows if lo <= float(r.get("token_ask") or 0) < hi]
        if not sub:
            continue
        decided = [r for r in sub if is_win(r) is not None]
        wins = sum(1 for r in decided if is_win(r))
        wr = wins / len(decided) * 100 if decided else 0
        pnl = sum(float(r.get("pnl") or 0) for r in sub)
        avg = pnl / len(sub)
        print(f"{lo:.2f}-{hi:.2f}   {len(sub):>4} {wr:>6.1f}% {pnl:>+9.2f} {avg:>+8.2f}")

    print("\n=== UP / DOWN 分方向 (0.05步长, 已决>=2) ===")
    by = side_bucket(rows)
    for side in ("UP", "DOWN"):
        print(f"\n{side}:")
        for (lo, hi), sub in sorted(by[side].items()):
            decided = [r for r in sub if is_win(r) is not None]
            if len(decided) < 2:
                continue
            wins = sum(1 for r in decided if is_win(r))
            pnl = sum(float(r.get("pnl") or 0) for r in sub)
            print(
                f"  {lo:.2f}-{hi:.2f}  n={len(sub)} wr={wins/len(decided)*100:.0f}% "
                f"pnl={pnl:+.2f}"
            )


if __name__ == "__main__":
    main()
