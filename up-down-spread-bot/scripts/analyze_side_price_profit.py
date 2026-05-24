#!/usr/bin/env python3
"""Analyze UP/DOWN profitability by entry price range from trades.jsonl."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG = ROOT / "logs" / "late_v3_btc" / "trades.jsonl"

BUCKETS: List[Tuple[float, float]] = [
    (0.50, 0.60),
    (0.60, 0.70),
    (0.70, 0.75),
    (0.75, 0.80),
    (0.80, 0.85),
    (0.85, 0.90),
    (0.90, 0.95),
]


def load_closed_normal(path: Path) -> List[dict]:
    rows: List[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        if r.get("is_open"):
            continue
        if (r.get("entry_reason") or "normal") != "normal":
            continue
        rows.append(r)
    return rows


def stat(sub: List[dict]) -> tuple:
    if not sub:
        return 0, 0.0, 0.0, 0.0, 0.0
    n = len(sub)
    wins = sum(1 for r in sub if r.get("bet_result_label") == "押中")
    pnl = sum(float(r.get("pnl") or 0) for r in sub)
    avg_ask = sum(float(r.get("token_ask") or 0) for r in sub) / n
    win_rows = [r for r in sub if r.get("bet_result_label") == "押中"]
    loss_rows = [r for r in sub if r.get("bet_result_label") == "未中"]
    avg_win = sum(float(r.get("pnl") or 0) for r in win_rows) / len(win_rows) if win_rows else 0.0
    avg_loss = sum(float(r.get("pnl") or 0) for r in loss_rows) / len(loss_rows) if loss_rows else 0.0
    return n, wins / n * 100, pnl, avg_ask, avg_win if win_rows else 0.0


def bucket_label(lo: float, hi: float) -> str:
    return f"{lo:.2f}-{hi:.2f}"


def main() -> None:
    log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LOG
    rows = load_closed_normal(log_path)
    print(f"数据源: {log_path}")
    print(f"已平仓首单: {len(rows)} 笔\n")

    print("=== 按方向 + 入场价 (token_ask) ===")
    hdr = (
        f"{'区间':<12} {'UP_n':>5} {'UP_wr':>7} {'UP_pnl':>9} "
        f"{'DN_n':>5} {'DN_wr':>7} {'DN_pnl':>9} {'合计':>9}"
    )
    print(hdr)
    print("-" * len(hdr))

    cells: List[tuple] = []
    for lo, hi in BUCKETS:
        up = [r for r in rows if r.get("bet_side") == "UP" and lo <= float(r.get("token_ask") or 0) < hi]
        dn = [r for r in rows if r.get("bet_side") == "DOWN" and lo <= float(r.get("token_ask") or 0) < hi]
        un, wu, pu, _, _ = stat(up)
        dn_n, wd, pd, _, _ = stat(dn)
        if un + dn_n == 0:
            continue
        print(
            f"{bucket_label(lo, hi):<12} {un:>5} {wu:>6.1f}% {pu:>+9.2f} "
            f"{dn_n:>5} {wd:>6.1f}% {pd:>+9.2f} {pu + pd:>+9.2f}"
        )
        for side, sub, pnl, wr, cnt in [("UP", up, pu, wu, un), ("DOWN", dn, pd, wd, dn_n)]:
            if cnt >= 3:
                cells.append((side, bucket_label(lo, hi), cnt, wr, pnl))

    print("\n=== 方向汇总 ===")
    for side in ("UP", "DOWN"):
        sub = [r for r in rows if r.get("bet_side") == side]
        n, wr, pnl, avg_ask, _ = stat(sub)
        wins = [r for r in sub if r.get("bet_result_label") == "押中"]
        losses = [r for r in sub if r.get("bet_result_label") == "未中"]
        aw = sum(float(r.get("pnl") or 0) for r in wins) / len(wins) if wins else 0
        al = sum(float(r.get("pnl") or 0) for r in losses) / len(losses) if losses else 0
        print(
            f"{side}: n={n} 胜率={wr:.1f}% PnL={pnl:+.2f} "
            f"均价={avg_ask:.3f} 均赢={aw:+.2f} 均输={al:+.2f}"
        )

    print("\n=== 细粒度 0.05 步长 (>=2笔) ===")
    fine: List[tuple] = []
    for side in ("UP", "DOWN"):
        for i in range(50, 94, 5):
            lo, hi = i / 100, (i + 5) / 100
            sub = [
                r
                for r in rows
                if r.get("bet_side") == side and lo <= float(r.get("token_ask") or 0) < hi
            ]
            if len(sub) < 2:
                continue
            _, wr, pnl, _, _ = stat(sub)
            fine.append((pnl, side, bucket_label(lo, hi), len(sub), wr))
    fine.sort(key=lambda x: x[0], reverse=True)
    print("最赚钱 TOP8:")
    for pnl, side, rng, cnt, wr in fine[:8]:
        print(f"  {side} @{rng} n={cnt} wr={wr:.0f}% pnl={pnl:+.2f}")
    print("最亏钱 BOT8:")
    for pnl, side, rng, cnt, wr in fine[-8:]:
        print(f"  {side} @{rng} n={cnt} wr={wr:.0f}% pnl={pnl:+.2f}")

    print("\n=== 推荐区间 (n>=3, 按PnL排序) ===")
    cells.sort(key=lambda x: x[4], reverse=True)
    profitable = [c for c in cells if c[4] > 0]
    losing = [c for c in cells if c[4] <= 0]
    print("盈利区间:")
    for side, rng, cnt, wr, pnl in profitable[:6]:
        print(f"  {side} ask {rng} | {cnt}笔 胜率{wr:.1f}% PnL={pnl:+.2f}")
    print("亏损区间:")
    for side, rng, cnt, wr, pnl in sorted(losing, key=lambda x: x[4])[:6]:
        print(f"  {side} ask {rng} | {cnt}笔 胜率{wr:.1f}% PnL={pnl:+.2f}")

    # breakeven win rate by ask
    print("\n=== 盈亏平衡胜率 (按入场价) ===")
    for ask in (0.65, 0.70, 0.75, 0.80, 0.85, 0.90):
        win_pnl = 5.0 / ask - 5.0
        loss_pnl = -5.0
        be = abs(loss_pnl) / (win_pnl + abs(loss_pnl)) * 100
        print(f"  ask={ask:.2f} 赢+{win_pnl:.2f}/输{loss_pnl:.2f} -> 需胜率>={be:.1f}%")


if __name__ == "__main__":
    main()
