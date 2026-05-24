#!/usr/bin/env python3
import json
from collections import Counter, defaultdict
from pathlib import Path

p = Path(__file__).resolve().parent.parent / "logs" / "late_v3_btc" / "trades.jsonl"
rows = [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]
today = max((r.get("entry_timestamp", "")[:10] for r in rows if r.get("entry_timestamp")), default="")

wins = [
    r
    for r in rows
    if (r.get("entry_timestamp") or "")[:10] == today and float(r.get("pnl") or r.get("pnl_usd") or 0) > 0
]
all_today = [r for r in rows if (r.get("entry_timestamp") or "")[:10] == today]

print("TODAY", today)
print("wins", len(wins), "total_pnl", round(sum(float(r.get("pnl") or 0) for r in wins), 2))
print("all_today", len(all_today), "net", round(sum(float(r.get("pnl") or 0) for r in all_today), 2))

by_exit: dict = defaultdict(list)
for r in wins:
    ex = r.get("exit_reason") or ("open" if r.get("is_open") else "unknown")
    by_exit[ex].append(r)

for ex, sub in sorted(by_exit.items(), key=lambda x: -sum(float(r.get("pnl") or 0) for r in x[1])):
    pnl = sum(float(r.get("pnl") or 0) for r in sub)
    avg = pnl / len(sub)
    avg_ask = sum(float(r.get("token_ask") or 0) for r in sub) / len(sub)
    print(f"\n=== {ex} | n={len(sub)} | +{pnl:.2f} | avg +{avg:.2f} | ask {avg_ask:.3f} ===")
    print(" sides", dict(Counter(r.get("bet_side") for r in sub)))
    for r in sorted(sub, key=lambda x: -float(x.get("pnl") or 0)):
        print(
            " ",
            (r.get("entry_timestamp") or "")[11:16],
            (r.get("entry_reason") or "normal")[:10],
            r.get("bet_side"),
            f"ask {float(r.get('token_ask') or 0):.2f}",
            r.get("bet_result_label"),
            f"+{float(r.get('pnl') or 0):.2f}",
            "winner",
            r.get("settlement_winner"),
            (r.get("market_slug") or "")[-10:],
        )

zero = [r for r in all_today if float(r.get("pnl") or 0) == 0]
print("\n--- zero pnl", len(zero))
