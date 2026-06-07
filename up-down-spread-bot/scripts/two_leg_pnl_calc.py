#!/usr/bin/env python3
"""Two-leg (first + second entry) settlement PnL under Polymarket rules."""
S1, S2 = 5.0, 1.0
TOTAL = S1 + S2
THR = 0.85

print("=" * 72)
print(f"Config: leg1=${S1:.0f} leg2=${S2:.0f} total=${TOTAL:.0f} | 2nd leg when P1>{THR}")
print("Polymarket: winner redeems $1/share, loser $0")
print("=" * 72)
print()

p1_be = S1 / TOTAL
p2_be = S2 / TOTAL
print("[Breakeven prices]")
print(f"  1st leg wins (profit):  P1 < {p1_be:.4f}")
print(f"  2nd leg wins (profit):  P2 < {p2_be:.4f}")
print(f"  2nd entry needs P1 > {THR} but breakeven needs P1 < {p1_be:.4f} -> no guaranteed win")
print()

print("[When 2nd entry triggers: P1 > 0.85, spread=1.00]")
hdr = f"{'P1':>8} {'P2':>8} {'sh1':>7} {'sh2':>7} {'1st win':>10} {'2nd win':>10} {'both+':>6}"
print(hdr)
print("-" * 72)
for p1 in [0.86, 0.87, 0.88, 0.89, 0.90, 0.91, 0.92]:
    p2 = round(1.0 - p1, 2)
    if p2 <= 0:
        continue
    c1 = round(S1 / p1, 2)
    c2 = round(S2 / p2, 2)
    pnl1 = round(S1 / p1 - TOTAL, 2)
    pnl2 = round(S2 / p2 - TOTAL, 2)
    both = "Y" if pnl1 > 0 and pnl2 > 0 else "N"
    print(f"{p1:>8.2f} {p2:>8.2f} {c1:>7.2f} {c2:>7.2f} {pnl1:>+10.2f} {pnl2:>+10.2f} {both:>6}")

print()
print("[Spread effect at P1=0.90]")
for spread in [0.99, 1.00, 1.02, 1.05]:
    p1 = 0.90
    p2 = round(spread - p1, 2)
    if p2 <= 0:
        continue
    print(f"  spread={spread:.2f} P2={p2:.2f} | 1st win {S1/p1-TOTAL:+.2f} | 2nd win {S2/p2-TOTAL:+.2f}")

print()
print("[Your live cases]")
for p1, p2 in [(0.90, 0.09), (0.87, 0.12)]:
    print(f"  P1={p1} P2={p2}: 1st wins {S1/p1-TOTAL:+.2f} | 2nd wins {S2/p2-TOTAL:+.2f}")

print()
print("[Single leg only $5, no 2nd entry (P1 0.77-0.81)]")
print(f"{'P1':>8} {'shares':>7} {'if win':>10} {'if lose':>10}")
for p1 in [0.77, 0.78, 0.79, 0.80, 0.81]:
    c = round(S1 / p1, 2)
    print(f"{p1:>8.2f} {c:>7.2f} {c-S1:>+10.2f} {-S1:>+10.2f}")

print()
print("[Reverse: P1=0.90 P2=0.09 — S2/S1 ratio for both outcomes positive]")
p1, p2 = 0.90, 0.09
for ratio in [0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0]:
    s2 = S1 * ratio
    t = S1 + s2
    pf = S1 / p1 - t
    ph = s2 / p2 - t
    flag = "YES" if pf > 0 and ph > 0 else "no"
    print(f"  S2=${s2:.1f} (ratio={ratio:.1f}) | 1st {pf:+.2f} | 2nd {ph:+.2f} | both+ {flag}")
