#!/usr/bin/env python3
"""
Two-leg settlement PnL when:
  - 1st leg FILLED at P1_entry in [0.77, 0.85]
  - 2nd leg triggers when favorite ask > 0.85 (buys opposite at P2_trigger)
"""
S1, S2 = 5.0, 1.0
TOTAL = S1 + S2
THR = 0.85

print("=" * 78)
print("Premise: 1st fill P1_entry in [0.77, 0.85]; 2nd leg when live ask > 0.85")
print(f"Leg1=${S1:.0f} Leg2=${S2:.0f} Total=${TOTAL:.0f}")
print("=" * 78)

print("\n[1] First leg win PnL depends on ENTRY price (not trigger ask)")
print(f"{'P1_entry':>10} {'shares':>8} {'if 1st wins':>12} {'both+ w/ P2=0.13':>18}")
for p1e in [0.77, 0.78, 0.79, 0.80, 0.81, 0.82, 0.83, 0.84, 0.85]:
    sh = S1 / p1e
    pnl1 = sh - TOTAL
    pnl2 = S2 / 0.13 - TOTAL
    both = "YES" if pnl1 > 0 and pnl2 > 0 else "no"
    print(f"{p1e:>10.2f} {sh:>8.2f} {pnl1:>+12.2f} {both:>18}")

print("\n[2] Hedge win PnL depends on P2 AT 2nd entry (favorite already >0.85)")
print(f"{'P1_live':>10} {'P2_hedge':>10} {'sh2':>8} {'if 2nd wins':>12}")
for p1_live in [0.86, 0.87, 0.88, 0.89, 0.90, 0.91, 0.92]:
    for spread in [1.00]:
        p2 = round(spread - p1_live, 2)
        if p2 <= 0:
            continue
        print(f"{p1_live:>10.2f} {p2:>10.2f} {S2/p2:>8.2f} {S2/p2-TOTAL:>+12.2f}")

print("\n[3] Full matrix: P1_entry x P2_at_2nd_entry (settlement, no early exit)")
hdr = f"{'P1_entry':>8} | " + " ".join(f"P2={p:.2f}" for p in [0.15, 0.14, 0.13, 0.12, 0.11, 0.10])
print(hdr)
print("-" * 78)
for p1e in [0.77, 0.79, 0.81, 0.83, 0.85]:
    row1 = f"{p1e:>8.2f} |"
    row2 = "         |"
    for p2 in [0.15, 0.14, 0.13, 0.12, 0.11, 0.10]:
        pf = S1 / p1e - TOTAL
        ph = S2 / p2 - TOTAL
        flag = "Y" if pf > 0 and ph > 0 else "N"
        row1 += f" 1st{pf:+.1f}"
        row2 += f" 2nd{ph:+.1f}"
    print(row1 + "  (1st leg win $)")
    print(row2 + f"  both+ check row above")
    # compact both+ line
    flags = []
    for p2 in [0.15, 0.14, 0.13, 0.12, 0.11, 0.10]:
        pf = S1 / p1e - TOTAL
        ph = S2 / p2 - TOTAL
        flags.append("Y" if pf > 0 and ph > 0 else "N")
    print(f"{'both+?':>8} | " + " ".join(f"  {f}   " for f in flags))
    print()

print("[4] Typical path: entry 0.80, later favorite rises to 0.87, hedge at 0.13")
p1e, p2 = 0.80, 0.13
print(f"  1st leg win (UP):  {S1/p1e - TOTAL:+.2f}")
print(f"  2nd leg win (DOWN): {S2/p2 - TOTAL:+.2f}")
print(f"  Both positive? {S1/p1e > TOTAL and S2/p2 > TOTAL}")

print("\n[5] Entry at top of range 0.85, trigger at 0.90 / hedge 0.10")
p1e, p2 = 0.85, 0.10
print(f"  1st leg win: {S1/p1e - TOTAL:+.2f}")
print(f"  2nd leg win: {S2/p2 - TOTAL:+.2f}")

print("\n[6] Breakeven P1_entry for 1st-leg-win profit: P1_entry < {:.4f}".format(S1 / TOTAL))
print("    In [0.77,0.85]: entries 0.77-0.83 -> 1st win profit; 0.84-0.85 -> 1st win small loss")
