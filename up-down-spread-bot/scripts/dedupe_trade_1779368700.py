"""Remove duplicate flip_reverse trade rows for btc-updown-5m-1779368700."""
import json
from pathlib import Path

SLUG = "btc-updown-5m-1779368700"
PATH = Path(__file__).resolve().parents[1] / "logs" / "late_v3_btc" / "trades.jsonl"
ACTUAL_STOP_LOSS_PNL = -13.18


def main() -> None:
    rows = []
    for line in PATH.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))

    kept_rev = None
    normal = None
    rest = []
    rev_dups = []

    for r in rows:
        if r.get("market_slug") != SLUG:
            rest.append(r)
            continue
        if r.get("entry_reason") == "flip_reverse":
            rev_dups.append(r)
        elif r.get("entry_reason") in ("normal", "", None):
            normal = r

    if not rev_dups:
        print("No flip_reverse rows to dedupe")
        return

    rev_dups.sort(key=lambda r: float(r.get("entry_time") or 0))
    kept_rev = dict(rev_dups[0])
    print(f"Removing {len(rev_dups) - 1} duplicate flip_reverse row(s)")

    leg_pnl = round(ACTUAL_STOP_LOSS_PNL / 2, 2)
    leg_payout = round(5.0 + leg_pnl, 2)
    roi = round(leg_pnl / 5.0 * 100, 3)

    for rec in (normal, kept_rev):
        if not rec:
            continue
        rec["pnl"] = leg_pnl
        rec["pnl_usd"] = leg_pnl
        rec["payout"] = leg_payout
        rec["roi_pct"] = roi

    patched = []
    if kept_rev:
        patched.append(kept_rev)
    if normal:
        patched.append(normal)
    patched.extend(rest)
    patched.sort(
        key=lambda r: float(r.get("entry_time") or r.get("close_time") or 0),
        reverse=True,
    )

    tmp = PATH.with_suffix(".jsonl.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for r in patched:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(PATH)
    print(f"OK: {len(patched)} rows total, slug {SLUG} now has 2 rows (normal + flip_reverse)")


if __name__ == "__main__":
    main()
