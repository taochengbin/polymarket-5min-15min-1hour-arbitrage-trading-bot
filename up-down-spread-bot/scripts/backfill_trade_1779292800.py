"""One-off: restore normal UP leg + fix flip_reverse row for btc-updown-5m-1779292800."""
import json
import time
from pathlib import Path

SLUG = "btc-updown-5m-1779292800"
PATH = Path(__file__).resolve().parents[1] / "logs" / "late_v3_btc" / "trades.jsonl"

ET_NORMAL = 1779292981.0
UP_CONTRACTS = 5.62
DOWN_CONTRACTS = 8.2
UP_ASK_HEDGE = 0.4
DOWN_ASK_HEDGE = 0.61


def main() -> None:
    rows = []
    for line in PATH.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))

    normal = None
    rev = None
    rest = []
    for r in rows:
        if r.get("market_slug") != SLUG:
            rest.append(r)
            continue
        if r.get("entry_reason") == "flip_reverse":
            rev = r
        elif r.get("entry_reason") in ("normal", "", None):
            normal = r

    ts_normal = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ET_NORMAL))
    if normal is None:
        normal = {
            "record_key": f"{SLUG}#normal#{int(ET_NORMAL * 1000)}",
            "market_slug": SLUG,
            "coin": "btc",
            "bet_side": "UP",
            "spot_at_entry": 0,
            "token_ask": 0.89,
            "contracts": UP_CONTRACTS,
            "size_usd": 5.0,
            "total_cost": 5.0,
            "up_shares": UP_CONTRACTS,
            "down_shares": 0.0,
            "entry_time": ET_NORMAL,
            "entry_timestamp": ts_normal,
            "close_time": ET_NORMAL,
            "is_open": True,
            "bet_result_label": "持仓",
            "exit_label": "持仓中",
            "exit_reason": None,
            "exit_type": None,
            "bet_won": None,
            "settlement_winner": None,
            "settlement_pending": True,
            "spot_start": 0,
            "spot_end": 0,
            "btc_start": 0,
            "btc_final": 0,
            "btc_end": 0,
            "pnl": round(UP_CONTRACTS * UP_ASK_HEDGE - 5.0, 2),
            "pnl_usd": round(UP_CONTRACTS * UP_ASK_HEDGE - 5.0, 2),
            "price_source": "pending",
            "entry_reason": "normal",
            "entry_label": "",
        }

    if rev is None:
        raise SystemExit(f"No flip_reverse row found for {SLUG}")

    rev = dict(rev)
    rev["total_cost"] = 5.0
    rev["size_usd"] = 5.0
    rev["up_shares"] = 0.0
    rev["down_shares"] = DOWN_CONTRACTS
    rev["contracts"] = DOWN_CONTRACTS
    leg_pnl = round(DOWN_CONTRACTS * DOWN_ASK_HEDGE - 5.0, 2)
    rev["pnl"] = leg_pnl
    rev["pnl_usd"] = leg_pnl
    et_rev = float(rev.get("entry_time") or 1779293021.5661893)
    rev["record_key"] = rev.get("record_key") or f"{SLUG}#flip_reverse#{int(et_rev * 1000)}"
    rev["is_open"] = bool(normal.get("is_open", True))

    patched = [normal, rev] + rest
    patched.sort(
        key=lambda r: float(r.get("entry_time") or r.get("close_time") or 0),
        reverse=True,
    )
    tmp = PATH.with_suffix(".jsonl.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for r in patched:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(PATH)
    print(f"OK normal {ts_normal} pnl={normal['pnl']}")
    print(f"OK rev   {rev.get('entry_timestamp')} pnl={rev['pnl']}")


if __name__ == "__main__":
    main()
