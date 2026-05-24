#!/usr/bin/env python3
"""Fetch Polymarket settlement for the latest N trades and update trades.jsonl."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_api import clear_outcome_cache, get_official_settlement, market_slug_window_end_ts
from trade_record import apply_chainlink_labels, finalize_settlement


def _load_config() -> dict:
    cfg_path = ROOT / "config" / "config.json"
    if cfg_path.is_file():
        return json.loads(cfg_path.read_text(encoding="utf-8"))
    return {}


def _proxy_url(config: dict) -> str | None:
    pm = config.get("data_sources", {}).get("polymarket", {})
    px = (pm.get("http_proxy") or "").strip()
    return px or None


def _apply_bet_labels(trade: dict, winner: str) -> None:
    side = trade.get("bet_side")
    if side not in ("UP", "DOWN") or winner not in ("UP", "DOWN"):
        trade["bet_won"] = None
        trade["bet_result"] = "pending"
        trade["bet_result_label"] = "待结算"
        return
    trade["bet_won"] = side == winner
    trade["bet_result"] = "win" if trade["bet_won"] else "loss"
    trade["bet_result_label"] = "押中" if trade["bet_won"] else "未中"
    trade["settlement_winner"] = winner
    trade["winner"] = winner
    trade["bet_result_source"] = "polymarket_gamma"


def _apply_chainlink_prices(trade: dict, ptb: float, fp: float) -> None:
    if ptb > 0:
        trade["spot_start"] = round(ptb, 2)
        trade["btc_start"] = trade["spot_start"]
    if fp > 0:
        trade["spot_end"] = round(fp, 2)
        trade["btc_final"] = trade["spot_end"]
        trade["btc_end"] = trade["spot_end"]
    if ptb > 0 and fp > 0:
        trade["price_source"] = "polymarket_chainlink"
    elif trade.get("settlement_winner") in ("UP", "DOWN"):
        trade["price_source"] = "polymarket_gamma"


def refresh_one(trade: dict, *, coin: str, proxy_url: str | None) -> dict:
    slug = str(trade.get("market_slug") or "")
    clear_outcome_cache(slug)
    api = get_official_settlement(slug, timeout=15, proxy_url=proxy_url, use_cache=False)

    if not api.get("success"):
        trade["bet_result_source"] = trade.get("bet_result_source") or "api_unavailable"
        if trade.get("is_open") and not trade.get("exit_reason"):
            trade["bet_result_label"] = trade.get("bet_result_label") or "待结算"
            trade["settlement_pending"] = True
        return trade

    winner = api.get("winner") if api.get("winner") in ("UP", "DOWN") else None
    ptb = float(api.get("price_to_beat") or 0)
    fp = float(api.get("final_price") or 0)
    exit_reason = trade.get("exit_reason")
    past_end = market_slug_window_end_ts(slug) > 0 and time.time() >= market_slug_window_end_ts(slug) + 10
    market_closed = bool(api.get("closed") or api.get("resolved") or past_end)
    has_chainlink = ptb > 0 and fp > 0

    if not winner:
        trade["settlement_pending"] = True
        trade["bet_result_source"] = "polymarket_gamma_pending"
        if trade.get("is_open") and not exit_reason:
            trade["bet_result_label"] = "待结算"
            trade["bet_won"] = None
        return trade

    # --- Early exit: keep realized PnL, only fill official direction + chainlink ---
    if exit_reason in ("stop_loss", "flip_stop", "early_exit"):
        _apply_bet_labels(trade, winner)
        _apply_chainlink_prices(trade, ptb, fp)
        if has_chainlink:
            apply_chainlink_labels(
                trade,
                spot_start=ptb,
                spot_end=fp,
                settlement_winner=winner,
            )
        trade["settlement_pending"] = not has_chainlink
        trade["is_open"] = False
        return trade

    # --- Held to expiry (or still marked open but market already closed on Poly) ---
    if market_closed or exit_reason == "settlement" or trade.get("is_open"):
        finalize_settlement(
            trade,
            spot_start=ptb,
            spot_end=fp,
            settlement_winner=winner,
        )
        if not has_chainlink:
            # Gamma outcomePrices resolved but eventMetadata chainlink not published yet
            trade["price_source"] = "polymarket_gamma"
            trade["settlement_pending"] = False
        return trade

    # --- Fallback: direction only ---
    _apply_bet_labels(trade, winner)
    _apply_chainlink_prices(trade, ptb, fp)
    trade["settlement_pending"] = not market_closed

    trade["coin"] = coin
    from trade_record import apply_entry_labels

    apply_entry_labels(trade)
    return trade


def main(limit: int = 20) -> None:
    config = _load_config()
    proxy_url = _proxy_url(config)
    log_path = ROOT / "logs" / "late_v3_btc" / "trades.jsonl"
    if not log_path.is_file():
        print(f"Missing {log_path}")
        sys.exit(1)

    lines = log_path.read_text(encoding="utf-8").splitlines()
    all_trades = []
    for line in lines:
        line = line.strip()
        if line:
            all_trades.append(json.loads(line))

    recent = all_trades[:limit]
    keys = {t.get("record_key") or t.get("market_slug"): i for i, t in enumerate(all_trades)}

    print(f"Refreshing {len(recent)} trades from Polymarket (proxy={proxy_url or 'env'})...\n")

    updated = 0
    for i, trade in enumerate(recent, 1):
        coin = (trade.get("coin") or "btc").lower()
        before = (
            trade.get("bet_result_label"),
            trade.get("bet_won"),
            trade.get("is_open"),
            trade.get("pnl"),
        )
        try:
            refresh_one(trade, coin=coin, proxy_url=proxy_url)
        except Exception as exc:
            print(f"{i:2d} ERROR {trade.get('entry_timestamp')} {exc}")
            continue

        trade["coin"] = coin
        from trade_record import apply_entry_labels

        apply_entry_labels(trade)

        rk = trade.get("record_key") or trade.get("market_slug")
        if rk in keys:
            all_trades[keys[rk]] = trade
            updated += 1

        after_lbl = trade.get("bet_result_label", "—")
        after_won = trade.get("bet_won")
        winner = trade.get("settlement_winner") or "?"
        pnl = trade.get("pnl", 0)
        status = "OPEN" if trade.get("is_open") else (trade.get("exit_reason") or "closed")
        print(
            f"{i:2d} | {trade.get('entry_timestamp')} | {trade.get('entry_reason', 'normal'):12s} | "
            f"{trade.get('bet_side')} | label={after_lbl} won={after_won} | "
            f"winner={winner} pnl={pnl:+.2f} | {status} pending={trade.get('settlement_pending')}"
        )
        after = (
            trade.get("bet_result_label"),
            trade.get("bet_won"),
            trade.get("is_open"),
            trade.get("pnl"),
        )
        if before != after:
            print(f"     [changed] {before} -> {after}")

    log_path.write_text(
        "\n".join(json.dumps(t, ensure_ascii=False) for t in all_trades) + "\n",
        encoding="utf-8",
    )
    print(f"\nDone: wrote {log_path} ({updated}/{len(recent)} rows updated)")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    main(n)
