"""
Manual settlement: pull Gamma/Chainlink for pending trade rows (no periodic polling).
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from polymarket_api import wait_for_official_settlement
from spot_price import infer_up_down_winner


def settle_one_market(
    *,
    coin: str,
    market_slug: str,
    multi_trader,
    strategy_base: str,
    proxy_url: Optional[str],
    lock_chainlink_window: Optional[Callable[[str, str], Tuple[float, float]]] = None,
    market_window_prices: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    delay_sec: float = 0.0,
    max_wait: float = 60.0,
) -> Dict[str, Any]:
    """Fetch official settlement and update trade record + position if any."""
    out: Dict[str, Any] = {
        "market_slug": market_slug,
        "coin": coin,
        "success": False,
        "error": None,
    }
    if not market_slug:
        out["error"] = "empty slug"
        return out

    strategy_name = f"{strategy_base}_{coin}"
    trader = multi_trader.get_trader(strategy_name)
    if not trader:
        out["error"] = f"no trader {strategy_name}"
        return out

    try:
        if delay_sec > 0:
            time.sleep(delay_sec)

        ptb, fp = 0.0, 0.0
        if lock_chainlink_window:
            ptb, fp = lock_chainlink_window(coin, market_slug)

        api = wait_for_official_settlement(
            market_slug,
            proxy_url=proxy_url,
            max_wait=max_wait,
            poll_interval=2.0,
            request_timeout=8,
        )
        winner = api.get("winner") if api.get("winner") in ("UP", "DOWN") else None
        ptb = float(api.get("price_to_beat") or 0) or ptb
        fp = float(api.get("final_price") or 0) or fp

        if not winner and ptb > 0 and fp > 0:
            winner = infer_up_down_winner(ptb, fp)

        market_closed = bool(api.get("closed") or api.get("resolved"))
        has_chainlink = ptb > 0 and fp > 0

        if winner not in ("UP", "DOWN"):
            out["error"] = (
                f"no winner (ptb={ptb:.2f} fp={fp:.2f} closed={api.get('closed')})"
            )
            return out

        if not market_closed and not has_chainlink:
            out["error"] = (
                f"market not closed yet (ptb={ptb:.2f} fp={fp:.2f} "
                f"closed={api.get('closed')})"
            )
            return out

        if not has_chainlink:
            out["partial"] = True

        if market_window_prices is not None:
            pw = dict(market_window_prices.get(coin, {}).get(market_slug) or {})
            if ptb > 0:
                pw["spot_start"] = ptb
            if fp > 0:
                pw["spot_end"] = fp
            if ptb > 0 and fp > 0:
                pw["price_source"] = "chainlink"
            pw["updated_at"] = time.time()
            market_window_prices.setdefault(coin, {})[market_slug] = pw

        if market_slug in trader.positions:
            result = multi_trader.close_market(
                strategy_name=strategy_name,
                market_slug=market_slug,
                winner=winner,
                btc_start=ptb,
                btc_final=fp,
                skip_official_fetch=True,
            )
            if result:
                out["success"] = True
                out["bet_result_label"] = result.get("bet_result_label")
                out["pnl"] = result.get("pnl")
            else:
                out["error"] = "close_market returned None"
        else:
            updated = trader.apply_chainlink_to_record(market_slug, ptb, fp, winner)
            if updated:
                sp1 = float(updated.get("spot_end") or updated.get("btc_final") or 0)
                out["success"] = True
                out["bet_result_label"] = updated.get("bet_result_label")
                out["pnl"] = updated.get("pnl")
                if sp1 <= 0 and not has_chainlink:
                    out["partial"] = True
            else:
                out["error"] = "no record found"
    except Exception as exc:
        out["error"] = str(exc)

    return out


def _slug_needs_chainlink_fill(trade: Dict[str, Any]) -> bool:
    if trade.get("is_open"):
        return True
    sp0 = float(trade.get("spot_start") or trade.get("btc_start") or 0)
    sp1 = float(trade.get("spot_end") or trade.get("btc_final") or trade.get("btc_end") or 0)
    if trade.get("settlement_pending"):
        return True
    if sp0 <= 0 or sp1 <= 0:
        return True
    if trade.get("bet_won") is None and trade.get("exit_reason") in (
        "flip_stop",
        "stop_loss",
        "early_exit",
        None,
    ):
        return True
    return False


def _entry_ts(trade: Dict[str, Any]) -> float:
    try:
        et = float(trade.get("entry_time") or 0)
        if et > 0:
            return et
    except (TypeError, ValueError):
        pass
    ts = trade.get("entry_timestamp")
    if ts:
        try:
            return datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S").timestamp()
        except ValueError:
            pass
    try:
        return float(trade.get("close_time") or 0)
    except (TypeError, ValueError):
        return 0.0


def _find_trade_record(tr, slug: str) -> Optional[Dict[str, Any]]:
    rec = getattr(tr, "open_trade_records", {}).get(slug)
    if rec is not None:
        return rec
    for t in reversed(getattr(tr, "closed_trades", []) or []):
        if t.get("market_slug") == slug:
            return t
    return None


def _register_pending(
    by_key: Dict[Tuple[str, str], float],
    coin: str,
    slug: str,
    trade: Optional[Dict[str, Any]],
    tr,
) -> None:
    slug = str(slug or "")
    if not slug:
        return
    if trade is not None:
        if not _slug_needs_chainlink_fill(trade) and not tr.record_needs_phase2(slug):
            return
    elif not tr.record_needs_phase2(slug):
        return
    if trade is None:
        trade = _find_trade_record(tr, slug) or {}
    key = (coin, slug)
    ts = _entry_ts(trade)
    if ts <= 0:
        try:
            ts = float(str(slug).rsplit("-", 1)[-1])
        except (TypeError, ValueError):
            ts = 0.0
    prev = by_key.get(key, 0.0)
    if ts >= prev:
        by_key[key] = ts


def collect_pending_settlements(
    multi_trader,
    strategy_base: str,
    coins: List[str],
    *,
    limit: Optional[int] = None,
) -> Tuple[List[Tuple[str, str]], int]:
    """
    Pending (coin, slug) sorted by entry time descending (newest first).
    Returns (items_to_process, total_pending_before_limit).
    """
    by_key: Dict[Tuple[str, str], float] = {}
    for coin in coins:
        strategy_name = f"{strategy_base}_{coin}"
        tr = multi_trader.get_trader(strategy_name)
        if not tr:
            continue
        for _rk, rec in dict(getattr(tr, "open_trade_records", {}) or {}).items():
            slug = str(rec.get("market_slug") or _rk)
            _register_pending(by_key, coin, slug, rec, tr)
        for trade in list(getattr(tr, "closed_trades", []) or []):
            slug = str(trade.get("market_slug") or "")
            _register_pending(by_key, coin, slug, trade, tr)
        log_dir = getattr(tr, "log_dir", None)
        path = Path(log_dir) / "trades.jsonl" if log_dir else None
        if path and path.is_file():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            raw = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        slug = str(raw.get("market_slug") or "")
                        if not slug or not _slug_needs_chainlink_fill(raw):
                            continue
                        _register_pending(by_key, coin, slug, raw, tr)
            except OSError:
                pass

    ordered = sorted(by_key.items(), key=lambda kv: kv[1], reverse=True)
    total = len(ordered)
    keys = [(c, s) for (c, s), _ in ordered]
    if limit is not None and limit > 0:
        keys = keys[: int(limit)]
    return keys, total


def settle_all_pending(
    *,
    multi_trader,
    strategy_base: str,
    coins: List[str],
    proxy_url: Optional[str],
    lock_chainlink_window: Optional[Callable[[str, str], Tuple[float, float]]] = None,
    market_window_prices: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    delay_sec: float = 0.0,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Settle pending slugs (newest first); optional cap via limit."""
    items, pending_total = collect_pending_settlements(
        multi_trader, strategy_base, coins, limit=limit
    )
    results: List[Dict[str, Any]] = []
    ok = 0
    fail = 0
    for coin, slug in items:
        r = settle_one_market(
            coin=coin,
            market_slug=slug,
            multi_trader=multi_trader,
            strategy_base=strategy_base,
            proxy_url=proxy_url,
            lock_chainlink_window=lock_chainlink_window,
            market_window_prices=market_window_prices,
            delay_sec=delay_sec,
        )
        results.append(r)
        if r.get("success"):
            ok += 1
        else:
            fail += 1
    return {
        "pending_total": pending_total,
        "pending_count": len(items),
        "limit": limit,
        "settled_ok": ok,
        "settled_fail": fail,
        "results": results,
    }
