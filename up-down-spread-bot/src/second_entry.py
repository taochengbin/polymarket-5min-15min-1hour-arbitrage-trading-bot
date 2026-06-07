"""
Second entry: after first leg fills, buy opposite when hedge ask < 1 - first_leg_ask_threshold
(config: strategy.second_entry.first_leg_ask_threshold). Works after flip-stop; max 2 orders/market.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from order_executor import OrderExecutor
from trade_logger import log_second_entry_trigger

_second_locks: Dict[str, threading.Lock] = {}
_second_locks_guard = threading.Lock()

HEDGE_ENTRY_REASONS = frozenset({"flip_reverse", "second_entry"})


def _second_lock(strategy_name: str, market_slug: str) -> threading.Lock:
    key = f"{strategy_name}:{market_slug}"
    with _second_locks_guard:
        if key not in _second_locks:
            _second_locks[key] = threading.Lock()
        return _second_locks[key]


def _already_has_second_entry(trader: Any, market_slug: str) -> bool:
    with trader.lock:
        pos = trader.positions.get(market_slug) or {}
        for ent in pos.get("all_entries") or []:
            if (ent.get("entry_reason") or "") in HEDGE_ENTRY_REASONS:
                return True
        for rec in (getattr(trader, "open_trade_records", None) or {}).values():
            if str(rec.get("market_slug") or "") != market_slug:
                continue
            if (rec.get("entry_reason") or "") == "second_entry" and rec.get("is_open"):
                return True
    return False


def _position_invested(trader: Any, market_slug: str) -> float:
    with trader.lock:
        pos = trader.positions.get(market_slug) or {}
        return float((pos.get("UP") or {}).get("total_invested") or 0) + float(
            (pos.get("DOWN") or {}).get("total_invested") or 0
        )


def _resolve_spot_open(
    *,
    coin: str,
    market_slug: str,
    market_state: Dict[str, Any],
    market_window_prices: Dict[str, Dict[str, Dict[str, Any]]],
    market_start_prices: Dict[str, Dict[str, float]],
    market_lock: threading.Lock,
) -> float:
    with market_lock:
        win = dict(market_window_prices[coin].get(market_slug) or {})
        tracked_open = market_start_prices[coin].get(market_slug, 0)
    cl_open = float(win.get("spot_start") or 0)
    if cl_open > 0 and win.get("price_source") == "chainlink":
        return cl_open
    if isinstance(tracked_open, (int, float)) and tracked_open > 0:
        return float(tracked_open)
    return float(market_state.get("market_start_price") or market_state.get("spot_start") or 0)


def _hedge_side_and_ask(first_leg_side: str, up_ask: float, down_ask: float) -> tuple:
    side = (first_leg_side or "").upper()
    if side == "UP":
        return "DOWN", float(down_ask)
    if side == "DOWN":
        return "UP", float(up_ask)
    return "", 0.0


def try_second_entry(
    *,
    strategy: Any,
    multi_trader: Any,
    strategy_name: str,
    market_slug: str,
    coin: str,
    first_leg_side: str,
    first_leg_live_ask: float,
    up_ask: float,
    down_ask: float,
    market_state: Dict[str, Any],
    data_feed: Any,
    market_window_prices: Dict[str, Dict[str, Dict[str, Any]]],
    market_start_prices: Dict[str, Dict[str, float]],
    market_lock: threading.Lock,
    window_range_tracker: Any = None,
    spot_open: float = 0.0,
    spot_now: float = 0.0,
) -> bool:
    """Place opposite-side second entry (entry_reason=second_entry). Returns True on success."""
    reverse_side, reverse_price = _hedge_side_and_ask(first_leg_side, up_ask, down_ask)
    if reverse_price <= 0:
        print(f"[SECOND-ENTRY] ✗ Invalid opposite ask for {reverse_side} @ {market_slug}")
        return False

    usd = float(strategy.second_entry_usd)
    contracts = round(usd / reverse_price, 2)
    if contracts <= 0:
        print(f"[SECOND-ENTRY] ✗ Contract size 0 for {market_slug}")
        return False

    log_second_entry_trigger(
        market_slug=market_slug,
        coin=coin,
        first_leg_side=first_leg_side,
        trigger_price=first_leg_live_ask,
        threshold_price=float(strategy.second_entry_max_hedge_ask()),
        reverse_side=reverse_side,
        reverse_ask=reverse_price,
        spot_open=spot_open,
        spot_now=spot_now,
        max_spot_distance=float(strategy.second_entry_max_spot_distance_usd),
    )

    OrderExecutor.unblock_market(market_slug, coin)

    try:
        spot_at_entry = float(
            data_feed.refresh_coin_spot(coin) or market_state.get("price") or spot_now or 0
        )
    except Exception:
        spot_at_entry = float(market_state.get("price") or spot_now or 0)

    market_spot_open = spot_open
    if market_spot_open <= 0:
        market_spot_open = _resolve_spot_open(
            coin=coin,
            market_slug=market_slug,
            market_state=market_state,
            market_window_prices=market_window_prices,
            market_start_prices=market_start_prices,
            market_lock=market_lock,
        )

    wr_fields: Dict[str, Any] = {}
    if window_range_tracker is not None and getattr(
        window_range_tracker, "enabled", False
    ):
        wr_fields = window_range_tracker.fields_for_trade_record(
            coin,
            market_slug,
            spot_now=float(spot_at_entry or market_state.get("price") or 0),
        )

    success = multi_trader.enter_position(
        strategy_name=strategy_name,
        market_slug=market_slug,
        side=reverse_side,
        price=reverse_price,
        contracts=contracts,
        up_ask=up_ask,
        down_ask=down_ask,
        entry_reason="second_entry",
        seconds_till_end=int(market_state.get("seconds_till_end") or 0),
        spot_at_entry=spot_at_entry,
        market_spot_open=market_spot_open,
        window_range_high=wr_fields.get("window_range_high"),
        window_range_low=wr_fields.get("window_range_low"),
    )
    if success:
        strategy.mark_flip_reverse_placed(market_slug)
        strategy._entry_signal_pending.pop(market_slug, None)
        print(
            f"[SECOND-ENTRY] ✓ {coin.upper()} {first_leg_side} live ask ${first_leg_live_ask:.2f} "
            f"→ hedge {reverse_side} {contracts:.2f} @ ${reverse_price:.2f} (~${usd:.0f}) | {market_slug}"
        )
    else:
        strategy.abort_flip_reverse(market_slug)
        print(
            f"[SECOND-ENTRY] ✗ Hedge {reverse_side} @ ${reverse_price:.2f} failed | {market_slug}"
        )
    return bool(success)


def maybe_second_entry(
    *,
    config: Dict[str, Any],
    strategy: Any,
    multi_trader: Any,
    strategy_name: str,
    market_slug: str,
    coin: str,
    up_ask: float,
    down_ask: float,
    market_state: Dict[str, Any],
    data_feed: Any,
    market_window_prices: Dict[str, Dict[str, Dict[str, Any]]],
    market_start_prices: Dict[str, Dict[str, float]],
    market_lock: threading.Lock,
    window_range_tracker: Any = None,
) -> bool:
    """If opposite ask < 1 - first_leg_ask_threshold, buy hedge."""
    if not strategy:
        return False

    strategy.apply_second_entry_config_from(config)
    if not strategy.second_entry_pending(market_slug):
        return False

    trader = multi_trader.get_trader(strategy_name)
    if not trader:
        return False

    with _second_lock(strategy_name, market_slug):
        if int(strategy._entries_placed.get(market_slug, 0) or 0) < 1:
            if trader.has_first_leg_for_flip_reverse(market_slug):
                strategy.sync_entry_from_open_position(market_slug, trader)
        if int(strategy._entries_placed.get(market_slug, 0) or 0) < 1:
            return False

        first_leg = strategy.get_first_leg_side(market_slug)
        if not first_leg:
            return False

        first_leg_live_ask = strategy.first_leg_live_ask(
            market_slug, up_ask=up_ask, down_ask=down_ask
        )
        opposite_live_ask = strategy.opposite_leg_live_ask(
            market_slug, up_ask=up_ask, down_ask=down_ask
        )

        spot_open = _resolve_spot_open(
            coin=coin,
            market_slug=market_slug,
            market_state=market_state,
            market_window_prices=market_window_prices,
            market_start_prices=market_start_prices,
            market_lock=market_lock,
        )
        try:
            spot_now = float(
                data_feed.refresh_coin_spot(coin) or market_state.get("price") or 0
            )
        except Exception:
            spot_now = float(market_state.get("price") or 0)

        allowed, reason = strategy.second_entry_allowed(
            market_slug,
            first_leg_live_ask=first_leg_live_ask,
            opposite_live_ask=opposite_live_ask,
            spot_now=spot_now,
            spot_open=spot_open,
        )
        if not allowed:
            return False

        if _already_has_second_entry(trader, market_slug):
            strategy.mark_flip_reverse_placed(market_slug)
            return False

        invested = _position_invested(trader, market_slug)
        if invested + float(strategy.second_entry_usd) > float(strategy.max_investment) + 0.01:
            print(
                f"[SECOND-ENTRY] Skip {market_slug}: would exceed max_investment "
                f"(${invested:.2f}+${strategy.second_entry_usd:.0f}>{strategy.max_investment:.0f})"
            )
            return False

        if not strategy.try_begin_flip_reverse(market_slug):
            return False

        ok = try_second_entry(
            strategy=strategy,
            multi_trader=multi_trader,
            strategy_name=strategy_name,
            market_slug=market_slug,
            coin=coin,
            first_leg_side=first_leg,
            first_leg_live_ask=first_leg_live_ask,
            up_ask=up_ask,
            down_ask=down_ask,
            market_state=market_state,
            data_feed=data_feed,
            market_window_prices=market_window_prices,
            market_start_prices=market_start_prices,
            market_lock=market_lock,
            window_range_tracker=window_range_tracker,
            spot_open=spot_open,
            spot_now=spot_now,
        )
        if ok:
            with market_lock:
                if market_start_prices[coin].get(market_slug) == -2:
                    market_start_prices[coin][market_slug] = 0
        return ok
