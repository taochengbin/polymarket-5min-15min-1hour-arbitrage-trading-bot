"""
Price-triggered reverse hedge: after first leg fills, buy opposite when token ask <= reverse_entry_price.
Independent of flip-stop (flip only sells; no buy at flip time).
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from trade_logger import log_reverse_entry_trigger

_reverse_locks: Dict[str, threading.Lock] = {}
_reverse_locks_guard = threading.Lock()


def _reverse_lock(strategy_name: str, market_slug: str) -> threading.Lock:
    key = f"{strategy_name}:{market_slug}"
    with _reverse_locks_guard:
        if key not in _reverse_locks:
            _reverse_locks[key] = threading.Lock()
        return _reverse_locks[key]


def _position_has_flip_reverse(trader: Any, market_slug: str) -> bool:
    with trader.lock:
        pos = trader.positions.get(market_slug) or {}
        for ent in pos.get("all_entries") or []:
            if (ent.get("entry_reason") or "") == "flip_reverse":
                return True
    return False


def _position_invested(trader: Any, market_slug: str) -> float:
    with trader.lock:
        pos = trader.positions.get(market_slug) or {}
        return float((pos.get("UP") or {}).get("total_invested") or 0) + float(
            (pos.get("DOWN") or {}).get("total_invested") or 0
        )


def try_reverse_hedge_entry(
    *,
    strategy: Any,
    multi_trader: Any,
    strategy_name: str,
    market_slug: str,
    coin: str,
    first_leg_side: str,
    first_leg_price: float,
    up_ask: float,
    down_ask: float,
    market_state: Dict[str, Any],
    data_feed: Any,
    market_window_prices: Dict[str, Dict[str, Dict[str, Any]]],
    market_start_prices: Dict[str, Dict[str, float]],
    market_lock: threading.Lock,
    window_range_tracker: Any = None,
) -> bool:
    """Place opposite-side hedge (entry_reason=flip_reverse). Returns True on success."""
    reverse_side = "DOWN" if first_leg_side == "UP" else "UP"
    reverse_price = float(down_ask if reverse_side == "DOWN" else up_ask)
    if reverse_price <= 0:
        print(f"[REVERSE-ENTRY] ✗ Invalid opposite ask for {reverse_side} @ {market_slug}")
        return False

    usd = float(strategy.flip_reverse_entry_usd)
    contracts = round(usd / reverse_price, 2)
    if contracts <= 0:
        print(f"[REVERSE-ENTRY] ✗ Contract size 0 for {market_slug}")
        return False

    log_reverse_entry_trigger(
        market_slug=market_slug,
        coin=coin,
        first_leg_side=first_leg_side,
        trigger_price=first_leg_price,
        threshold_price=float(strategy.reverse_entry_price),
        reverse_side=reverse_side,
        reverse_ask=reverse_price,
    )

    try:
        spot_at_entry = float(
            data_feed.refresh_coin_spot(coin) or market_state.get("price") or 0
        )
    except Exception:
        spot_at_entry = float(market_state.get("price") or 0)

    market_spot_open = 0.0
    with market_lock:
        win = dict(market_window_prices[coin].get(market_slug) or {})
        tracked_open = market_start_prices[coin].get(market_slug, 0)
    cl_open = float(win.get("spot_start") or 0)
    if cl_open > 0 and win.get("price_source") == "chainlink":
        market_spot_open = cl_open
    elif isinstance(tracked_open, (int, float)) and tracked_open > 0:
        market_spot_open = float(tracked_open)

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
        entry_reason="flip_reverse",
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
            f"[REVERSE-ENTRY] ✓ {coin.upper()} {first_leg_side} @ ${first_leg_price:.2f} "
            f"→ hedge {reverse_side} {contracts:.2f} @ ${reverse_price:.2f} (~${usd:.0f}) | {market_slug}"
        )
    else:
        strategy.abort_flip_reverse(market_slug)
        print(
            f"[REVERSE-ENTRY] ✗ Hedge {reverse_side} @ ${reverse_price:.2f} failed | {market_slug}"
        )
    return bool(success)


def maybe_reverse_hedge_entry(
    *,
    config: Dict[str, Any],
    strategy: Any,
    multi_trader: Any,
    strategy_name: str,
    market_slug: str,
    coin: str,
    our_side: str,
    our_price: float,
    up_ask: float,
    down_ask: float,
    market_state: Dict[str, Any],
    data_feed: Any,
    market_window_prices: Dict[str, Dict[str, Dict[str, Any]]],
    market_start_prices: Dict[str, Dict[str, float]],
    market_lock: threading.Lock,
    window_range_tracker: Any = None,
) -> bool:
    """If price trigger met, place reverse hedge. True = entered or already done."""
    if not strategy:
        return False

    strategy.apply_exit_config_from(config)
    trader = multi_trader.get_trader(strategy_name)
    if not trader or market_slug not in trader.positions:
        return False

    with _reverse_lock(strategy_name, market_slug):
        if int(strategy._entries_placed.get(market_slug, 0) or 0) < 1:
            if trader.has_first_leg_for_flip_reverse(market_slug):
                strategy.sync_entry_from_open_position(market_slug, trader)

        allowed, reason = strategy.reverse_hedge_entry_allowed(
            market_slug, our_side=our_side, our_price=our_price
        )
        if not allowed:
            return False

        if _position_has_flip_reverse(trader, market_slug):
            strategy.mark_flip_reverse_placed(market_slug)
            return False

        invested = _position_invested(trader, market_slug)
        if invested + float(strategy.flip_reverse_entry_usd) > float(strategy.max_investment) + 0.01:
            print(
                f"[REVERSE-ENTRY] Skip {market_slug}: would exceed max_investment "
                f"(${invested:.2f}+${strategy.flip_reverse_entry_usd:.0f}>{strategy.max_investment:.0f})"
            )
            return False

        if not strategy.try_begin_flip_reverse(market_slug):
            return False

        first_leg = strategy.get_first_leg_side(market_slug) or our_side
        ok = try_reverse_hedge_entry(
            strategy=strategy,
            multi_trader=multi_trader,
            strategy_name=strategy_name,
            market_slug=market_slug,
            coin=coin,
            first_leg_side=first_leg,
            first_leg_price=our_price,
            up_ask=up_ask,
            down_ask=down_ask,
            market_state=market_state,
            data_feed=data_feed,
            market_window_prices=market_window_prices,
            market_start_prices=market_start_prices,
            market_lock=market_lock,
            window_range_tracker=window_range_tracker,
        )
        if ok:
            with market_lock:
                if market_start_prices[coin].get(market_slug) == -2:
                    market_start_prices[coin][market_slug] = 0
        return ok
