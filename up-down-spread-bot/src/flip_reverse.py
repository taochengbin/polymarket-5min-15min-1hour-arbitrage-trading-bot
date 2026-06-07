"""
Flip-stop: sell the losing leg only (no reverse buy at flip time).
Reverse hedge is handled by reverse_entry.py on price trigger while first leg is open.
When second_entry exists: flip-stop may sell first leg only if still open;
second_entry leg is never flip-stopped and holds to settlement.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from order_executor import OrderExecutor
from trade_logger import log_exit_trigger


def execute_flip_stop_sell_only(
    *,
    strategy: Any,
    multi_trader: Any,
    strategy_name: str,
    market_slug: str,
    coin: str,
    our_side: str,
    our_price: float,
    up_bid: float,
    down_bid: float,
    flip_stop_price: float,
    open_spot: float = 0.0,
    cur_spot: float = 0.0,
    market_lock: Optional[threading.Lock] = None,
    market_start_prices: Optional[Dict[str, Dict]] = None,
    order_executor: Any = None,
) -> Optional[Dict]:
    """Sell flip-stop leg. With second_entry open, first leg only; hedge leg stays to settlement."""
    if not strategy.try_begin_flip_stop(market_slug):
        return None

    trader = multi_trader.get_trader(strategy_name)
    if not trader:
        strategy.abort_flip_stop(market_slug)
        return None

    second_entry_open = trader.has_open_second_entry(market_slug)

    if second_entry_open:
        return _execute_first_leg_flip_with_second_entry(
            strategy=strategy,
            multi_trader=multi_trader,
            strategy_name=strategy_name,
            market_slug=market_slug,
            coin=coin,
            our_side=our_side,
            our_price=our_price,
            up_bid=up_bid,
            down_bid=down_bid,
            flip_stop_price=flip_stop_price,
            open_spot=open_spot,
            cur_spot=cur_spot,
            trader=trader,
        )

    prev_status: Any = -999
    blocked = False

    def _rollback() -> None:
        strategy.abort_flip_stop(market_slug)
        if blocked and order_executor is not None:
            OrderExecutor.unblock_market(market_slug, coin)
        if market_lock is not None and market_start_prices is not None and coin:
            with market_lock:
                if prev_status == -2:
                    return
                bucket = market_start_prices.get(coin)
                if bucket is None:
                    return
                if prev_status == -999:
                    bucket.pop(market_slug, None)
                else:
                    bucket[market_slug] = prev_status

    if market_lock is not None and market_start_prices is not None and coin:
        with market_lock:
            bucket = market_start_prices.setdefault(coin, {})
            prev_status = bucket.get(market_slug, -999)
            if prev_status == -2:
                strategy.abort_flip_stop(market_slug)
                return None
            bucket[market_slug] = -2

    if order_executor is not None:
        order_executor.block_market(market_slug, coin)
        blocked = True

    log_exit_trigger(
        market_slug=market_slug,
        exit_reason="flip_stop",
        coin=coin,
        trigger_price=our_price,
        threshold_price=flip_stop_price,
    )

    snap = trader.snapshot_flip_position(market_slug)
    if not snap:
        _rollback()
        return None

    sell_results = trader.flip_exchange_sell(
        market_slug,
        snap["up_contracts"],
        snap["down_contracts"],
        up_bid,
        down_bid,
    )

    print(f"[FLIP] 仅卖 {our_side} @ ${our_price:.2f} | {market_slug}")

    result = multi_trader.close_market_early_exit(
        strategy_name=strategy_name,
        market_slug=market_slug,
        exit_price=our_price,
        exit_reason="flip_stop",
        up_bid=up_bid,
        down_bid=down_bid,
        keep_market_open_for_reentry=False,
        skip_exchange_sell=True,
        parallel_sell_results=sell_results,
    )

    if not result:
        _rollback()
        return None

    strategy.mark_flip_stop_handled(market_slug)

    _print_flip_summary(
        coin=coin,
        strategy_name=strategy_name,
        market_slug=market_slug,
        our_side=our_side,
        our_price=our_price,
        flip_stop_price=flip_stop_price,
        open_spot=open_spot,
        cur_spot=cur_spot,
        max_spot_dist=float(strategy.flip_stop_max_spot_distance_usd),
        result=result,
    )
    return result


def _execute_first_leg_flip_with_second_entry(
    *,
    strategy: Any,
    multi_trader: Any,
    strategy_name: str,
    market_slug: str,
    coin: str,
    our_side: str,
    our_price: float,
    up_bid: float,
    down_bid: float,
    flip_stop_price: float,
    open_spot: float,
    cur_spot: float,
    trader: Any,
) -> Optional[Dict]:
    """First leg flip-stop only; second_entry leg remains open for settlement."""
    leg = (our_side or "").upper()
    if leg not in ("UP", "DOWN"):
        strategy.abort_flip_stop(market_slug)
        return None

    log_exit_trigger(
        market_slug=market_slug,
        exit_reason="flip_stop",
        coin=coin,
        trigger_price=our_price,
        threshold_price=flip_stop_price,
    )

    snap = trader.snapshot_flip_position(market_slug)
    if not snap:
        strategy.abort_flip_stop(market_slug)
        return None

    up_sell = snap["up_contracts"] if leg == "UP" else 0.0
    down_sell = snap["down_contracts"] if leg == "DOWN" else 0.0
    sell_results = trader.flip_exchange_sell(
        market_slug,
        up_sell,
        down_sell,
        up_bid,
        down_bid,
    )

    print(
        f"[FLIP] 首单 {leg} flip-stop @ ${our_price:.2f} | "
        f"second_entry 持有至结算 | {market_slug}"
    )

    result = multi_trader.close_first_leg_flip_stop(
        strategy_name=strategy_name,
        market_slug=market_slug,
        leg_side=leg,
        exit_price=our_price,
        up_bid=up_bid,
        down_bid=down_bid,
        skip_exchange_sell=True,
        parallel_sell_results=sell_results,
    )

    if not result:
        strategy.abort_flip_stop(market_slug)
        return None

    strategy.mark_flip_stop_handled(market_slug)

    _print_flip_summary(
        coin=coin,
        strategy_name=strategy_name,
        market_slug=market_slug,
        our_side=our_side,
        our_price=our_price,
        flip_stop_price=flip_stop_price,
        open_spot=open_spot,
        cur_spot=cur_spot,
        max_spot_dist=float(strategy.flip_stop_max_spot_distance_usd),
        result=result,
        second_leg_held=True,
    )
    return result


def _print_flip_summary(
    *,
    coin: str,
    strategy_name: str,
    market_slug: str,
    our_side: str,
    our_price: float,
    flip_stop_price: float,
    open_spot: float,
    cur_spot: float,
    max_spot_dist: float,
    result: Dict,
    second_leg_held: bool = False,
) -> None:
    print(f"\n{'=' * 80}")
    print(f"[{coin.upper()}] 🛑 FLIP-STOP @ ${our_price:.2f}")
    print(f"[{strategy_name}] {market_slug}")
    print(f"[EXIT] Closed leg: {our_side}")
    if second_leg_held:
        print("[EXIT] second_entry leg held open until settlement (no flip-stop on hedge)")
    if max_spot_dist > 0 and open_spot > 0 and cur_spot > 0:
        if our_side == "UP":
            print(f"[EXIT] spot ${cur_spot:,.2f} < ${open_spot + max_spot_dist:,.2f}")
        else:
            print(f"[EXIT] spot ${cur_spot:,.2f} > ${open_spot - max_spot_dist:,.2f}")
    print(f"[EXIT] Token ${our_price:.2f} ≤ ${flip_stop_price:.2f}")
    if isinstance(result, dict):
        print(f"[EXIT] Leg PnL: ${result.get('pnl', 0):+.2f}")
    print(f"{'=' * 80}\n")
