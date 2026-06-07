"""Track and persist first-leg direction live ask peak (after first fill → window end)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def tick_first_leg_ask_max(
    *,
    coin: str,
    slug: str,
    up_ask: float,
    down_ask: float,
    strategies: Dict[str, Any],
    strategy_bases: List[str],
) -> None:
    for base_name in strategy_bases:
        sn = f"{base_name}_{coin}"
        stg = strategies.get(sn)
        if stg and stg.first_leg_ask_tracking_active(slug):
            stg.update_first_leg_ask_max(slug, up_ask=up_ask, down_ask=down_ask)
        if stg and stg.second_entry_pending(slug):
            stg.update_second_entry_hedge_ask_min(slug, up_ask=up_ask, down_ask=down_ask)


def flush_first_leg_ask_analytics(
    *,
    coin: str,
    slug: str,
    strategies: Dict[str, Any],
    multi_trader,
    strategy_bases: List[str],
    up_ask: float = 0.0,
    down_ask: float = 0.0,
) -> int:
    """Persist first-leg direction ask peak onto normal entry rows; clear tracker."""
    updated = 0
    for base_name in strategy_bases:
        sn = f"{base_name}_{coin}"
        stg = strategies.get(sn)
        if not stg:
            continue
        if up_ask > 0 or down_ask > 0:
            if stg.first_leg_ask_tracking_active(slug):
                stg.update_first_leg_ask_max(slug, up_ask=up_ask, down_ask=down_ask)
            if stg.second_entry_pending(slug):
                stg.update_second_entry_hedge_ask_min(slug, up_ask=up_ask, down_ask=down_ask)
        mx = stg.pop_first_leg_ask_max(slug)
        hmin = stg.pop_second_entry_hedge_ask_min(slug)
        if mx is None and hmin is None:
            continue
        thr = float(getattr(stg, "second_entry_ask_threshold", 0) or 0)
        hthr = float(getattr(stg, "second_entry_hedge_ask_threshold", 0) or 0)
        n = multi_trader.persist_first_leg_ask_max(
            sn,
            slug,
            mx or 0,
            second_entry_ask_threshold=thr,
            hedge_ask_min=hmin,
            hedge_ask_threshold=hthr,
        )
        if n:
            updated += n
            print(
                f"[ANALYTICS] {coin.upper()} {slug} first_leg_ask_max={mx} "
                f"hedge_ask_min={hmin} hedge_max={hthr:.4f}"
            )
    return updated
