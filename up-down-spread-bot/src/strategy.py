"""
Meridian — late-window entry strategy (Late Entry V3 / late_v3).
Time-based sizing; supports 5m and 15m Polymarket windows (see data_sources.polymarket.market_interval_sec).
"""
import threading
import time
from typing import Any, Optional, Dict, Union


def check_flip_stop_trigger(
    *,
    our_price: float,
    bet_side: str,
    flip_stop_price: float,
    market_open_spot: float,
    current_spot: float,
    max_spot_distance_usd: float,
) -> bool:
    """
    Flip-stop: BOTH must hold —
    1) our token ask <= price_threshold
    2) spot crossed back toward window open (after adverse move):
       UP bet (entered when spot > open): current_spot < open + max
         e.g. open 60000, max 10 → spot < 60010
       DOWN bet (entered when spot < open): current_spot > open - max
         e.g. open 60000, max 10 → spot > 59990
    If max_spot_distance_usd <= 0, only condition (1) applies (legacy).
    """
    if our_price > flip_stop_price:
        return False
    if max_spot_distance_usd <= 0:
        return True
    if market_open_spot <= 0 or current_spot <= 0:
        return False
    side = (bet_side or "").upper()
    if side == "UP":
        return current_spot < market_open_spot + max_spot_distance_usd
    if side == "DOWN":
        return current_spot > market_open_spot - max_spot_distance_usd
    return False


HEDGE_ENTRY_REASONS = frozenset({"flip_reverse", "second_entry"})


def _is_hedge_entry_reason(entry_reason: Optional[str]) -> bool:
    return (entry_reason or "") in HEDGE_ENTRY_REASONS


def _first_leg_from_entries(all_entries: list) -> Optional[str]:
    for ent in all_entries or []:
        if _is_hedge_entry_reason(ent.get("entry_reason")):
            continue
        s = (ent.get("side") or "").upper()
        if s in ("UP", "DOWN"):
            return s
    return None


def _has_flip_reverse_entry(all_entries: list) -> bool:
    """True only for flip_reverse hedge — second_entry does not count."""
    return any(
        (ent.get("entry_reason") or "") == "flip_reverse"
        for ent in (all_entries or [])
    )


def _has_second_entry(all_entries: list) -> bool:
    return any(
        (ent.get("entry_reason") or "") == "second_entry"
        for ent in (all_entries or [])
    )


def _second_entry_active(all_entries: list, *, entries_placed: int = 0) -> bool:
    """True when second_entry hedge exists or was just placed (not flip_reverse)."""
    entries = all_entries or []
    if _has_second_entry(entries):
        return True
    if int(entries_placed or 0) >= 2 and not _has_flip_reverse_entry(entries):
        return True
    return False


def _first_leg_shares_open(
    *,
    first_leg_side: Optional[str],
    all_entries: Optional[list],
    up_shares: float,
    down_shares: float,
) -> float:
    first = (first_leg_side or "").upper() if first_leg_side else None
    if not first:
        first = _first_leg_from_entries(all_entries or [])
    if first == "UP":
        return float(up_shares or 0)
    if first == "DOWN":
        return float(down_shares or 0)
    return 0.0


def resolve_flip_stop_target(
    *,
    flip_cfg: Dict,
    up_ask: float,
    down_ask: float,
    all_entries: Optional[list] = None,
    flip_reverse_done: bool = False,
    flip_stop_handled: bool = False,
    first_leg_side: Optional[str] = None,
    up_shares: float = 0,
    down_shares: float = 0,
    entries_placed: int = 0,
) -> Optional[Dict[str, Any]]:
    """
    Which leg flip-stop monitors:
    - first leg only when both legs may be open; second_entry leg never flip-stopped
    - if only second_entry shares remain → no flip-stop (hold to settlement)
    - after flip_reverse hedge only → reverse_stop_price_threshold
    """
    if flip_stop_handled:
        return None
    if not bool(flip_cfg.get("enabled", True)):
        return None

    entries = all_entries or []
    first = (first_leg_side or "").upper() if first_leg_side else None
    if not first:
        first = _first_leg_from_entries(entries)

    second_active = _second_entry_active(entries, entries_placed=entries_placed)
    if second_active:
        if _first_leg_shares_open(
            first_leg_side=first,
            all_entries=entries,
            up_shares=up_shares,
            down_shares=down_shares,
        ) <= 0:
            return None

    first_thr = float(flip_cfg.get("price_threshold", 0.48))
    rev_thr_raw = float(flip_cfg.get("reverse_stop_price_threshold", 0) or 0)
    rev_thr = rev_thr_raw if rev_thr_raw > 0 else first_thr

    has_rev = _has_flip_reverse_entry(entries)
    if not has_rev and flip_reverse_done and not second_active:
        # flip_reverse fill in flight — entries not appended yet
        has_rev = True
    if has_rev and second_active:
        has_rev = False

    if not first:
        first = _first_leg_from_entries(entries)

    if has_rev:
        if not bool(flip_cfg.get("reverse_entry_enabled", False)):
            return None
        if first not in ("UP", "DOWN"):
            return None
        rev_side = "DOWN" if first == "UP" else "UP"
        px = float(down_ask if rev_side == "DOWN" else up_ask)
        return {
            "side": rev_side,
            "price": px,
            "threshold": rev_thr,
            "leg": "reverse",
        }

    if first not in ("UP", "DOWN"):
        return None
    px = float(up_ask if first == "UP" else down_ask)
    return {
        "side": first,
        "price": px,
        "threshold": first_thr,
        "leg": "first",
    }


def _parse_skip_ranges(raw: Any) -> list:
    out: list = []
    for item in raw or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            lo, hi = float(item[0]), float(item[1])
            if hi > lo:
                out.append((lo, hi))
    return out


def _parse_side_price_filters(strategy_cfg: Dict, default_price_max: float) -> Dict[str, Dict]:
    filters: Dict[str, Dict] = {}
    raw = strategy_cfg.get("side_price_filters") or {}
    for side in ("UP", "DOWN"):
        sf = raw.get(side) or {}
        cap = float(sf.get("price_max") or default_price_max)
        filters[side] = {
            "price_max": cap,
            "skip_ranges": _parse_skip_ranges(sf.get("skip_ask_ranges")),
        }
    return filters


def side_entry_price_allowed(
    side: str,
    price: float,
    *,
    global_price_max: float,
    side_filters: Dict[str, Dict],
) -> bool:
    side_u = (side or "").upper()
    if side_u not in ("UP", "DOWN") or price <= 0:
        return False
    cap = min(
        float(global_price_max),
        float((side_filters.get(side_u) or {}).get("price_max") or global_price_max),
    )
    if price > cap:
        return False
    for lo, hi in (side_filters.get(side_u) or {}).get("skip_ranges") or []:
        if lo <= price < hi:
            return False
    return True


class LateEntryStrategy:
    """Late-window entry: trade the favorite side in the final minutes of the window."""
    
    def __init__(self, config: Dict):
        # Read ALL params from config (NO HARDCODED VALUES!)
        strategy_cfg = config.get('strategy', {})
        pm = config.get("data_sources", {}).get("polymarket", {})
        self.market_interval_sec = int(pm.get("market_interval_sec", 900))
        if self.market_interval_sec <= 0:
            self.market_interval_sec = 900
        
        # Default entry window: ~last 4 min of 15m, ~last 2 min of 5m (only when config omits entry_window_sec)
        default_entry = 240 if self.market_interval_sec >= 900 else min(120, self.market_interval_sec - 10)
        raw_ew = int(strategy_cfg.get("entry_window_sec", default_entry))
        # Honor config as-is; only clamp to [10, market_interval - 5] so window fits market length
        self.entry_window = max(10, min(raw_ew, self.market_interval_sec - 5))
        self.entry_freq = strategy_cfg.get('entry_frequency_sec', 7)
        self.min_confidence = strategy_cfg.get('min_confidence', 0.30)
        self.max_spread = strategy_cfg.get('max_spread', 1.05)
        self.price_max = strategy_cfg.get('price_max', 0.93)
        self.side_price_filters = _parse_side_price_filters(strategy_cfg, float(self.price_max))
        # Min |current spot - market open| in USD to allow entry (0 = disabled)
        self.min_spot_move_usd = float(strategy_cfg.get('min_spot_move_usd', 0) or 0)
        # Min Chainlink high-low range (USD) since open lock before entry (0 = disabled)
        self.min_window_range_usd = float(strategy_cfg.get('min_window_range_usd', 0) or 0)
        # At most N entry signals per market slug (1 = one shot per 5m/15m window)
        self.max_entries_per_market = int(strategy_cfg.get("max_entries_per_market", 999))
        # Fixed USD notional per entry (0 = use time-tier contract sizing below)
        self.entry_order_usd = float(strategy_cfg.get("entry_order_usd", 0.0) or 0.0)
        
        # Sizing (contracts) - time-based FROM CONFIG!
        sizing_cfg = strategy_cfg.get('sizing', {})
        self.size_above_180 = sizing_cfg.get('above_180_sec', 8)
        self.size_above_120 = sizing_cfg.get('above_120_sec', 10)
        self.size_below_120 = sizing_cfg.get('below_120_sec', 12)
        # Scale 180s/120s thresholds for shorter windows (e.g. 5m → 60s/40s)
        scale = self.market_interval_sec / 900.0
        self.sizing_t1 = max(15, int(180 * scale))
        self.sizing_t2 = max(10, int(120 * scale))
        
        # Max investment per market
        self.max_investment = strategy_cfg.get('max_investment_per_market', 300)

        # second_entry_ask_threshold ← config strategy.second_entry.first_leg_ask_threshold
        second_cfg = strategy_cfg.get("second_entry", {}) or {}
        self.second_entry_enabled = bool(second_cfg.get("enabled", False))
        self.second_entry_ask_threshold = float(
            second_cfg.get("first_leg_ask_threshold", 0.85) or 0.85
        )
        hedge_thr = float(second_cfg.get("hedge_ask_threshold", 0) or 0)
        self.second_entry_hedge_ask_threshold = (
            hedge_thr
            if hedge_thr > 0
            else max(0.01, round(1.0 - self.second_entry_ask_threshold, 4))
        )
        self.second_entry_usd = float(second_cfg.get("order_usd", 0) or 0)
        if self.second_entry_usd <= 0:
            self.second_entry_usd = self.entry_order_usd
        self.second_entry_max_spot_distance_usd = float(
            second_cfg.get("max_spot_distance_from_open_usd", 100) or 100
        )
        
        # Flip-stop price (price reversal protection)
        exit_cfg = config.get('exit', {})
        flip_cfg = exit_cfg.get('flip_stop', {})
        self.flip_stop_enabled = bool(flip_cfg.get('enabled', True))
        self.flip_stop_price = flip_cfg.get('price_threshold', 0.48)
        self.flip_stop_max_spot_distance_usd = float(
            flip_cfg.get('max_spot_distance_from_open_usd', 0) or 0
        )
        rev_stop_raw = float(flip_cfg.get('reverse_stop_price_threshold', 0) or 0)
        self.reverse_stop_price_threshold = (
            rev_stop_raw if rev_stop_raw > 0 else float(self.flip_stop_price)
        )
        self.flip_reverse_enabled = bool(flip_cfg.get('reverse_entry_enabled', False))
        self.flip_reverse_entry_usd = float(flip_cfg.get('reverse_entry_usd', 0) or 0)
        if self.flip_reverse_entry_usd <= 0:
            self.flip_reverse_entry_usd = self.entry_order_usd
        # When first-leg token ask <= this, buy opposite (like min_confidence for spread)
        raw_rev_px = float(flip_cfg.get('reverse_entry_price', 0) or 0)
        self.reverse_entry_price = (
            raw_rev_px if raw_rev_px > 0 else float(self.flip_stop_price)
        )

        # Track last entry per market
        self.last_entry = {}
        self.last_favorite = {}
        # Confirmed fills per market (increment only after enter succeeds)
        self._entries_placed: Dict[str, int] = {}
        # Signal emitted, waiting for fill confirmation
        self._entry_signal_pending: Dict[str, bool] = {}
        # Second leg: price-triggered hedge (max one per market)
        self._flip_reverse_done: Dict[str, bool] = {}
        self._flip_reverse_pending: Dict[str, bool] = {}
        # Side of first normal entry (UP/DOWN) for reverse trigger
        self._first_leg_side: Dict[str, str] = {}
        # Max first-leg token ask from normal entry until window end (for threshold analysis)
        self._first_leg_ask_max: Dict[str, float] = {}
        self._second_entry_hedge_track: Dict[str, bool] = {}
        self._second_entry_hedge_ask_min: Dict[str, float] = {}
        # After flip-stop on first leg: no more normal entries this window
        self._flip_stop_handled: Dict[str, bool] = {}
        self._flip_stop_pending: Dict[str, bool] = {}
        self._state_lock = threading.Lock()
        # Hard cap: 1 normal + 1 hedge per market slug
        self.max_orders_per_market = 2

    def apply_second_entry_config_from(self, config: Dict) -> None:
        """Reload second-entry params from config (each price tick)."""
        second_cfg = config.get("strategy", {}).get("second_entry", {}) or {}
        self.second_entry_enabled = bool(second_cfg.get("enabled", self.second_entry_enabled))
        self.second_entry_ask_threshold = float(
            second_cfg.get("first_leg_ask_threshold", self.second_entry_ask_threshold)
            or self.second_entry_ask_threshold
        )
        hedge_thr = float(second_cfg.get("hedge_ask_threshold", 0) or 0)
        if hedge_thr > 0:
            self.second_entry_hedge_ask_threshold = hedge_thr
        else:
            self.second_entry_hedge_ask_threshold = max(
                0.01, round(1.0 - self.second_entry_ask_threshold, 4)
            )
        self.second_entry_usd = float(second_cfg.get("order_usd", 0) or 0)
        if self.second_entry_usd <= 0:
            self.second_entry_usd = float(
                config.get("strategy", {}).get("entry_order_usd", self.entry_order_usd)
                or self.entry_order_usd
            )
        self.second_entry_max_spot_distance_usd = float(
            second_cfg.get(
                "max_spot_distance_from_open_usd",
                self.second_entry_max_spot_distance_usd,
            )
            or self.second_entry_max_spot_distance_usd
        )

    def second_entry_pending(self, market_slug: str) -> bool:
        """First leg confirmed; second leg not yet placed."""
        if not self.second_entry_enabled:
            return False
        if self._flip_reverse_done.get(market_slug):
            return False
        if int(self._entries_placed.get(market_slug, 0) or 0) < 1:
            return False
        return bool(self.get_first_leg_side(market_slug))

    def first_leg_live_ask(
        self, market_slug: str, *, up_ask: float, down_ask: float
    ) -> float:
        """Real-time ask on the first-leg side (UP or DOWN), even after flip-stop."""
        first = self.get_first_leg_side(market_slug)
        if first == "UP":
            return float(up_ask or 0)
        if first == "DOWN":
            return float(down_ask or 0)
        return 0.0

    def opposite_leg_live_ask(
        self, market_slug: str, *, up_ask: float, down_ask: float
    ) -> float:
        """Real-time ask on the hedge (opposite) side of the first leg."""
        first = self.get_first_leg_side(market_slug)
        if first == "UP":
            return float(down_ask or 0)
        if first == "DOWN":
            return float(up_ask or 0)
        return 0.0

    def second_entry_max_hedge_ask(self) -> float:
        """Max hedge ask = 1 - first_leg_ask_threshold (from config), unless hedge_ask_threshold set."""
        return float(self.second_entry_hedge_ask_threshold or 0)

    def second_entry_allowed(
        self,
        market_slug: str,
        *,
        first_leg_live_ask: float,
        opposite_live_ask: float,
        spot_now: float,
        spot_open: float,
    ) -> tuple:
        """Hedge when opposite live ask < 1 - first_leg_ask_threshold."""
        if not self.second_entry_enabled:
            return False, "second_entry_disabled"
        if self._flip_reverse_done.get(market_slug):
            return False, "second_entry_already_placed"
        if self._flip_reverse_pending.get(market_slug):
            return False, "second_entry_in_flight"
        if self.orders_placed_count(market_slug) >= self.max_orders_per_market:
            return False, "max_two_orders_per_market"
        if self.orders_placed_count(market_slug) < 1:
            return False, "need_first_leg_before_second"
        first = self.get_first_leg_side(market_slug)
        if not first:
            return False, "no_first_leg"
        if opposite_live_ask <= 0:
            return False, "invalid_opposite_ask"
        threshold = float(self.second_entry_ask_threshold)
        max_hedge_ask = self.second_entry_max_hedge_ask()
        if max_hedge_ask <= 0:
            return False, "hedge_ask_threshold_missing"
        if opposite_live_ask >= max_hedge_ask:
            return False, (
                f"hedge_ask_too_high({opposite_live_ask:.2f}>={max_hedge_ask:.2f},"
                f"1-first_leg_ask_threshold={max_hedge_ask:.2f})"
            )
        if spot_open <= 0 or spot_now <= 0:
            return False, "spot_missing"
        max_dist = float(self.second_entry_max_spot_distance_usd)
        if max_dist > 0:
            dist = abs(spot_now - spot_open)
            if dist >= max_dist:
                return False, f"spot_distance_high({dist:.1f}>={max_dist:.0f})"
        opp = "DOWN" if first == "UP" else "UP"
        return True, (
            f"opp {opp} ask {opposite_live_ask:.2f}<{max_hedge_ask:.2f} "
            f"(1-first_leg_ask_threshold={threshold:.2f})"
        )

    def apply_exit_config_from(self, config: Dict) -> None:
        """Reload flip/reverse params from config (each price tick)."""
        flip_cfg = config.get('exit', {}).get('flip_stop', {})
        self.flip_stop_enabled = bool(flip_cfg.get('enabled', self.flip_stop_enabled))
        self.flip_stop_price = float(flip_cfg.get('price_threshold', self.flip_stop_price))
        self.flip_stop_max_spot_distance_usd = float(
            flip_cfg.get('max_spot_distance_from_open_usd', 0) or 0
        )
        rev_stop_raw = float(flip_cfg.get('reverse_stop_price_threshold', 0) or 0)
        self.reverse_stop_price_threshold = (
            rev_stop_raw if rev_stop_raw > 0 else float(self.flip_stop_price)
        )
        self.flip_reverse_enabled = bool(flip_cfg.get('reverse_entry_enabled', False))
        self.flip_reverse_entry_usd = float(flip_cfg.get('reverse_entry_usd', 0) or 0)
        if self.flip_reverse_entry_usd <= 0:
            self.flip_reverse_entry_usd = float(
                config.get('strategy', {}).get('entry_order_usd', 5) or 5
            )
        raw_rev_px = float(flip_cfg.get('reverse_entry_price', 0) or 0)
        self.reverse_entry_price = (
            raw_rev_px if raw_rev_px > 0 else float(self.flip_stop_price)
        )
        self.apply_second_entry_config_from(config)

    def orders_placed_count(self, market_slug: str) -> int:
        return int(self._entries_placed.get(market_slug, 0) or 0)

    def get_first_leg_side(self, market_slug: str) -> Optional[str]:
        s = (self._first_leg_side.get(market_slug) or "").upper()
        return s if s in ("UP", "DOWN") else None

    def first_leg_ask_tracking_active(self, market_slug: str) -> bool:
        """First leg filled; peak tracker active until market-end flush."""
        if int(self._entries_placed.get(market_slug, 0) or 0) < 1:
            return False
        if not self.get_first_leg_side(market_slug):
            return False
        return market_slug in self._first_leg_ask_max

    def start_first_leg_ask_tracking(self, market_slug: str, side: str) -> None:
        """Track max live ask on first-leg side from after first fill until market end."""
        s = (side or "").upper()
        if s not in ("UP", "DOWN"):
            return
        self._first_leg_side[market_slug] = s
        if market_slug not in self._first_leg_ask_max:
            self._first_leg_ask_max[market_slug] = 0.0

    def update_first_leg_ask_max(
        self, market_slug: str, *, up_ask: float, down_ask: float
    ) -> Optional[float]:
        """Running max of first-leg direction live ask (up_ask or down_ask)."""
        side = self.get_first_leg_side(market_slug)
        if not side or market_slug not in self._first_leg_ask_max:
            return None
        px = float(up_ask if side == "UP" else down_ask)
        if px <= 0:
            cur = float(self._first_leg_ask_max.get(market_slug, 0) or 0)
            return cur if cur > 0 else None
        cur = float(self._first_leg_ask_max.get(market_slug, 0) or 0)
        if px > cur:
            self._first_leg_ask_max[market_slug] = px
        return float(self._first_leg_ask_max[market_slug])

    def get_first_leg_ask_max(self, market_slug: str) -> Optional[float]:
        v = self._first_leg_ask_max.get(market_slug)
        return float(v) if v is not None else None

    def pop_first_leg_ask_max(self, market_slug: str) -> Optional[float]:
        v = self._first_leg_ask_max.pop(market_slug, None)
        return float(v) if v is not None else None

    def start_second_entry_hedge_tracking(self, market_slug: str) -> None:
        """Track min live hedge ask only while first leg is open (for analytics)."""
        self._second_entry_hedge_track[market_slug] = True

    def stop_second_entry_hedge_tracking(self, market_slug: str) -> None:
        self._second_entry_hedge_track.pop(market_slug, None)

    def update_second_entry_hedge_ask_min(
        self, market_slug: str, *, up_ask: float, down_ask: float
    ) -> Optional[float]:
        if not self._second_entry_hedge_track.get(market_slug):
            return None
        if self._flip_reverse_done.get(market_slug):
            return None
        first = self.get_first_leg_side(market_slug)
        if not first:
            return None
        hedge = float(down_ask if first == "UP" else up_ask)
        if hedge <= 0:
            v = self._second_entry_hedge_ask_min.get(market_slug)
            return float(v) if v is not None else None
        cur = float(self._second_entry_hedge_ask_min.get(market_slug, 0) or 0)
        if cur <= 0 or hedge < cur:
            self._second_entry_hedge_ask_min[market_slug] = hedge
        return float(self._second_entry_hedge_ask_min[market_slug])

    def pop_second_entry_hedge_ask_min(self, market_slug: str) -> Optional[float]:
        self._second_entry_hedge_track.pop(market_slug, None)
        v = self._second_entry_hedge_ask_min.pop(market_slug, None)
        return float(v) if v is not None and float(v) > 0 else None

    def reverse_hedge_entry_allowed(
        self, market_slug: str, *, our_side: str, our_price: float
    ) -> tuple:
        """Price-triggered opposite entry while first leg is still open."""
        if not self.flip_reverse_enabled:
            return False, "reverse_entry_disabled"
        if self._flip_reverse_done.get(market_slug):
            return False, "reverse_already_placed"
        if self._flip_reverse_pending.get(market_slug):
            return False, "reverse_entry_in_flight"
        if self._flip_stop_handled.get(market_slug):
            return False, "flip_stop_already_handled"
        if self.orders_placed_count(market_slug) >= self.max_orders_per_market:
            return False, "max_two_orders_per_market"
        if self.orders_placed_count(market_slug) < 1:
            return False, "need_first_leg_before_reverse"
        first_leg = self.get_first_leg_side(market_slug)
        if not first_leg:
            n = int(self._entries_placed.get(market_slug, 0) or 0)
            if n < 1:
                return False, "no_first_leg"
            first_leg = (our_side or "").upper()
        side = (our_side or "").upper()
        if side != first_leg:
            return False, f"not_first_leg_side({first_leg})"
        if our_price <= 0:
            return False, "invalid_price"
        if our_price > float(self.reverse_entry_price):
            return False, f"above_reverse_entry_price({self.reverse_entry_price:.2f})"
        return True, f"{side}<=${self.reverse_entry_price:.2f}"

    def flip_reverse_allowed(
        self, market_slug: str, *, has_open_first_leg: bool = False
    ) -> tuple:
        """Legacy alias — reverse is no longer tied to flip-stop."""
        if self._flip_reverse_done.get(market_slug):
            return False, "reverse_already_placed"
        return False, "use_reverse_hedge_entry_allowed"

    def sync_entry_from_open_position(self, market_slug: str, trader: Any = None) -> None:
        if int(self._entries_placed.get(market_slug, 0) or 0) < 1:
            self._entries_placed[market_slug] = 1
        if not trader:
            return
        pos = getattr(trader, "positions", {}).get(market_slug) or {}
        for ent in pos.get("all_entries") or []:
            if _is_hedge_entry_reason(ent.get("entry_reason")):
                continue
            s = (ent.get("side") or "").upper()
            if s in ("UP", "DOWN"):
                if market_slug not in self._first_leg_side:
                    self._first_leg_side[market_slug] = s
                if market_slug not in self._first_leg_ask_max:
                    self.start_first_leg_ask_tracking(market_slug, s)
                if market_slug not in self._second_entry_hedge_track:
                    self.start_second_entry_hedge_tracking(market_slug)
                break

    def confirm_entry_success(
        self,
        market_slug: str,
        side: Optional[str] = None,
        entry_ask: Optional[float] = None,
    ) -> None:
        with self._state_lock:
            self._entry_signal_pending.pop(market_slug, None)
            self._entries_placed[market_slug] = (
                int(self._entries_placed.get(market_slug, 0) or 0) + 1
            )
            s = (side or "").upper()
            if s in ("UP", "DOWN") and market_slug not in self._first_leg_side:
                self._first_leg_side[market_slug] = s
            placed = int(self._entries_placed.get(market_slug, 0) or 0)
            if placed == 1 and s in ("UP", "DOWN"):
                self.start_first_leg_ask_tracking(market_slug, s)
            self.start_second_entry_hedge_tracking(market_slug)

    def try_reserve_entry(self, market_slug: str) -> bool:
        """Atomically reserve entry slot before filters (prevents concurrent duplicate signals)."""
        with self._state_lock:
            if self._entry_signal_pending.get(market_slug):
                return False
            if self._entries_placed.get(market_slug, 0) >= self.max_entries_per_market:
                return False
            self._entry_signal_pending[market_slug] = True
            return True

    def try_begin_flip_reverse(self, market_slug: str) -> bool:
        """Reserve reverse slot before exchange order (prevents concurrent duplicates)."""
        if self._flip_reverse_done.get(market_slug):
            return False
        if self._flip_reverse_pending.get(market_slug):
            return False
        if self.orders_placed_count(market_slug) >= self.max_orders_per_market:
            return False
        self._flip_reverse_pending[market_slug] = True
        return True

    def abort_flip_reverse(self, market_slug: str) -> None:
        self._flip_reverse_pending.pop(market_slug, None)

    def mark_flip_reverse_placed(self, market_slug: str) -> None:
        self._flip_reverse_pending.pop(market_slug, None)
        self._flip_reverse_done[market_slug] = True
        self.stop_second_entry_hedge_tracking(market_slug)
        self._entries_placed[market_slug] = max(
            2, int(self._entries_placed.get(market_slug, 0) or 0)
        )

    def is_reverse_leg_only(self, market_slug: str) -> bool:
        """After second_entry / flip_reverse hedge placed."""
        return bool(self._flip_reverse_done.get(market_slug))

    def resolve_flip_stop_target(
        self,
        market_slug: str,
        *,
        up_ask: float,
        down_ask: float,
        trader: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Flip-stop on first leg only; second_entry leg never flip-stopped."""
        all_entries: list = []
        up_shares = 0.0
        down_shares = 0.0
        if trader is not None:
            pos = getattr(trader, "positions", {}).get(market_slug) or {}
            all_entries = list(pos.get("all_entries") or [])
            up_shares = float(pos.get("UP", {}).get("total_shares") or 0)
            down_shares = float(pos.get("DOWN", {}).get("total_shares") or 0)
            if int(self._entries_placed.get(market_slug, 0) or 0) < 1:
                self.sync_entry_from_open_position(market_slug, trader)
        flip_cfg = {
            "enabled": self.flip_stop_enabled,
            "price_threshold": self.flip_stop_price,
            "reverse_stop_price_threshold": self.reverse_stop_price_threshold,
            "reverse_entry_enabled": self.flip_reverse_enabled,
        }
        return resolve_flip_stop_target(
            flip_cfg=flip_cfg,
            up_ask=up_ask,
            down_ask=down_ask,
            all_entries=all_entries,
            flip_reverse_done=self.is_reverse_leg_only(market_slug),
            flip_stop_handled=bool(self._flip_stop_handled.get(market_slug)),
            first_leg_side=self.get_first_leg_side(market_slug),
            up_shares=up_shares,
            down_shares=down_shares,
            entries_placed=int(self._entries_placed.get(market_slug, 0) or 0),
        )

    def try_begin_flip_stop(self, market_slug: str) -> bool:
        """Reserve flip-stop handling before exchange sell (prevents double sell)."""
        with self._state_lock:
            if self._flip_stop_handled.get(market_slug):
                return False
            if self._flip_stop_pending.get(market_slug):
                return False
            self._flip_stop_pending[market_slug] = True
            return True

    def abort_flip_stop(self, market_slug: str) -> None:
        with self._state_lock:
            self._flip_stop_pending.pop(market_slug, None)

    def mark_flip_stop_handled(self, market_slug: str) -> None:
        """First leg closed by flip-stop; block further normal entries this market."""
        with self._state_lock:
            self._flip_stop_pending.pop(market_slug, None)
            self._flip_stop_handled[market_slug] = True

    def _abort_entry_signal(self, market_slug: str) -> None:
        self.release_reserved_entry(market_slug)

    def entry_block_reason(
        self, state: Dict, position: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Read-only entry gate (no try_reserve_entry). Returns reason if blocked, else None.
        """
        market = state.get("market_slug") or ""
        if not market:
            return "no_market_slug"
        time_left = int(state.get("seconds_till_end") or 0)
        up_ask = float(state.get("up_ask") or 0)
        down_ask = float(state.get("down_ask") or 0)

        if self._flip_stop_handled.get(market):
            return "flip_stop_handled"
        if self._flip_reverse_done.get(market):
            return "flip_reverse_done"
        if self._entries_placed.get(market, 0) >= self.max_entries_per_market:
            return "already_entered"
        with self._state_lock:
            if self._entry_signal_pending.get(market):
                return "entry_pending"

        if time_left > self.entry_window or time_left <= 0:
            return "outside_entry_window"

        if self.min_window_range_usd > 0:
            wr = state.get("window_range_usd")
            if wr is None:
                return "window_range_unknown"
            try:
                wr_f = float(wr)
                if wr_f < self.min_window_range_usd:
                    return f"window_range_low({wr_f:.1f}<{self.min_window_range_usd:.0f})"
            except (TypeError, ValueError):
                return "window_range_unknown"

        now = time.time()
        if market in self.last_entry and now - self.last_entry[market] < self.entry_freq:
            return "entry_cooldown"

        if self.min_spot_move_usd > 0:
            open_px = float(
                state.get("market_start_price") or state.get("spot_start") or 0
            )
            cur_px = float(
                state.get("price") or state.get("current_price") or state.get("spot") or 0
            )
            if open_px <= 0 or cur_px <= 0:
                return "spot_open_or_cur_missing"
            spot_move_abs = abs(cur_px - open_px)
            if spot_move_abs < self.min_spot_move_usd:
                return (
                    f"spot_move_low({spot_move_abs:.1f}<{self.min_spot_move_usd:.0f})"
                )

        spread = up_ask + down_ask
        if spread <= 0:
            return "spread_invalid"
        if spread > self.max_spread:
            return f"spread_high({spread:.3f}>{self.max_spread})"

        confidence = abs(up_ask - down_ask)
        if confidence < self.min_confidence:
            return f"confidence_low({confidence:.2f}<{self.min_confidence:.2f})"

        favorite = "UP" if up_ask > down_ask else "DOWN"
        fav_price = up_ask if favorite == "UP" else down_ask

        if fav_price > self.price_max:
            return f"price_max({fav_price:.2f}>{self.price_max})"
        if not side_entry_price_allowed(
            favorite,
            fav_price,
            global_price_max=float(self.price_max),
            side_filters=self.side_price_filters,
        ):
            return "side_price_filter"

        if position:
            total_cost = position.get("total_cost", 0)
            if total_cost >= self.max_investment:
                return "max_investment"

        return None

    def should_enter(self, state: Dict, position: Optional[Dict] = None) -> Optional[Dict]:
        """
        Check if should enter (Late Entry V3 logic)
        
        Args:
            state: Market state with keys:
                - market_slug: str
                - seconds_till_end: int
                - up_ask: float
                - down_ask: float
                - market_start_price: float (open / 标的起)
                - price: float (current underlying spot)
            position: Optional position stats
        
        Returns:
            Signal dict or None
        """
        market = state['market_slug']
        time_left = state['seconds_till_end']
        up_ask = state['up_ask']
        down_ask = state['down_ask']

        if self._flip_stop_handled.get(market):
            return None

        if self._flip_reverse_done.get(market):
            return None

        if self._entries_placed.get(market, 0) >= self.max_entries_per_market:
            return None

        if not self.try_reserve_entry(market):
            return None

        # TIME: only inside configured late window
        if time_left > self.entry_window or time_left <= 0:
            self._abort_entry_signal(market)
            return None

        in_entry_window = time_left <= self.entry_window

        # WINDOW RANGE: Chainlink amplitude since open (coin-flip filter)
        if self.min_window_range_usd > 0 and in_entry_window:
            wr = state.get('window_range_usd')
            if wr is None:
                self._abort_entry_signal(market)
                return None
            try:
                if float(wr) < self.min_window_range_usd:
                    self._abort_entry_signal(market)
                    return None
            except (TypeError, ValueError):
                self._abort_entry_signal(market)
                return None
        
        # FREQUENCY
        now = time.time()
        if market in self.last_entry and now - self.last_entry[market] < self.entry_freq:
            self._abort_entry_signal(market)
            return None

        # SPOT MOVE: |current - open| >= min_spot_move_usd (USD)
        spot_move_abs = 0.0
        if self.min_spot_move_usd > 0:
            open_px = float(
                state.get('market_start_price') or state.get('spot_start') or 0
            )
            cur_px = float(
                state.get('price') or state.get('current_price') or state.get('spot') or 0
            )
            if open_px <= 0 or cur_px <= 0:
                self._abort_entry_signal(market)
                return None
            spot_move_abs = abs(cur_px - open_px)
            if spot_move_abs < self.min_spot_move_usd:
                self._abort_entry_signal(market)
                return None
        
        # SPREAD
        spread = up_ask + down_ask
        if spread > self.max_spread or spread <= 0:
            self._abort_entry_signal(market)
            return None
        
        # CONFIDENCE
        confidence = abs(up_ask - down_ask)
        if confidence < self.min_confidence:
            self._abort_entry_signal(market)
            return None
        
        # FAVORITE
        favorite = 'UP' if up_ask > down_ask else 'DOWN'
        fav_price = up_ask if favorite == 'UP' else down_ask
        
        # PRICE MAX (+ optional per-side skip ranges from historical analysis)
        if fav_price > self.price_max:
            self._abort_entry_signal(market)
            return None
        if not side_entry_price_allowed(
            favorite,
            fav_price,
            global_price_max=float(self.price_max),
            side_filters=self.side_price_filters,
        ):
            self._abort_entry_signal(market)
            return None
        
        # INVESTMENT LIMIT
        if position:
            total_cost = position.get('total_cost', 0)
            if total_cost >= self.max_investment:
                self._abort_entry_signal(market)
                return None
        
        # RISK CHECKS - stop-loss removed, only flip-stop via main.py
        # Flip-stop logic in main.py (check: our_price <= strategy.flip_stop_price)
        
        # ENTRY SIZE: fixed USD notional or time-tier contracts
        if self.entry_order_usd > 0 and fav_price > 0:
            size: Union[int, float] = round(self.entry_order_usd / fav_price, 2)
            if size <= 0:
                self._abort_entry_signal(market)
                return None
        else:
            size = (
                self.size_above_180
                if time_left > self.sizing_t1
                else (self.size_above_120 if time_left > self.sizing_t2 else self.size_below_120)
            )
        
        self.last_entry[market] = now
        self.last_favorite[market] = favorite
        
        return {
            'favored': {
                'side': favorite,
                'price': fav_price,
                'contracts': size,
            },
            'hedge': {
                'side': 'DOWN' if favorite == 'UP' else 'UP',
                'price': down_ask if favorite == 'UP' else up_ask,
                'contracts': 0,
            },
            'confidence': confidence,
            'spot_move_abs': spot_move_abs,
            'is_recovery': False,
            'entry_reason': f'late_entry_{time_left}s',
            'winner_ratio': 0.0
        }
    
    def get_stats(self) -> Dict:
        """Get strategy statistics (for dashboard compatibility)"""
        return {
            'generated': 0,
            'skipped': 0,
            'total': 0,
            'skip_breakdown': {},
            'gen_pct': 0,
            'skip_pct': 0,
            'wr_recoveries': 0
        }
    
    def reset_market(self, market_slug: str):
        """Reset tracking for a market"""
        if market_slug in self.last_entry:
            del self.last_entry[market_slug]
        if market_slug in self.last_favorite:
            del self.last_favorite[market_slug]
        if market_slug in self._entries_placed:
            del self._entries_placed[market_slug]
        if market_slug in self._entry_signal_pending:
            del self._entry_signal_pending[market_slug]
        if market_slug in self._flip_reverse_done:
            del self._flip_reverse_done[market_slug]
        if market_slug in self._flip_reverse_pending:
            del self._flip_reverse_pending[market_slug]
        if market_slug in self._first_leg_side:
            del self._first_leg_side[market_slug]
        if market_slug in self._first_leg_ask_max:
            del self._first_leg_ask_max[market_slug]
        if market_slug in self._second_entry_hedge_track:
            del self._second_entry_hedge_track[market_slug]
        if market_slug in self._second_entry_hedge_ask_min:
            del self._second_entry_hedge_ask_min[market_slug]
        if market_slug in self._flip_stop_handled:
            del self._flip_stop_handled[market_slug]
        if market_slug in self._flip_stop_pending:
            del self._flip_stop_pending[market_slug]

    def release_reserved_entry(self, market_slug: str) -> None:
        """Signal was issued but enter did not run or failed — allow another signal."""
        with self._state_lock:
            self._entry_signal_pending.pop(market_slug, None)
